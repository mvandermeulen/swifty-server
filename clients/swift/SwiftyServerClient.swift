import Foundation
import Combine

/// Reference implementation demonstrating how to work with the Swifty Server API
/// from macOS or iOS clients using Swift Concurrency and Combine.
public final class SwiftyServerClient: NSObject {
    public enum ConnectionState: Equatable {
        case idle
        case connecting
        case connected(UUID)
        case reconnecting(Int)
        case failed(String)
    }

    private enum ClientError: Error {
        case unauthenticated
        case invalidURL
    }

    public let connectionState = CurrentValueSubject<ConnectionState, Never>(.idle)
    public let events = PassthroughSubject<InboundFrame, Never>()
    public let errors = PassthroughSubject<Error, Never>()

    private let restBaseURL: URL
    private let webSocketBaseURL: URL
    private let session: URLSession
    private let encoder: JSONEncoder
    private let decoder: JSONDecoder
    private let outboundBuffer: OutboundBuffer
    private let outboundCapacity: Int

    private var token: String?
    private var registration: RegistrationResponse?
    private var webSocketTask: URLSessionWebSocketTask?
    private var sendLoopTask: Task<Void, Never>?
    private var receiveLoopTask: Task<Void, Never>?
    private var reconnectionAttempts: Int = 0
    private var isManuallyClosed = false

    public init(restBaseURL: URL, webSocketBaseURL: URL, sessionConfiguration: URLSessionConfiguration = .default, outboundCapacity: Int = 64) {
        self.restBaseURL = restBaseURL
        self.webSocketBaseURL = webSocketBaseURL
        self.outboundCapacity = outboundCapacity
        self.session = URLSession(configuration: sessionConfiguration)
        self.encoder = JSONEncoder()
        self.decoder = JSONDecoder()
        self.decoder.dateDecodingStrategy = .secondsSince1970
        self.encoder.dateEncodingStrategy = .secondsSince1970
        self.outboundBuffer = OutboundBuffer()
        super.init()
    }

    // MARK: - REST helpers

    @discardableResult
    public func authenticate(name: String, uuid: UUID) async throws -> RegistrationResponse {
        let payload = ClientRegistrationRequest(name: name, uuid: uuid)
        let data = try JSONEncoder().encode(payload)
        var request = URLRequest(url: restBaseURL.appendingPathComponent("register"))
        request.httpMethod = "POST"
        request.httpBody = data
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")

        let (responseData, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw URLError(.badServerResponse)
        }

        if http.statusCode == 200 {
            let registration = try decoder.decode(RegistrationResponse.self, from: responseData)
            token = registration.token
            self.registration = registration
            return registration
        }

        if let error = try? decoder.decode(ErrorResponse.self, from: responseData) {
            throw error
        }

        throw URLError(.init(rawValue: http.statusCode))
    }

    public func listTopics() async throws -> TopicListResponse {
        let request = URLRequest(url: restBaseURL.appendingPathComponent("topics"))
        let (data, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse, http.statusCode == 200 else {
            throw URLError(.badServerResponse)
        }
        return try decoder.decode(TopicListResponse.self, from: data)
    }

    public func createTopic(_ requestBody: TopicCreateRequest) async throws -> TopicCreateResponse {
        guard let token else { throw ClientError.unauthenticated }
        var components = URLComponents(url: restBaseURL.appendingPathComponent("topics/create"), resolvingAgainstBaseURL: false)
        components?.queryItems = [URLQueryItem(name: "token", value: token)]
        guard let url = components?.url else { throw ClientError.invalidURL }
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try encoder.encode(requestBody)
        let (data, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse, http.statusCode == 200 else {
            if let error = try? decoder.decode(ErrorResponse.self, from: data) {
                throw error
            }
            throw URLError(.badServerResponse)
        }
        return try decoder.decode(TopicCreateResponse.self, from: data)
    }

    // MARK: - WebSocket lifecycle

    public func connect() async throws {
        guard let token, let registration else { throw ClientError.unauthenticated }
        isManuallyClosed = false
        connectionState.send(.connecting)
        try await openWebSocket(with: token, clientID: registration.uuid, attempt: 0)
    }

    public func disconnect() {
        isManuallyClosed = true
        reconnectionAttempts = 0
        connectionState.send(.idle)
        sendLoopTask?.cancel()
        receiveLoopTask?.cancel()
        webSocketTask?.cancel(with: .goingAway, reason: nil)
        webSocketTask = nil
        Task { await outboundBuffer.close() }
    }

    public func send(_ frame: OutboundFrame) async throws {
        guard webSocketTask != nil else { throw URLError(.notConnectedToInternet) }
        try await outboundBuffer.enqueue(frame, capacity: outboundCapacity)
    }

    private func openWebSocket(with token: String, clientID: UUID, attempt: Int) async throws {
        var components = URLComponents(url: webSocketBaseURL, resolvingAgainstBaseURL: false)
        components?.queryItems = [URLQueryItem(name: "token", value: token)]
        guard let url = components?.url else { throw ClientError.invalidURL }

        let task = session.webSocketTask(with: url)
        webSocketTask = task
        await outboundBuffer.reset()
        task.resume()
        reconnectionAttempts = attempt

        sendLoopTask?.cancel()
        receiveLoopTask?.cancel()

        sendLoopTask = Task { [weak self, weak task] in
            guard let task, let self else { return }
            await self.runSendLoop(task)
        }

        receiveLoopTask = Task { [weak self, weak task] in
            guard let task, let self else { return }
            await self.runReceiveLoop(task: task)
        }
    }

    private func runSendLoop(_ task: URLSessionWebSocketTask) async {
        while !Task.isCancelled {
            guard let frame = await outboundBuffer.dequeue() else {
                if await outboundBuffer.isClosed() { break }
                try? await Task.sleep(nanoseconds: 50_000_000)
                continue
            }
            do {
                let data = try encoder.encode(frame)
                guard let string = String(data: data, encoding: .utf8) else { continue }
                try await task.send(.string(string))
            } catch {
                errors.send(error)
                await handleDisconnect(dueTo: error)
                return
            }
        }
    }

    private func runReceiveLoop(task: URLSessionWebSocketTask) async {
        do {
            while !Task.isCancelled {
                let message = try await task.receive()
                switch message {
                case .string(let text):
                    guard let data = text.data(using: .utf8) else { continue }
                    do {
                        let frame = try decoder.decode(InboundFrame.self, from: data)
                        if case .connection(let notice) = frame {
                            self.reconnectionAttempts = 0
                            connectionState.send(.connected(notice.uuid))
                        }
                        events.send(frame)
                    } catch {
                        errors.send(error)
                    }
                case .data:
                    continue
                @unknown default:
                    continue
                }
            }
        } catch {
            errors.send(error)
            await handleDisconnect(dueTo: error)
        }
    }

    private func handleDisconnect(dueTo error: Error?) async {
        guard !isManuallyClosed else { return }
        reconnectionAttempts += 1
        connectionState.send(.reconnecting(reconnectionAttempts))
        let delay = min(pow(2.0, Double(reconnectionAttempts)), 30.0)
        try? await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
        guard let token, let registration else {
            connectionState.send(.failed("Missing token"))
            return
        }
        do {
            try await openWebSocket(with: token, clientID: registration.uuid, attempt: reconnectionAttempts)
        } catch {
            connectionState.send(.failed(error.localizedDescription))
            Task { [weak self] in
                await self?.handleDisconnect(dueTo: error)
            }
        }
    }
}

// MARK: - Outbound buffering with backpressure

actor OutboundBuffer {
    enum BufferError: Error { case closed }

    private var queue: [OutboundFrame] = []
    private var closed = false

    func enqueue(_ frame: OutboundFrame, capacity: Int) async throws {
        while queue.count >= capacity {
            try await Task.sleep(nanoseconds: 10_000_000)
            if closed { throw BufferError.closed }
        }
        if closed { throw BufferError.closed }
        queue.append(frame)
    }

    func dequeue() async -> OutboundFrame? {
        while queue.isEmpty && !closed {
            try? await Task.sleep(nanoseconds: 20_000_000)
            if Task.isCancelled { return nil }
        }
        guard !queue.isEmpty else { return nil }
        return queue.removeFirst()
    }

    func isClosed() -> Bool { closed }

    func reset() {
        queue.removeAll(keepingCapacity: true)
        closed = false
    }

    func close() {
        closed = true
        queue.removeAll()
    }
}
