import Foundation

// MARK: - REST Models

public struct MetadataResponse: Codable {
    public let message: String
    public let version: String
    public let endpoints: [String: String]
}

public struct ClientRegistrationRequest: Codable {
    public let name: String
    public let uuid: UUID
}

public struct RegistrationResponse: Codable {
    public let token: String
    public let uuid: UUID
    public let name: String
    public let message: String
}

public struct ClientListResponse: Codable {
    public let count: Int
    public let clients: [UUID: String]
}

public struct TopicCreateRequest: Codable {
    public let topicID: String
    public let metadata: [String: JSONValue]?

    enum CodingKeys: String, CodingKey {
        case topicID = "topic_id"
        case metadata
    }
}

public struct TopicCreateResponse: Codable {
    public let message: String
    public let topicID: String
    public let creator: UUID

    enum CodingKeys: String, CodingKey {
        case message
        case topicID = "topic_id"
        case creator
    }
}

public struct TopicSubscribeRequest: Codable {
    public let topicID: String

    enum CodingKeys: String, CodingKey {
        case topicID = "topic_id"
    }
}

public struct TopicSubscribeResponse: Codable {
    public let message: String
    public let topicID: String
    public let clientUUID: UUID

    enum CodingKeys: String, CodingKey {
        case message
        case topicID = "topic_id"
        case clientUUID = "client_uuid"
    }
}

public typealias TopicUnsubscribeResponse = TopicSubscribeResponse

public struct TopicSummary: Codable {
    public let id: String
    public let creator: UUID
    public let metadata: [String: JSONValue]
    public let subscriberCount: Int

    enum CodingKeys: String, CodingKey {
        case id
        case creator
        case metadata
        case subscriberCount = "subscriber_count"
    }
}

public struct TopicListResponse: Codable {
    public let count: Int
    public let topics: [TopicSummary]
}

public struct TopicDetailResponse: Codable {
    public let id: String
    public let creator: UUID
    public let metadata: [String: JSONValue]
    public let subscriberCount: Int
    public let subscribers: [UUID]

    enum CodingKeys: String, CodingKey {
        case id
        case creator
        case metadata
        case subscriberCount = "subscriber_count"
        case subscribers
    }
}

public struct ErrorResponse: Codable, Error {
    public let type: String
    public let message: String
    public let timestamp: TimeInterval
    public let detail: [String: JSONValue]?
}

// MARK: - WebSocket Payloads

public struct DirectPayload: Codable, Identifiable {
    public let to: UUID
    public let from: UUID
    public let timestamp: TimeInterval
    public let priority: String
    public let subject: String
    public let msgid: UUID
    public let acknowledge: Bool
    public let content: String
    public let action: String
    public let event: String
    public let status: String
    public let conversationID: String
    public let msgno: Int

    public var id: UUID { msgid }

    enum CodingKeys: String, CodingKey {
        case to
        case from = "from"
        case timestamp
        case priority
        case subject
        case msgid
        case acknowledge
        case content
        case action
        case event
        case status
        case conversationID = "conversation_id"
        case msgno
    }
}

public struct TopicPayload: Codable, Identifiable {
    public let topicID: String
    public let to: UUID
    public let from: UUID
    public let timestamp: TimeInterval
    public let priority: String
    public let subject: String
    public let msgid: UUID
    public let acknowledge: Bool
    public let content: String
    public let action: String
    public let event: String
    public let status: String
    public let conversationID: String
    public let msgno: Int

    public var id: UUID { msgid }

    enum CodingKeys: String, CodingKey {
        case topicID = "topic_id"
        case to
        case from = "from"
        case timestamp
        case priority
        case subject
        case msgid
        case acknowledge
        case content
        case action
        case event
        case status
        case conversationID = "conversation_id"
        case msgno
    }

    public var asDirectPayload: DirectPayload {
        DirectPayload(
            to: to,
            from: from,
            timestamp: timestamp,
            priority: priority,
            subject: subject,
            msgid: msgid,
            acknowledge: acknowledge,
            content: content,
            action: action,
            event: event,
            status: status,
            conversationID: conversationID,
            msgno: msgno
        )
    }
}

public struct AckPayload: Codable {
    public let type: String
    public let msgid: UUID
    public let from: UUID
    public let timestamp: TimeInterval
    public let status: String
}

public struct ConnectionNotice: Codable {
    public let type: String
    public let message: String
    public let uuid: UUID
    public let name: String
    public let timestamp: TimeInterval
}

public struct DeliveryReceipt: Codable {
    public let type: String
    public let message: String
    public let msgid: UUID
    public let timestamp: TimeInterval
    public let topicID: String?

    enum CodingKeys: String, CodingKey {
        case type
        case message
        case msgid
        case timestamp
        case topicID = "topic_id"
    }
}

public struct ErrorPayload: Codable, Error {
    public let type: String
    public let message: String
    public let timestamp: TimeInterval
    public let detail: [String: JSONValue]?
    public let msgid: UUID?
}

public struct StatusPayload: Codable {
    public let type: String
    public let message: String
    public let timestamp: TimeInterval
    public let version: String
    public let affected: [String]
}

// MARK: - Shared JSON helpers

public enum JSONValue: Codable {
    case string(String)
    case int(Int)
    case double(Double)
    case bool(Bool)
    case array([JSONValue])
    case object([String: JSONValue])
    case null

    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if container.decodeNil() {
            self = .null
        } else if let string = try? container.decode(String.self) {
            self = .string(string)
        } else if let int = try? container.decode(Int.self) {
            self = .int(int)
        } else if let double = try? container.decode(Double.self) {
            self = .double(double)
        } else if let bool = try? container.decode(Bool.self) {
            self = .bool(bool)
        } else if let array = try? container.decode([JSONValue].self) {
            self = .array(array)
        } else if let object = try? container.decode([String: JSONValue].self) {
            self = .object(object)
        } else {
            throw DecodingError.dataCorruptedError(in: container, debugDescription: "Unsupported JSON value")
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .string(let value):
            try container.encode(value)
        case .int(let value):
            try container.encode(value)
        case .double(let value):
            try container.encode(value)
        case .bool(let value):
            try container.encode(value)
        case .array(let value):
            try container.encode(value)
        case .object(let value):
            try container.encode(value)
        case .null:
            try container.encodeNil()
        }
    }
}

// MARK: - Envelope enums

public enum OutboundFrame: Encodable {
    case direct(DirectPayload)
    case topic(TopicPayload)
    case ack(AckPayload)

    public func encode(to encoder: Encoder) throws {
        switch self {
        case .direct(let payload):
            try payload.encode(to: encoder)
        case .topic(let payload):
            try payload.encode(to: encoder)
        case .ack(let payload):
            try payload.encode(to: encoder)
        }
    }
}

public enum InboundFrame: Decodable {
    case connection(ConnectionNotice)
    case directReceipt(DeliveryReceipt)
    case topicReceipt(DeliveryReceipt)
    case inboundDirect(DirectPayload)
    case inboundTopic(TopicPayload)
    case error(ErrorPayload)
    case status(StatusPayload)

    private enum CodingKeys: String, CodingKey {
        case type
        case topicID = "topic_id"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let type = try container.decodeIfPresent(String.self, forKey: .type)
        switch type {
        case "connection":
            self = .connection(try ConnectionNotice(from: decoder))
        case "sent":
            self = .directReceipt(try DeliveryReceipt(from: decoder))
        case "topic_sent":
            self = .topicReceipt(try DeliveryReceipt(from: decoder))
        case "error":
            self = .error(try ErrorPayload(from: decoder))
        case "status.updated":
            self = .status(try StatusPayload(from: decoder))
        case .some:
            // Unknown typed frame defaults to direct payload
            self = .inboundDirect(try DirectPayload(from: decoder))
        case .none:
            if (try? container.decode(String.self, forKey: .topicID)) != nil {
                self = .inboundTopic(try TopicPayload(from: decoder))
            } else {
                self = .inboundDirect(try DirectPayload(from: decoder))
            }
        }
    }
}
