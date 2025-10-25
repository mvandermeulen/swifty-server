# Swifty Server Operational Runbooks

This document describes standard operating procedures for scaling, rotating secrets, and responding to incidents for the Swifty Server platform. The guidance assumes the infrastructure assets in `deploy/terraform` or `deploy/cloudformation` are used, but the steps translate to other environments with equivalent resources.

---

## 1. Scaling Runbook

### Objectives
* Increase or decrease capacity for HTTP and WebSocket traffic without downtime.
* Validate that rate limiting and throttling thresholds remain aligned with expected load.

### Prerequisites
* Access to the deployment environment (AWS credentials with `ecs`, `application-autoscaling`, and `elasticloadbalancing` permissions).
* Awareness of current traffic characteristics from monitoring dashboards (Prometheus/Grafana or CloudWatch).

### Procedure
1. **Review current metrics**
   * Confirm CPU, memory, and connection utilization. Key metrics:
     * `CONNECTION_GAUGE` (active WebSocket sessions)
     * `http_request_duration_seconds` and 4xx/5xx rates
     * ALB target response times
2. **Adjust autoscaling bounds**
   * **Terraform:** update `desired_count`/`max_count` variables and apply.
   * **CloudFormation:** modify the stack parameters `DesiredCount` and `MaxCount`.
   * For manual surge capacity, temporarily increase `desired_count` only.
3. **Tune rate limits if necessary**
   * Modify variables/parameters such as `rate_limit`, `http_max_concurrency`, or `websocket_max_connections`.
   * For Helm deployments, patch the release with updated values in `values.yaml`.
   * Re-run `terraform apply`, `aws cloudformation update-stack`, or `helm upgrade`.
4. **Validate post-change health**
   * Monitor readiness probes and ALB target health until all tasks return to `healthy`.
   * Run synthetic WebSocket checks to verify message flow and throttling behavior.
5. **Document the change**
   * Record scaling actions in the operations log with timestamps, before/after counts, and observed impact.

### Rollback
* Revert the parameter or variable changes to previous values and redeploy.
* If using autoscaling, verify cooldown timers have elapsed before reducing capacity to avoid thrashing.

---

## 2. Secrets Rotation Runbook

### Objectives
* Rotate JWT signing keys, Redis credentials, and other sensitive secrets with minimal service disruption.

### Prerequisites
* Access to secret stores (AWS SSM Parameter Store, Kubernetes secrets, or equivalent).
* Ability to restart workloads (ECS service update, Kubernetes rolling restart).

### Procedure
1. **Prepare new secrets**
   * Generate new JWT secret (`openssl rand -base64 64`) and Redis password.
   * Store them in the authoritative secret store:
     * **Terraform stack:** update SSM parameters referenced by `jwt_secret_ssm_parameter` and `redis_password_ssm_parameter`.
     * **CloudFormation stack:** supply new parameter values during stack update.
     * **Helm deployment:** update the Kubernetes secret referenced in `values.additionalEnv` or `redis.passwordSecret`.
2. **Deploy application updates**
   * Re-run the relevant deployment command (`terraform apply`, `aws cloudformation update-stack`, or `helm upgrade`).
   * ECS/Kubernetes will perform a rolling restart so new tasks pick up the secrets.
3. **Revoke prior credentials**
   * After new tasks pass health checks, remove or disable old JWT secrets and Redis passwords.
   * Use `/admin/tokens/revoke` if legacy tokens need to be invalidated proactively.
4. **Validate**
   * Attempt authentication flows (client registration, token refresh) using new credentials.
   * Monitor `ERROR_COUNTER` for spikes in `INVALID_TOKEN` during the rotation window.
5. **Document**
   * Record the rotation details, locations updated, and verification steps.

### Emergency Rotation
* If a secret compromise is suspected, immediately rotate the compromised secret, revoke affected tokens, and enable heightened logging for unusual traffic.

---

## 3. Incident Response Runbook

### Objectives
* Provide a consistent response to availability, security, or performance incidents.

### Detection
* Alerts from Prometheus/Alertmanager, CloudWatch alarms, or synthetic monitors.
* Security notifications (suspicious login attempts, rate-limit breaches).

### Triage Steps
1. **Assess severity**
   * Determine impacted functionality (HTTP APIs vs. WebSocket messaging).
   * Check recent deployments or infrastructure changes.
2. **Stabilize the platform**
   * If overloaded, temporarily tighten rate limits or throttle concurrency by updating environment variables (`HTTP_RATE_LIMIT_PER_MINUTE`, `HTTP_MAX_CONCURRENT_REQUESTS`).
   * Use the new connection cap to pause additional WebSocket sessions if necessary.
   * Consider scaling out using the scaling runbook.
3. **Collect diagnostics**
   * Capture application logs (FastAPI/uvicorn) with correlation IDs from `RequestContextMiddleware`.
   * Export metrics snapshots and, if relevant, Redis keyspace stats.
   * Save ALB request logs if enabled.
4. **Mitigate**
   * Apply hotfixes or configuration changes via Terraform/CloudFormation/Helm.
   * Block abusive IPs at the load balancer security group if rate limits are insufficient.
   * Revoke compromised tokens using the admin revoke endpoint.
5. **Verify recovery**
   * Ensure health probes return `ready`, message flow resumes, and metrics normalize.
   * Run regression tests or smoke checks from the `example_client.py` tool.
6. **Post-incident review**
   * Document the timeline, root cause, and follow-up actions.
   * Update alerts or thresholds if detection lagged.

### Communication
* Notify stakeholders according to the incident severity matrix.
* Provide status updates at agreed intervals until resolution.

---

## Reference Links
* Deployment assets: `deploy/docker`, `deploy/helm`, `deploy/terraform`, `deploy/cloudformation`
* Observability: `/metrics` endpoint, Prometheus counters (`ERROR_COUNTER`, `MESSAGE_COUNTER`, `CONNECTION_GAUGE`)
* Authentication: `/auth/refresh`, `/admin/tokens/revoke`
