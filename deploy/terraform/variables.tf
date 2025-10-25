variable "aws_region" {
  description = "AWS region to deploy to"
  type        = string
}

variable "environment" {
  description = "Environment name (e.g. dev, staging, prod)"
  type        = string
}

variable "vpc_id" {
  description = "ID of the VPC to deploy resources into"
  type        = string
}

variable "public_subnet_ids" {
  description = "List of public subnet IDs for the load balancer"
  type        = list(string)
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs for the ECS service"
  type        = list(string)
}

variable "container_image" {
  description = "Container image for the Swifty server"
  type        = string
}

variable "container_port" {
  description = "Container port for the application"
  type        = number
  default     = 8000
}

variable "desired_count" {
  description = "Number of ECS tasks to run"
  type        = number
  default     = 2
}

variable "max_count" {
  description = "Maximum number of ECS tasks for autoscaling"
  type        = number
  default     = 10
}

variable "certificate_arn" {
  description = "ACM certificate ARN for TLS termination"
  type        = string
}

variable "jwt_secret_ssm_parameter" {
  description = "SSM parameter name containing the JWT secret"
  type        = string
}

variable "redis_endpoint" {
  description = "Redis endpoint hostname"
  type        = string
}

variable "redis_port" {
  description = "Redis port"
  type        = number
  default     = 6379
}

variable "redis_password_ssm_parameter" {
  description = "Optional SSM parameter containing the Redis password"
  type        = string
  default     = ""
}

variable "rate_limit" {
  description = "HTTP request rate limit per minute"
  type        = number
  default     = 600
}

variable "rate_limit_window" {
  description = "HTTP rate limit window in seconds"
  type        = number
  default     = 60
}

variable "http_max_concurrency" {
  description = "Maximum concurrent HTTP requests"
  type        = number
  default     = 200
}

variable "http_throttle_timeout" {
  description = "Throttle timeout in seconds"
  type        = number
  default     = 0.25
}

variable "websocket_message_rate" {
  description = "WebSocket messages allowed per client per window"
  type        = number
  default     = 900
}

variable "websocket_message_window" {
  description = "WebSocket message window size in seconds"
  type        = number
  default     = 60
}

variable "websocket_max_connections" {
  description = "Maximum concurrent WebSocket connections"
  type        = number
  default     = 4000
}

variable "allowed_ingress_cidrs" {
  description = "CIDR blocks allowed to access the load balancer"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "cpu" {
  description = "CPU units for the Fargate task"
  type        = number
  default     = 512
}

variable "memory" {
  description = "Memory (MiB) for the Fargate task"
  type        = number
  default     = 1024
}

variable "log_retention_days" {
  description = "CloudWatch log retention"
  type        = number
  default     = 30
}

variable "health_check_path" {
  description = "Path used for ALB health checks"
  type        = string
  default     = "/health/ready"
}
