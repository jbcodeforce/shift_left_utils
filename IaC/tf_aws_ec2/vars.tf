# =============================================================================
# Variables for Shift Left GPU EC2 Infrastructure
# =============================================================================

# -----------------------------------------------------------------------------
# AWS Region Configuration
# -----------------------------------------------------------------------------

variable "aws_region" {
  type        = string
  description = "The AWS region to deploy resources"
  default     = "us-west-2"
}

variable "availability_zone" {
  type        = string
  description = "Availability zone for subnets (must match region)"
  default     = "us-west-2a"
}

# -----------------------------------------------------------------------------
# VPC Configuration
# -----------------------------------------------------------------------------

variable "main_vpc_cidr" {
  type        = string
  description = "CIDR block for the VPC"
  default     = "10.0.0.0/24"
}

variable "public_subnets" {
  type        = string
  description = "CIDR block for public subnet"
  default     = "10.0.0.128/26"
}

variable "private_subnets" {
  type        = string
  description = "CIDR block for private subnet"
  default     = "10.0.0.192/26"
}

# -----------------------------------------------------------------------------
# EC2 Instance Configuration
# -----------------------------------------------------------------------------

variable "instance_name" {
  type        = string
  description = "Name tag for the EC2 instance"
  default     = "shift-left-gpu-server"
}

variable "environment" {
  type        = string
  description = "Environment tag (Development, Staging, Production)"
  default     = "Development"
}

variable "ssh_key_name" {
  type        = string
  description = "Name of the AWS key pair for SSH access"
}

variable "ami_id" {
  type        = string
  description = <<-EOT
    AMI ID for the EC2 instance.
    Recommended: AWS Deep Learning AMI with NVIDIA drivers pre-installed.

    Find latest Deep Learning AMIs:
    aws ec2 describe-images --owners amazon --filters "Name=name,Values=Deep Learning AMI GPU PyTorch*" --query 'Images | sort_by(@, &CreationDate) | [-1]'

    Common AMI IDs (us-west-2):
    - ami-0c2d3e23c876c6093: Deep Learning AMI (Amazon Linux 2)
    - ami-0b20a6f09484773af: Deep Learning Base OSS Nvidia (Ubuntu 22.04)
  EOT
}

variable "instance_type" {
  type        = string
  description = <<-EOT
    EC2 instance type. Must be a GPU instance for LLM inference.

    Recommended GPU instances with 64GB+ RAM:

    | Instance Type | vCPUs | RAM    | GPU              | GPU Memory | Cost/hr  |
    |--------------|-------|--------|------------------|------------|----------|
    | g5.4xlarge   | 16    | 64 GB  | 1x NVIDIA A10G   | 24 GB      | ~$1.01   |
    | g5.8xlarge   | 32    | 128 GB | 1x NVIDIA A10G   | 24 GB      | ~$2.02   |
    | g5.12xlarge  | 48    | 192 GB | 4x NVIDIA A10G   | 96 GB      | ~$4.04   |
    | p3.2xlarge   | 8     | 61 GB  | 1x NVIDIA V100   | 16 GB      | ~$3.06   |
    | p3.8xlarge   | 32    | 244 GB | 4x NVIDIA V100   | 64 GB      | ~$12.24  |
    | g4dn.4xlarge | 16    | 64 GB  | 1x NVIDIA T4     | 16 GB      | ~$1.20   |

    Note: g5.4xlarge is recommended for shift_left with 32B parameter models.
    For larger models or faster inference, consider g5.8xlarge or g5.12xlarge.
  EOT
  default     = "g5.4xlarge"

  validation {
    condition = contains([
      "g5.xlarge", "g5.2xlarge", "g5.4xlarge", "g5.8xlarge", "g5.12xlarge", "g5.16xlarge", "g5.24xlarge", "g5.48xlarge",
      "g4dn.xlarge", "g4dn.2xlarge", "g4dn.4xlarge", "g4dn.8xlarge", "g4dn.12xlarge", "g4dn.16xlarge",
      "p3.2xlarge", "p3.8xlarge", "p3.16xlarge",
      "p4d.24xlarge",
      "p5.48xlarge"
    ], var.instance_type)
    error_message = "Instance type must be a valid GPU instance type (g5.*, g4dn.*, p3.*, p4d.*, or p5.*)."
  }
}

variable "root_volume_size" {
  type        = number
  description = "Size of the root EBS volume in GB (should be at least 200GB for large models)"
  default     = 200

  validation {
    condition     = var.root_volume_size >= 100
    error_message = "Root volume size must be at least 100 GB for storing LLM models."
  }
}

# -----------------------------------------------------------------------------
# Security Configuration
# -----------------------------------------------------------------------------

variable "allowed_ssh_cidrs" {
  type        = list(string)
  description = "List of CIDR blocks allowed to SSH to the instance. Restrict to your IP for security."
  default     = ["0.0.0.0/0"]
}

variable "allowed_ollama_cidrs" {
  type        = list(string)
  description = "List of CIDR blocks allowed to access Ollama API. Restrict for security."
  default     = ["0.0.0.0/0"]
}

# -----------------------------------------------------------------------------
# Shift Left Configuration
# -----------------------------------------------------------------------------

variable "shift_left_repo" {
  type        = string
  description = "Git repository URL for shift_left_utils"
  default     = "https://github.com/jbcodeforce/shift_left_utils.git"
}

variable "ollama_model" {
  type        = string
  description = "Ollama model to pull during setup"
  default     = "qwen3-coder:30b"
}
