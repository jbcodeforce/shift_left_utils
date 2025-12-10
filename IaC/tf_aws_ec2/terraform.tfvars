# =============================================================================
# Shift Left GPU EC2 - Variable Values
# =============================================================================
# Customize these values for your deployment.
# =============================================================================

# -----------------------------------------------------------------------------
# AWS Region & Availability Zone
# -----------------------------------------------------------------------------
aws_region        = "us-west-2"
availability_zone = "us-west-2a"

# -----------------------------------------------------------------------------
# VPC Network Configuration
# -----------------------------------------------------------------------------
main_vpc_cidr   = "10.0.0.0/24"
public_subnets  = "10.0.0.128/26"
private_subnets = "10.0.0.192/26"

# -----------------------------------------------------------------------------
# EC2 Instance Configuration
# -----------------------------------------------------------------------------

# Name of your AWS SSH key pair (must exist in the target region)
# Create one via: aws ec2 create-key-pair --key-name my-key --query 'KeyMaterial' --output text > my-key.pem
ssh_key_name = "j9r-keys"

# Deep Learning AMI with NVIDIA GPU support and CUDA pre-installed (us-west-2)
# Find the latest AMI:
#   aws ec2 describe-images --owners amazon \
#     --filters "Name=name,Values=Deep Learning AMI GPU PyTorch*Amazon Linux 2*" \
#     --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' --output text
ami_id = "ami-0c2d3e23c876c6093"

# Instance type with GPU and 64GB RAM
# g5.4xlarge: 16 vCPUs, 64GB RAM, 1x A10G GPU (24GB VRAM) - ~$1.01/hour
# For larger models, use g5.8xlarge (128GB RAM) or g5.12xlarge (4 GPUs)
instance_type = "g5.4xlarge"

# Instance naming
instance_name = "shift-left-gpu-server"
environment   = "Development"

# Root volume size in GB (200GB recommended for large LLM models)
root_volume_size = 200

# -----------------------------------------------------------------------------
# Security Configuration
# -----------------------------------------------------------------------------
# Restrict these CIDRs for production deployments

# SSH access - replace with your IP for security (e.g., ["203.0.113.0/32"])
allowed_ssh_cidrs = ["0.0.0.0/0"]

# Ollama API access - replace with your IP for security
allowed_ollama_cidrs = ["0.0.0.0/0"]

# -----------------------------------------------------------------------------
# Shift Left Configuration
# -----------------------------------------------------------------------------
shift_left_repo = "https://github.com/jbcodeforce/shift_left_utils.git"
ollama_model    = "qwen3-coder:30b"
