main_vpc_cidr     = "10.0.0.0/24"
public_subnets    = "10.0.0.128/26"
private_subnets   = "10.0.0.192/26"
aws_region        = "us-west-2"
availability_zone = "us-west-2a"
ssh_api_key       = "j9r-keys"
# Deep Learning AMI with NVIDIA GPU support and CUDA pre-installed
ami_id        = "ami-0c2d3e23c876c6093" # Deep Learning AMI (Amazon Linux 2) in us-west-2
instance_type = "g5.4xlarge"            # 16 vCPUs, 64GB RAM, 1x A10G GPU (24GB VRAM) - $1.01/hour
