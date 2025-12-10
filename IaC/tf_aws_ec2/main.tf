# =============================================================================
# Shift Left Utils - GPU EC2 Infrastructure
# =============================================================================
# This Terraform configuration provisions an EC2 instance with:
# - GPU support (NVIDIA A10G) for LLM inference via Ollama
# - 64GB RAM for running large language models
# - Git, Python, uv, and shift_left CLI pre-installed
# - Ollama for local LLM inference (qwen2.5-coder model)
# =============================================================================

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.80"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# =============================================================================
# VPC Configuration
# =============================================================================

resource "aws_vpc" "main" {
  cidr_block           = var.main_vpc_cidr
  instance_tenancy     = "default"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "shift-left-vpc"
  }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "shift-left-igw"
  }
}

# =============================================================================
# Subnets
# =============================================================================

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = var.public_subnets
  availability_zone       = var.availability_zone
  map_public_ip_on_launch = true

  tags = {
    Name = "shift-left-public-subnet"
  }
}

resource "aws_subnet" "private" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = var.private_subnets
  availability_zone = var.availability_zone

  tags = {
    Name = "shift-left-private-subnet"
  }
}

# =============================================================================
# Route Tables
# =============================================================================

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }

  tags = {
    Name = "shift-left-public-rt"
  }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.nat.id
  }

  tags = {
    Name = "shift-left-private-rt"
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "private" {
  subnet_id      = aws_subnet.private.id
  route_table_id = aws_route_table.private.id
}

# =============================================================================
# NAT Gateway
# =============================================================================

resource "aws_eip" "nat" {
  domain = "vpc"

  tags = {
    Name = "shift-left-nat-eip"
  }
}

resource "aws_nat_gateway" "nat" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public.id

  tags = {
    Name = "shift-left-nat-gw"
  }

  depends_on = [aws_internet_gateway.igw]
}

# =============================================================================
# Security Group
# =============================================================================

resource "aws_security_group" "shift_left" {
  name_prefix = "shift-left-sg"
  vpc_id      = aws_vpc.main.id
  description = "Security group for Shift Left GPU EC2 instance"

  # SSH access
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.allowed_ssh_cidrs
    description = "SSH access"
  }

  # Ollama API access
  ingress {
    from_port   = 11434
    to_port     = 11434
    protocol    = "tcp"
    cidr_blocks = var.allowed_ollama_cidrs
    description = "Ollama API access"
  }

  # All outbound traffic
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "All outbound traffic"
  }

  tags = {
    Name = "shift-left-security-group"
  }

  lifecycle {
    create_before_destroy = true
  }
}

# =============================================================================
# EC2 Instance - GPU Server for Shift Left
# =============================================================================

resource "aws_instance" "shift_left_server" {
  ami                    = var.ami_id
  instance_type          = var.instance_type
  key_name               = var.ssh_key_name
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.shift_left.id]

  # Storage for models, code, and data
  root_block_device {
    volume_size           = var.root_volume_size
    volume_type           = "gp3"
    iops                  = 3000
    throughput            = 125
    encrypted             = true
    delete_on_termination = true
  }

  # User data script for setup
  user_data                   = file("${path.module}/setup.sh")
  user_data_replace_on_change = false

  # Enable detailed monitoring
  monitoring = true

  # Metadata options for IMDSv2
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "optional"
    http_put_response_hop_limit = 1
  }

  tags = {
    Name        = var.instance_name
    Purpose     = "Shift-Left-LLM-Migration"
    Environment = var.environment
    ManagedBy   = "Terraform"
  }

  volume_tags = {
    Name = "${var.instance_name}-root-volume"
  }
}

# =============================================================================
# Elastic IP for the instance
# =============================================================================

resource "aws_eip" "server" {
  instance = aws_instance.shift_left_server.id
  domain   = "vpc"

  tags = {
    Name = "${var.instance_name}-eip"
  }

  depends_on = [aws_internet_gateway.igw]
}
