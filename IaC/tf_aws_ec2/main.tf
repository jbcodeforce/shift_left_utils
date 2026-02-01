# =============================================================================
# Shift Left Utils - GPU EC2 Infrastructure
# =============================================================================
# This Terraform configuration provisions an EC2 instance with:
# - GPU support (NVIDIA A10G) for LLM inference via Ollama
# - 64GB RAM for running large language models
# - Git, Python, uv, and shift_left CLI pre-installed
# - Ollama for local LLM inference
#
# Supports two modes:
# 1. Create new VPC (default): Creates all networking infrastructure
# 2. Use existing VPC: Set use_existing_vpc=true and provide VPC/subnet IDs
# =============================================================================

# =============================================================================
# Local Values - Determine which resources to use
# =============================================================================

locals {
  # Resolve VPC ID from various sources:
  # 1. Explicit ID provided
  # 2. Look up by name tag
  # 3. Create new VPC
  resolved_vpc_id = (
    var.existing_vpc_id != "" ? var.existing_vpc_id :
    var.existing_vpc_name != "" ? data.aws_vpc.by_name[0].id :
    null
  )

  # Resolve Subnet ID from various sources:
  # 1. Explicit ID provided
  # 2. Look up by name tag
  # 3. Auto-discover public subnet in VPC
  # 4. Create new subnet
  resolved_subnet_id = (
    var.existing_subnet_id != "" ? var.existing_subnet_id :
    var.existing_subnet_name != "" ? data.aws_subnet.by_name[0].id :
    var.auto_discover_subnet && length(try(data.aws_subnets.public[0].ids, [])) > 0 ? data.aws_subnets.public[0].ids[0] :
    null
  )

  # Final resource IDs to use
  vpc_id             = var.use_existing_vpc ? local.resolved_vpc_id : aws_vpc.main[0].id
  subnet_id          = var.use_existing_vpc ? local.resolved_subnet_id : aws_subnet.public[0].id
  security_group_ids = var.existing_security_group_id != "" ? [var.existing_security_group_id] : [aws_security_group.shift_left[0].id]

  # Determine if we need to create network resources
  create_vpc = !var.use_existing_vpc
  create_sg  = var.existing_security_group_id == ""

  # Availability zone - use from discovered subnet if available, otherwise from variable
  effective_az = var.use_existing_vpc && local.resolved_subnet_id != null ? (
    var.existing_subnet_id != "" ? data.aws_subnet.by_id[0].availability_zone :
    var.existing_subnet_name != "" ? data.aws_subnet.by_name[0].availability_zone :
    var.auto_discover_subnet && length(try(data.aws_subnets.public[0].ids, [])) > 0 ? data.aws_subnet.auto_discovered[0].availability_zone :
    var.availability_zone
  ) : var.availability_zone
}

# =============================================================================
# Data Sources - Look up existing resources when using existing VPC
# =============================================================================

# Look up VPC by ID (when explicit ID is provided)
data "aws_vpc" "by_id" {
  count = var.use_existing_vpc && var.existing_vpc_id != "" ? 1 : 0
  id    = var.existing_vpc_id
}

# Look up VPC by name tag (when name is provided instead of ID)
data "aws_vpc" "by_name" {
  count = var.use_existing_vpc && var.existing_vpc_id == "" && var.existing_vpc_name != "" ? 1 : 0

  filter {
    name   = "tag:Name"
    values = [var.existing_vpc_name]
  }

  filter {
    name   = "state"
    values = ["available"]
  }
}

# Look up subnet by ID (when explicit ID is provided)
data "aws_subnet" "by_id" {
  count = var.use_existing_vpc && var.existing_subnet_id != "" ? 1 : 0
  id    = var.existing_subnet_id
}

# Look up subnet by name tag (when name is provided instead of ID)
data "aws_subnet" "by_name" {
  count = var.use_existing_vpc && var.existing_subnet_id == "" && var.existing_subnet_name != "" ? 1 : 0

  filter {
    name   = "tag:Name"
    values = [var.existing_subnet_name]
  }

  filter {
    name   = "state"
    values = ["available"]
  }
}

# Auto-discover a public subnet in the VPC (when no specific subnet is provided)
data "aws_subnets" "public" {
  count = var.use_existing_vpc && var.existing_subnet_id == "" && var.existing_subnet_name == "" && var.auto_discover_subnet ? 1 : 0

  filter {
    name   = "vpc-id"
    values = [local.resolved_vpc_id]
  }

  filter {
    name   = "map-public-ip-on-launch"
    values = ["true"]
  }
}

# Get details of the first discovered public subnet
data "aws_subnet" "auto_discovered" {
  count = var.use_existing_vpc && var.existing_subnet_id == "" && var.existing_subnet_name == "" && var.auto_discover_subnet && length(try(data.aws_subnets.public[0].ids, [])) > 0 ? 1 : 0
  id    = data.aws_subnets.public[0].ids[0]
}

# Look up the Internet Gateway for the VPC (to verify subnet connectivity)
data "aws_internet_gateway" "existing" {
  count = var.use_existing_vpc ? 1 : 0

  filter {
    name   = "attachment.vpc-id"
    values = [local.resolved_vpc_id]
  }
}

# =============================================================================
# VPC Configuration (only created if use_existing_vpc = false)
# =============================================================================

resource "aws_vpc" "main" {
  count = local.create_vpc ? 1 : 0

  cidr_block           = var.main_vpc_cidr
  instance_tenancy     = "default"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "shift-left-vpc"
  }
}

resource "aws_internet_gateway" "igw" {
  count = local.create_vpc ? 1 : 0

  vpc_id = aws_vpc.main[0].id

  tags = {
    Name = "shift-left-igw"
  }
}

# =============================================================================
# Subnets (only created if use_existing_vpc = false)
# =============================================================================

resource "aws_subnet" "public" {
  count = local.create_vpc ? 1 : 0

  vpc_id                  = aws_vpc.main[0].id
  cidr_block              = var.public_subnets
  availability_zone       = var.availability_zone
  map_public_ip_on_launch = true

  tags = {
    Name = "shift-left-public-subnet"
  }
}

resource "aws_subnet" "private" {
  count = local.create_vpc ? 1 : 0

  vpc_id            = aws_vpc.main[0].id
  cidr_block        = var.private_subnets
  availability_zone = var.availability_zone

  tags = {
    Name = "shift-left-private-subnet"
  }
}

# =============================================================================
# Route Tables (only created if use_existing_vpc = false)
# =============================================================================

resource "aws_route_table" "public" {
  count = local.create_vpc ? 1 : 0

  vpc_id = aws_vpc.main[0].id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw[0].id
  }

  tags = {
    Name = "shift-left-public-rt"
  }
}

resource "aws_route_table" "private" {
  count = local.create_vpc ? 1 : 0

  vpc_id = aws_vpc.main[0].id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.nat[0].id
  }

  tags = {
    Name = "shift-left-private-rt"
  }
}

resource "aws_route_table_association" "public" {
  count = local.create_vpc ? 1 : 0

  subnet_id      = aws_subnet.public[0].id
  route_table_id = aws_route_table.public[0].id
}

resource "aws_route_table_association" "private" {
  count = local.create_vpc ? 1 : 0

  subnet_id      = aws_subnet.private[0].id
  route_table_id = aws_route_table.private[0].id
}

# =============================================================================
# NAT Gateway (only created if use_existing_vpc = false)
# =============================================================================

resource "aws_eip" "nat" {
  count = local.create_vpc ? 1 : 0

  domain = "vpc"

  tags = {
    Name = "shift-left-nat-eip"
  }
}

resource "aws_nat_gateway" "nat" {
  count = local.create_vpc ? 1 : 0

  allocation_id = aws_eip.nat[0].id
  subnet_id     = aws_subnet.public[0].id

  tags = {
    Name = "shift-left-nat-gw"
  }

  depends_on = [aws_internet_gateway.igw]
}

# =============================================================================
# Security Group (created unless existing_security_group_id is provided)
# =============================================================================

resource "aws_security_group" "shift_left" {
  count = local.create_sg ? 1 : 0

  name_prefix = "shift-left-sg"
  vpc_id      = local.vpc_id
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
  subnet_id              = local.subnet_id
  vpc_security_group_ids = local.security_group_ids

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

  # Only depend on IGW if we created it
  depends_on = [aws_internet_gateway.igw]
}

# =============================================================================
# Validation
# =============================================================================

# Ensure required variables are provided when using existing VPC
resource "null_resource" "validate_existing_vpc" {
  count = var.use_existing_vpc ? 1 : 0

  lifecycle {
    precondition {
      condition     = var.existing_vpc_id != "" || var.existing_vpc_name != ""
      error_message = "Either existing_vpc_id or existing_vpc_name must be provided when use_existing_vpc is true."
    }

    precondition {
      condition     = var.existing_subnet_id != "" || var.existing_subnet_name != "" || var.auto_discover_subnet
      error_message = "Either existing_subnet_id, existing_subnet_name, or auto_discover_subnet must be set when use_existing_vpc is true."
    }
  }
}

# Validate that auto-discovered subnet was found
resource "null_resource" "validate_auto_discover" {
  count = var.use_existing_vpc && var.auto_discover_subnet && var.existing_subnet_id == "" && var.existing_subnet_name == "" ? 1 : 0

  lifecycle {
    precondition {
      condition     = length(try(data.aws_subnets.public[0].ids, [])) > 0
      error_message = "No public subnet found in VPC with map_public_ip_on_launch=true. Please specify existing_subnet_id or existing_subnet_name."
    }
  }
}
