terraform {
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

resource "aws_vpc" "Main" {            # Creating VPC here
  cidr_block       = var.main_vpc_cidr # Defining the CIDR block use 10.0.0.0/24 for demo
  instance_tenancy = "default"
}

resource "aws_internet_gateway" "IGW" { # Creating Internet Gateway
  vpc_id = aws_vpc.Main.id              # vpc_id will be generated after we create VPC
}

resource "aws_subnet" "publicsubnets" { # Creating Public Subnets
  vpc_id                  = aws_vpc.Main.id
  cidr_block              = var.public_subnets # CIDR block of public subnets
  availability_zone       = var.availability_zone
  map_public_ip_on_launch = true

  tags = {
    Name = "Public Subnet"
  }
}

resource "aws_subnet" "privatesubnets" {
  vpc_id            = aws_vpc.Main.id
  cidr_block        = var.private_subnets # CIDR block of private subnets
  availability_zone = var.availability_zone

  tags = {
    Name = "Private Subnet"
  }
}

resource "aws_route_table" "PublicRT" { # Creating Route Table for Public Subnet
  vpc_id = aws_vpc.Main.id
  route {
    cidr_block = "0.0.0.0/0" # Traffic from Public Subnet reaches Internet via Internet Gateway
    gateway_id = aws_internet_gateway.IGW.id
  }
}

resource "aws_route_table" "PrivateRT" { # Creating RT for Private Subnet
  vpc_id = aws_vpc.Main.id
  route {
    cidr_block     = "0.0.0.0/0" # Traffic from Private Subnet reaches Internet via NAT Gateway
    nat_gateway_id = aws_nat_gateway.NATgw.id
  }
}

resource "aws_route_table_association" "PublicRTassociation" {
  subnet_id      = aws_subnet.publicsubnets.id
  route_table_id = aws_route_table.PublicRT.id
}

resource "aws_route_table_association" "PrivateRTassociation" {
  subnet_id      = aws_subnet.privatesubnets.id
  route_table_id = aws_route_table.PrivateRT.id
}

resource "aws_eip" "nateIP" {
  domain = "vpc"
}

resource "aws_nat_gateway" "NATgw" {
  allocation_id = aws_eip.nateIP.id
  subnet_id     = aws_subnet.publicsubnets.id
}

# Security Group for SSH and Ollama API access
resource "aws_security_group" "ollama_sg" {
  name_prefix = "ollama-sg"
  vpc_id      = aws_vpc.Main.id
  description = "Security group for Ollama EC2 instance"

  # SSH access
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "SSH access"
  }

  # Ollama API access
  ingress {
    from_port   = 11434
    to_port     = 11434
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
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
    Name = "ollama-security-group"
  }
}

# EC2 Instance for Ollama with GPU support
resource "aws_instance" "ollama_server" {
  ami                    = var.ami_id
  instance_type          = var.instance_type
  key_name               = var.ssh_api_key
  subnet_id              = aws_subnet.publicsubnets.id
  vpc_security_group_ids = [aws_security_group.ollama_sg.id]

  # Enhanced storage for GPU workloads and model storage
  root_block_device {
    volume_size = 200 # Increased for storing large models
    volume_type = "gp3"
    iops        = 3000
    throughput  = 125
    encrypted   = true
  }

  # User data script for setup
  user_data = file("${path.module}/setup.sh")

  # Enable detailed monitoring
  monitoring = true

  tags = {
    Name        = "Ollama-GPU-Server"
    Purpose     = "LLM-Inference"
    Environment = "Development"
  }
}

# Elastic IP for the instance
resource "aws_eip" "ollama_eip" {
  instance = aws_instance.ollama_server.id
  domain   = "vpc"

  tags = {
    Name = "ollama-server-eip"
  }
}
