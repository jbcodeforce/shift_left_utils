variable "aws_region" {
  type        = string
  description = "The AWS region to deploy the resource to"
  default     = "us-west-2"
}

variable "main_vpc_cidr" {
  type        = string
  description = "CIDR block for the VPC"
}

variable "public_subnets" {
  type        = string
  description = "CIDR block for public subnet"
}

variable "private_subnets" {
  type        = string
  description = "CIDR block for private subnet"
}

variable "ssh_api_key" {
  type        = string
  description = "Name of the AWS key pair for SSH access"
}

variable "ami_id" {
  type        = string
  description = "AMI ID for the EC2 instance"
}

variable "instance_type" {
  type        = string
  description = "EC2 instance type"
  default     = "g5.4xlarge"
}

variable "availability_zone" {
  type        = string
  description = "Availability zone for subnets"
  default     = "us-west-2a"
}