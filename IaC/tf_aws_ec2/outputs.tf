# =============================================================================
# Outputs for Shift Left GPU EC2 Infrastructure
# =============================================================================

# -----------------------------------------------------------------------------
# Instance Information
# -----------------------------------------------------------------------------

output "instance_id" {
  description = "ID of the EC2 instance"
  value       = aws_instance.shift_left_server.id
}

output "instance_public_ip" {
  description = "Public IP address of the EC2 instance (Elastic IP)"
  value       = aws_eip.server.public_ip
}

output "instance_private_ip" {
  description = "Private IP address of the EC2 instance"
  value       = aws_instance.shift_left_server.private_ip
}

output "instance_public_dns" {
  description = "Public DNS name of the EC2 instance"
  value       = aws_eip.server.public_dns
}

# -----------------------------------------------------------------------------
# Connection Information
# -----------------------------------------------------------------------------

output "ssh_command" {
  description = "SSH command to connect to the instance"
  value       = "ssh -i ~/.ssh/${var.ssh_key_name}.pem ec2-user@${aws_eip.server.public_ip}"
}

output "ollama_api_url" {
  description = "Ollama API URL for LLM inference"
  value       = "http://${aws_eip.server.public_ip}:11434"
}

output "ollama_api_curl_test" {
  description = "Command to test Ollama API"
  value       = "curl http://${aws_eip.server.public_ip}:11434/api/tags"
}

# -----------------------------------------------------------------------------
# Network Information
# -----------------------------------------------------------------------------

output "vpc_id" {
  description = "ID of the VPC (existing or created)"
  value       = local.vpc_id
}

output "subnet_id" {
  description = "ID of the subnet used by the instance"
  value       = local.subnet_id
}

output "security_group_id" {
  description = "ID of the security group"
  value       = local.security_group_ids[0]
}

output "using_existing_vpc" {
  description = "Whether using an existing VPC or a newly created one"
  value       = var.use_existing_vpc
}

# -----------------------------------------------------------------------------
# Instance Specifications
# -----------------------------------------------------------------------------

output "instance_type" {
  description = "EC2 instance type"
  value       = var.instance_type
}

output "instance_specs" {
  description = "Instance specifications summary"
  value       = <<-EOT
    Instance Type: ${var.instance_type}

    For g5.4xlarge:
    - vCPUs: 16
    - RAM: 64 GB
    - GPU: 1x NVIDIA A10G (24 GB VRAM)
    - Storage: ${var.root_volume_size} GB gp3

    Estimated Cost: ~$1.01/hour (on-demand)
  EOT
}

# -----------------------------------------------------------------------------
# Quick Start Guide
# -----------------------------------------------------------------------------

output "quick_start" {
  description = "Quick start guide for using the instance"
  value       = <<-EOT

    === Shift Left GPU Server Quick Start ===

    1. Connect to the instance:
       ${format("ssh -i ~/.ssh/%s.pem ec2-user@%s", var.ssh_key_name, aws_eip.server.public_ip)}

    2. Check server status:
       ./check_status.sh

    3. Set up environment:
       source ./set_env.sh

    4. Use shift_left CLI:
       shift_left version
       shift_left --help

    5. Test Ollama:
       ./test_ollama.sh

    Note: The LLM model downloads in the background.
    This takes 30-60 minutes. Check progress:
       tail -f /var/log/ollama-pull.log

    Ollama API Endpoint: http://${aws_eip.server.public_ip}:11434

  EOT
}
