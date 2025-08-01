# Outputs for easy access to instance information

output "instance_id" {
  description = "ID of the EC2 instance"
  value       = aws_instance.ollama_server.id
}

output "instance_public_ip" {
  description = "Public IP address of the EC2 instance"
  value       = aws_eip.ollama_eip.public_ip
}

output "instance_private_ip" {
  description = "Private IP address of the EC2 instance"
  value       = aws_instance.ollama_server.private_ip
}

output "ssh_command" {
  description = "SSH command to connect to the instance"
  value       = "ssh -i ~/.ssh/${var.ssh_api_key}.pem ec2-user@${aws_eip.ollama_eip.public_ip}"
}

output "ollama_api_url" {
  description = "Ollama API URL"
  value       = "http://${aws_eip.ollama_eip.public_ip}:11434"
}

output "security_group_id" {
  description = "ID of the security group"
  value       = aws_security_group.ollama_sg.id
}

output "vpc_id" {
  description = "ID of the VPC"
  value       = aws_vpc.Main.id
}

output "public_subnet_id" {
  description = "ID of the public subnet"
  value       = aws_subnet.publicsubnets.id
}