# AWS EC2 for Ollama and Shift Left tools

This Terraform configuration creates a GPU-enabled EC2 instance optimized for running Ollama with large language models. Using Ollama enables local access to large language models like Llama, Qwen, Cogito, and Mistral to ensure data privacy and security. For the Shift Left project, we can run SQL migrations table by table using an EC2 with sufficient GPU resources.

The `qwen2.5-coder:32b` model has excellent performance on code generation benchmarks and provides outstanding results for transforming KSQL and Spark SQL to Flink SQL. Future solutions may include fine-tuned models specifically for Flink SQL or RAG implementations.

## GPU-Optimized EC2 Configuration

### Instance Specifications
- **Instance Type**: `g5.4xlarge` - Optimized for GPU workloads ($1.01/hour)
- **vCPUs**: 16
- **RAM**: 64GB
- **GPU**: 1x NVIDIA A10G with 24GB VRAM
- **Storage**: 200GB GP3 SSD (encrypted, high IOPS)
- **OS**: Deep Learning AMI (Amazon Linux 2) with pre-installed NVIDIA drivers and CUDA

### GPU Advantages for Ollama
- **Faster Model Loading**: GPU memory allows faster model initialization
- **Improved Inference Speed**: 10-50x faster than CPU-only inference
- **Larger Model Support**: 24GB VRAM supports models up to 32B parameters
- **Concurrent Processing**: Better handling of multiple API requests

### AMI Details
The configuration uses AWS Deep Learning AMI with:
- Pre-installed NVIDIA drivers (latest stable)
- CUDA toolkit and cuDNN libraries
- Docker support for containerized workloads
- Optimized for machine learning frameworks

**Current AMI**: `ami-0c2d3e23c876c6093` (us-west-2 region)
*Note: If you change regions, update the AMI ID in terraform.tfvars*

## Infrastructure Components

The Terraform configuration creates:

### Network Infrastructure
- **VPC**: Isolated virtual network with CIDR 10.0.0.0/24
- **Public Subnet**: For the EC2 instance with internet access
- **Private Subnet**: For future scalability and security
- **Internet Gateway**: Enables public internet access
- **NAT Gateway**: Allows private subnet outbound internet access
- **Route Tables**: Properly configured routing for both subnets

### Security
- **Security Group**: Configured for:
  - SSH access (port 22) from anywhere
  - Ollama API access (port 11434) from anywhere
  - All outbound traffic allowed
- **Elastic IP**: Static public IP for consistent access

### Compute Resources
- **GPU-enabled EC2 Instance**: Optimized for Ollama LLM inference
- **Enhanced Storage**: 200GB GP3 with high IOPS for model storage
- **Automated Setup**: User data script installs and configures Ollama

## Prerequisites

1. **AWS CLI**: Configure with your AWS credentials
   ```bash
   aws configure
   ```

2. **Terraform CLI**: Install from [HashiCorp's website](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)

3. **SSH Key Pair**: Create or use existing AWS key pair
   - Update `ssh_api_key` in `terraform.tfvars` with your key name
   - Ensure you have the corresponding `.pem` file locally

4. **Update Configuration**: Modify `terraform.tfvars` as needed:
   ```hcl
   ssh_api_key = "your-key-name"  # Replace with your AWS key pair name
   aws_region = "us-west-2"       # Change if needed
   ```

## Deployment Instructions

### 1. Initialize Terraform
```bash
cd IaC/tf_aws_ec2
terraform init
```

### 2. Plan the Deployment
```bash
terraform plan
```
Review the planned resources to ensure everything looks correct.

### 3. Deploy the Infrastructure
```bash
terraform apply
```
Type `yes` when prompted to confirm the deployment.

### 4. Access Your Instance
After deployment, Terraform will output connection details:
```bash
# Get connection information
terraform output ssh_command
terraform output ollama_api_url

# Connect via SSH
ssh -i ~/.ssh/your-key.pem ec2-user@<public-ip>
```

### 5. Verify Ollama Installation
```bash
# Check GPU status
nvidia-smi

# Check Ollama status
sudo systemctl status ollama

# List available models
ollama list

# Test API (from local machine)
curl http://<public-ip>:11434/api/tags
```

## Usage Examples

### Using Ollama API
```bash
# Generate code completion
curl -X POST http://<public-ip>:11434/api/generate \
  -H "Content-Type: application/json" \
  -d '{
    "model": "qwen2.5-coder:32b",
    "prompt": "Convert this KSQL to Flink SQL: CREATE STREAM...",
    "stream": false
  }'
```

### SSH into Instance
```bash
# Connect to instance
ssh -i ~/.ssh/your-key.pem ec2-user@<public-ip>

# Check system resources
htop
nvtop  # GPU monitoring

# Manage Ollama
sudo systemctl status ollama
sudo systemctl restart ollama
```

## Cost Optimization

- **Instance Cost**: ~$1.01/hour for g5.4xlarge
- **Storage Cost**: ~$20/month for 200GB GP3
- **Data Transfer**: Minimal for API usage

**Cost-Saving Tips**:
- Stop instance when not in use: `terraform destroy`
- Use spot instances for development (manual configuration)
- Monitor usage with AWS Cost Explorer

## Troubleshooting

### Common Issues

1. **Ollama not accessible**:
   ```bash
   # Check service status
   sudo systemctl status ollama
   
   # Check logs
   sudo journalctl -u ollama -f
   
   # Restart service
   sudo systemctl restart ollama
   ```

2. **GPU not detected**:
   ```bash
   # Check NVIDIA drivers
   nvidia-smi
   
   # Reinstall if needed
   sudo yum update -y
   sudo reboot
   ```

3. **Model loading issues**:
   ```bash
   # Check disk space
   df -h
   
   # Re-pull model
   ollama pull qwen2.5-coder:32b
   ```

### Logs and Monitoring
- **User Data Logs**: `/var/log/user-data.log`
- **Ollama Logs**: `sudo journalctl -u ollama`
- **System Logs**: `/var/log/messages`

## Cleanup

To destroy all resources and avoid ongoing costs:
```bash
terraform destroy
```
Type `yes` when prompted to confirm destruction.

## Security Considerations

- The security group allows public access to ports 22 and 11434
- For production use, consider:
  - Restricting SSH access to specific IP ranges
  - Using a bastion host for SSH access
  - Implementing API authentication for Ollama
  - Using AWS Systems Manager Session Manager instead of SSH