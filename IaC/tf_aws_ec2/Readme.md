# Shift Left GPU EC2 Infrastructure

This Terraform configuration creates a GPU-enabled EC2 instance for running the shift_left CLI with Ollama for AI-powered SQL migrations. The setup enables local access to large language models like Qwen to ensure data privacy and security during SQL transformation tasks.

The `qwen3-coder:30b` model provides excellent performance for transforming KSQL and Spark SQL to Flink SQL.

## Instance Specifications

| Specification | Value |
|--------------|-------|
| Instance Type | `g5.4xlarge` |
| vCPUs | 16 |
| RAM | 64 GB |
| GPU | 1x NVIDIA A10G (24 GB VRAM) |
| Storage | 200 GB GP3 SSD (encrypted) |
| OS | Deep Learning AMI (Amazon Linux 2) |
| Cost | ~$1.01/hour (on-demand) |

### Alternative Instance Types

| Instance Type | RAM | GPU | VRAM | Cost/hr | Use Case |
|--------------|-----|-----|------|---------|----------|
| g5.4xlarge | 64 GB | 1x A10G | 24 GB | $1.01 | Default - 32B models |
| g5.8xlarge | 128 GB | 1x A10G | 24 GB | $2.02 | Large context windows |
| g5.12xlarge | 192 GB | 4x A10G | 96 GB | $4.04 | 70B+ models |
| g4dn.4xlarge | 64 GB | 1x T4 | 16 GB | $1.20 | Budget option |

## Pre-installed Components

The setup script automatically installs:

- Git client
- Python 3.11 with pip
- uv (fast Python package manager)
- Docker
- Ollama with GPU support
- shift_left CLI (from repository)
- qwen2.5-coder:32b model (downloads in background)

## Prerequisites

1. **AWS CLI**: Configure with your credentials
   ```bash
   aws configure
   ```

2. **Terraform**: Install from [HashiCorp](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)

3. **SSH Key Pair**: Create or use existing key pair
   ```bash
   # Create new key pair
   aws ec2 create-key-pair --key-name my-shift-left-key \
     --query 'KeyMaterial' --output text > ~/.ssh/my-shift-left-key.pem
   chmod 400 ~/.ssh/my-shift-left-key.pem
   ```

4. **Update Configuration**: Edit `terraform.tfvars`:
   ```hcl
   ssh_key_name = "my-shift-left-key"  # Your key pair name
   aws_region   = "us-west-2"          # Your preferred region
   ```

## Deployment

### 1. Initialize Terraform

```bash
cd IaC/tf_aws_ec2
terraform init
```

### 2. Review Plan

```bash
terraform plan
```

### 3. Deploy

```bash
terraform apply
```

Type `yes` to confirm. Deployment takes 3-5 minutes.

### 4. Get Connection Details

```bash
terraform output ssh_command
terraform output ollama_api_url
terraform output quick_start
```

## Usage

### Connect to Instance

```bash
# Use the SSH command from terraform output
ssh -i ~/.ssh/your-key.pem ec2-user@<public-ip>
```

### Check Server Status

```bash
./check_status.sh
```

This shows:
- GPU status and memory usage
- Ollama service status
- Available models
- Model download progress
- shift_left version

### Set Up Environment

```bash
source ./set_env.sh
```

### Run Shift Left CLI

```bash
# Check version
shift_left version

# View available commands
shift_left --help

# Validate configuration
shift_left project validate-config

# Run SQL migration
shift_left table migrate my_table input.sql output/ --source-type ksql
```

### Test Ollama

```bash
# Quick test
./test_ollama.sh

# Manual API test
curl http://localhost:11434/api/generate -d '{
  "model": "qwen2.5-coder:32b",
  "prompt": "Write a Flink SQL query to deduplicate records",
  "stream": false
}' | jq .response
```

### Monitor Model Download

The 32B model (~20GB) downloads in the background after boot:

```bash
# Check progress
tail -f /var/log/ollama-pull.log

# Check if complete
ollama list
```

## Infrastructure Components

### Network
- VPC with public and private subnets
- Internet Gateway for public access
- NAT Gateway for private subnet outbound traffic
- Elastic IP for stable public address

### Security
- Security group with SSH (22) and Ollama API (11434) access
- Encrypted EBS volume
- IMDSv2 enabled

### Compute
- GPU-optimized EC2 instance
- 200GB high-IOPS storage for models
- Automated provisioning via user data

## Configuration Options

### Using an Existing VPC

To deploy into an existing VPC instead of creating new network infrastructure:

1. **Find your VPC and Subnet IDs**:
   ```bash
   # List VPCs
   aws ec2 describe-vpcs --query 'Vpcs[*].[VpcId,Tags[?Key==`Name`].Value|[0]]' --output table

   # List public subnets (must have internet gateway route)
   aws ec2 describe-subnets --filters "Name=vpc-id,Values=vpc-xxx" \
     --query 'Subnets[*].[SubnetId,AvailabilityZone,CidrBlock,Tags[?Key==`Name`].Value|[0]]' --output table
   ```

2. **Update `terraform.tfvars`**:
   ```hcl
   # Use existing VPC
   use_existing_vpc   = true
   existing_vpc_id    = "vpc-0123456789abcdef0"
   existing_subnet_id = "subnet-0123456789abcdef0"  # Must be a public subnet

   # Optional: use existing security group (leave empty to create new one)
   existing_security_group_id = ""
   ```

3. **Subnet Requirements**:
   - Must be in a public subnet with a route to an Internet Gateway
   - Must have `map_public_ip_on_launch` enabled OR use the Elastic IP
   - Must allow outbound internet access for package installation

4. **Security Group Requirements** (if using existing):
   - Allow inbound SSH (port 22)
   - Allow inbound Ollama API (port 11434)
   - Allow all outbound traffic

### Restrict Access (Recommended for Production)

Edit `terraform.tfvars` to limit access:

```hcl
# Only allow SSH from your IP
allowed_ssh_cidrs = ["203.0.113.50/32"]

# Only allow Ollama API from your IP
allowed_ollama_cidrs = ["203.0.113.50/32"]
```

### Change Model

Edit `terraform.tfvars`:

```hcl
ollama_model = "codellama:34b"  # or any supported model
```

Then update the model on the server:

```bash
ollama pull codellama:34b
```

### Increase Storage

Edit `terraform.tfvars`:

```hcl
root_volume_size = 300  # GB
```

## Cost Management

| Component | Estimated Cost | Notes |
|-----------|---------------|-------|
| g5.4xlarge | ~$1.01/hour | Main cost driver |
| 200GB GP3 | ~$20/month | Persistent storage |
| Elastic IP | Free while attached | Charged if unattached |
| NAT Gateway | ~$32/month | Only if creating new VPC |

### Cost-Saving Tips

1. **Use existing VPC**: Set `use_existing_vpc = true` to avoid NAT Gateway costs (~$32/month)
2. **Stop when not in use**: Stop the instance from AWS Console (EBS charges continue)
3. **Use terraform destroy**: Remove all resources when done
4. **Spot instances**: For non-critical work (requires manual configuration)

## Troubleshooting

### Ollama Not Accessible

```bash
# Check service status
sudo systemctl status ollama

# View logs
sudo journalctl -u ollama -f

# Restart service
sudo systemctl restart ollama
```

### GPU Not Detected

```bash
# Check NVIDIA drivers
nvidia-smi

# Reboot if needed
sudo reboot
```

### shift_left Not Found

```bash
# Source the environment
source ~/set_env.sh

# Or use full path
cd ~/shift_left_utils/src/shift_left
uv run shift_left version
```

### Model Download Failed

```bash
# Check logs
cat /var/log/ollama-pull.log

# Manual retry
ollama pull qwen2.5-coder:32b
```

### Logs Location

| Log | Location |
|-----|----------|
| Setup script | `/var/log/user-data.log` |
| Model download | `/var/log/ollama-pull.log` |
| Ollama service | `sudo journalctl -u ollama` |
| System | `/var/log/messages` |

## Cleanup

Remove all resources to stop charges:

```bash
terraform destroy
```

Type `yes` to confirm.

## Security Notes

- Default configuration allows public access to SSH and Ollama API
- For production, restrict CIDRs in `terraform.tfvars`
- Consider using AWS Systems Manager Session Manager instead of SSH
- Enable AWS CloudTrail for audit logging
- Use IAM roles instead of long-term credentials
