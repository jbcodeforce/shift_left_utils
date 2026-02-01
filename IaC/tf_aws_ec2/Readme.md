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

### 1. Install Required Tools

1. AWS CLI
	**On macOS (using Homebrew):**

	```bash
	# Install Terraform
	brew install terraform

	# Install AWS CLI
	brew install awscli
	```

	**On Linux:**

	```bash
	# Terraform
	wget -O- https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
	echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
	sudo apt update && sudo apt install terraform

	# AWS CLI
	curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
	unzip awscliv2.zip
	sudo ./aws/install
	```

2. **Terraform**: Install from [HashiCorp](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)

### 2. Configure AWS Credentials

1. **AWS CLI**: Configure with your credentials
   ```bash
   aws configure
   ```

	You will be prompted for:

	- **AWS Access Key ID**: Your access key
	- **AWS Secret Access Key**: Your secret key
	- **Default region**: `us-west-2` (recommended for free tier)
	- **Output format**: `yaml`

	To get AWS credentials:

	1. Log in to [AWS Console](https://console.aws.amazon.com/)
	2. Go to **IAM** > **Users** > **Your User** > **Security credentials**
	3. Click **Create access key**

3. **SSH Key Pair**: Create or use existing key pair
   ```bash
   # Create new key pair
   aws ec2 create-key-pair --key-name my-shift-left-key \
     --query 'KeyMaterial' --output text > ~/.ssh/my-shift-left-key.pem
   chmod 400 ~/.ssh/my-shift-left-key.pem
   ```

4. **Update Configuration**: Edit `terraform.tfvars`:
   ```sh
   ssh_key_name = "my-shift-left-key"  # Your key pair name
   aws_region   = "us-west-2"          # Your preferred region
   ```

### 3. Verify Configuration

```bash
# Check AWS CLI is configured
aws sts get-caller-identity

# Check Terraform is installed
terraform version
```

## Deployment

### Quick Start

```bash
cd deployment/ec2_tf

# Initialize Terraform
terraform init

# Preview what will be created
terraform plan

# Create the infrastructure
terraform apply
```

Type `yes` when prompted to confirm.

### Get Connection Details

```bash
terraform output ssh_command
terraform output ollama_api_url
terraform output quick_start
```

## Usage

### Connect to Instance

Use script:

```sh
./connect.sh
```

OR

```bash
# Use the SSH command from terraform output
ssh -i ~/.ssh/your-key.pem ec2-user@<public-ip>
```

Then within the EC2 instance:

```bash
./check_status.sh
```

This script displays:
- GPU status and memory usage
- Ollama service status
- Available models
- Model download progress
- shift_left version


```bash
source ./set_env.sh
```

### Run Shift Left CLI inside EC2

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

## Using Remote Ollama for AI Migration

The primary use case for this infrastructure is running AI-powered SQL migrations from your local machine using a remote GPU-accelerated Ollama server.

### Configure Remote Ollama

After deployment, get the Ollama API URL:

```bash
terraform output ollama_api_url
# Example: http://54.201.123.45:11434
```

### Local Machine Setup

On your local development machine, set the environment variable to point to the remote Ollama:

```bash
# Set the remote Ollama host
export OLLAMA_HOST="http://<EC2_PUBLIC_IP>:11434"

# Verify connectivity
curl $OLLAMA_HOST/api/tags
```

### Run AI Migration with shift_left CLI

With the remote Ollama configured, run migrations from your local machine:

```bash
# Navigate to your Flink project
cd /path/to/your/flink_project

# Run AI-powered SQL migration from KSQL to Flink SQL
shift_left table migrate my_table input.ksql output/ --source-type ksql --validate

# Migrate from Spark SQL
shift_left table migrate my_table spark_query.sql output/ --source-type spark --validate

# Migrate from dbt models
shift_left table migrate my_table dbt_model.sql output/ --source-type dbt
```

### Example Migration Workflow

```bash
# 1. Set up environment
export OLLAMA_HOST="http://$(terraform output -raw instance_public_ip):11434"
export PIPELINES="/path/to/your/pipelines"

# 2. Validate configuration
shift_left project validate-config

# 3. Migrate a KSQL stream to Flink SQL
shift_left table migrate customer_events \
  $SRC_FOLDER/customer_events.ksql \
  $STAGING \
  --source-type ksql \
  --validate \
  --recursive

# 4. Build table inventory
shift_left table build-inventory $PIPELINES

# 5. Deploy to Confluent Cloud
shift_left pipeline deploy $PIPELINES --table-name customer_events
```

### Performance Tips

- The 30B parameter model provides high-quality translations
- First inference after model load takes longer (model warm-up)
- Subsequent translations are faster due to GPU memory caching
- For batch migrations, process multiple tables sequentially to benefit from warm cache

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

Deploy into an existing VPC instead of creating new network infrastructure. This saves costs (no NAT Gateway) and integrates with your existing network setup.

#### Option A: Discover VPC by Name (Recommended)

The easiest approach - reference your VPC and subnet by their Name tags:

```hcl
# terraform.tfvars
use_existing_vpc     = true
existing_vpc_name    = "my-company-vpc"      # Name tag of your VPC
existing_subnet_name = "my-public-subnet"    # Name tag of public subnet
```

#### Option B: Auto-Discover Public Subnet

Let Terraform find a suitable public subnet automatically:

```hcl
# terraform.tfvars
use_existing_vpc     = true
existing_vpc_name    = "my-company-vpc"      # Name tag of your VPC
auto_discover_subnet = true                   # Finds first public subnet
```

This discovers subnets with `map_public_ip_on_launch=true`.

#### Option C: Use Explicit IDs

Reference resources by their AWS IDs:

```hcl
# terraform.tfvars
use_existing_vpc   = true
existing_vpc_id    = "vpc-0123456789abcdef0"
existing_subnet_id = "subnet-0123456789abcdef0"
```

#### Finding Your VPC and Subnet Information

```bash
# List VPCs with names
aws ec2 describe-vpcs \
  --query 'Vpcs[*].[VpcId,Tags[?Key==`Name`].Value|[0],CidrBlock]' \
  --output table

# List subnets in a VPC with public IP mapping status
aws ec2 describe-subnets \
  --filters "Name=vpc-id,Values=vpc-xxx" \
  --query 'Subnets[*].[SubnetId,Tags[?Key==`Name`].Value|[0],AvailabilityZone,MapPublicIpOnLaunch]' \
  --output table

# Find public subnets (those with map_public_ip_on_launch=true)
aws ec2 describe-subnets \
  --filters "Name=vpc-id,Values=vpc-xxx" "Name=map-public-ip-on-launch,Values=true" \
  --query 'Subnets[*].[SubnetId,Tags[?Key==`Name`].Value|[0]]' \
  --output table
```

#### Subnet Requirements

The subnet must:
- Have a route to an Internet Gateway (public subnet)
- Have `map_public_ip_on_launch=true` OR rely on the Elastic IP
- Allow outbound internet access for package installation

#### Security Group Requirements (if using existing)

If providing `existing_security_group_id`, ensure it allows:
- Inbound SSH (port 22) from your IP
- Inbound Ollama API (port 11434) from your IP
- All outbound traffic

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
