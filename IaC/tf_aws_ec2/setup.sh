#!/bin/bash

# Log all output for debugging
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1
echo "Starting GPU EC2 setup for Ollama at $(date)"

# Update system packages
sudo yum update -y

# Install essential packages
sudo yum install -y git make wget curl htop nvtop

# Verify NVIDIA GPU is available
nvidia-smi
if [ $? -eq 0 ]; then
    echo "NVIDIA GPU detected successfully"
else
    echo "WARNING: NVIDIA GPU not detected or drivers not installed"
fi

# Install Docker (for potential containerized workloads)
sudo yum install -y docker
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -a -G docker ec2-user

# Install Ollama with GPU support
echo "Installing Ollama..."
curl -fsSL https://ollama.com/install.sh | sh

# Start Ollama service
sudo systemctl start ollama
sudo systemctl enable ollama

# Wait for Ollama to be ready
echo "Waiting for Ollama service to be ready..."
sleep 30

# Configure Ollama to listen on all interfaces (for API access)
sudo mkdir -p /etc/systemd/system/ollama.service.d
echo '[Service]' | sudo tee /etc/systemd/system/ollama.service.d/environment.conf
echo 'Environment="OLLAMA_HOST=0.0.0.0"' | sudo tee -a /etc/systemd/system/ollama.service.d/environment.conf
echo 'Environment="OLLAMA_ORIGINS=*"' | sudo tee -a /etc/systemd/system/ollama.service.d/environment.conf

# Reload and restart Ollama
sudo systemctl daemon-reload
sudo systemctl restart ollama

# Pull the model (this will take some time)
echo "Pulling qwen2.5-coder:32b model..."
ollama pull qwen2.5-coder:32b

# Verify the model is available
ollama list

# Clone the shift_left_utils repository
echo "Cloning shift_left_utils repository..."
cd /home/ec2-user
git clone https://github.com/jbcodeforce/shift_left_utils.git
chown -R ec2-user:ec2-user /home/ec2-user/shift_left_utils

# Create a simple health check script
cat > /home/ec2-user/check_ollama.sh << 'EOF'
#!/bin/bash
# Simple health check for Ollama
curl -s http://localhost:11434/api/tags | jq .
EOF
chmod +x /home/ec2-user/check_ollama.sh
chown ec2-user:ec2-user /home/ec2-user/check_ollama.sh

echo "Setup completed at $(date)"
echo "Ollama should be accessible at http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4):11434"
echo "Use 'ollama list' to see available models"
echo "Use 'nvidia-smi' to check GPU status"
