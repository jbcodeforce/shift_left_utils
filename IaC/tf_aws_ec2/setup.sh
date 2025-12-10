#!/bin/bash
# =============================================================================
# Shift Left GPU EC2 Setup Script
# =============================================================================
# This script runs as user_data during EC2 instance initialization.
# It installs all dependencies required for running shift_left with Ollama.
# =============================================================================

set -e

# Log all output for debugging
exec > >(tee /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1
echo "=========================================="
echo "Starting Shift Left GPU EC2 setup"
echo "Timestamp: $(date)"
echo "=========================================="

# =============================================================================
# System Updates and Base Packages
# =============================================================================

echo "[1/8] Updating system packages..."
sudo yum update -y

echo "[2/8] Installing essential packages..."
sudo yum install -y \
    git \
    make \
    wget \
    curl \
    htop \
    nvtop \
    jq \
    unzip \
    tar \
    gzip \
    bzip2 \
    openssl-devel \
    bzip2-devel \
    libffi-devel \
    zlib-devel \
    readline-devel \
    sqlite-devel \
    xz-devel \
    tk-devel

# =============================================================================
# Verify NVIDIA GPU
# =============================================================================

echo "[3/8] Verifying NVIDIA GPU..."
if nvidia-smi; then
    echo "NVIDIA GPU detected successfully"
    nvidia-smi --query-gpu=name,memory.total,driver_version --format=csv
else
    echo "WARNING: NVIDIA GPU not detected or drivers not installed"
    echo "The Deep Learning AMI should have drivers pre-installed."
    echo "If using a different AMI, install NVIDIA drivers manually."
fi

# =============================================================================
# Install Docker
# =============================================================================


#echo "[4/8] Installing Docker..."
#sudo yum install -y docker
#sudo systemctl start docker
#sudo systemctl enable docker
#sudo usermod -a -G docker ec2-user

# =============================================================================
# Install Python 3.11 and uv
# =============================================================================

echo "[5/8] Installing Python 3.11 and uv..."

# Install Python 3.11 from Amazon Linux extras or compile if needed
if command -v python3.11 &>/dev/null; then
    echo "Python 3.11 already installed"
else
    # Try Amazon Linux extras first
    sudo amazon-linux-extras enable python3.11 2>/dev/null || true
    sudo yum install -y python3.11 python3.11-pip 2>/dev/null || {
        echo "Installing Python 3.11 from source..."
        cd /tmp
        wget https://www.python.org/ftp/python/3.11.9/Python-3.11.9.tgz
        tar xzf Python-3.11.9.tgz
        cd Python-3.11.9
        ./configure --enable-optimizations --prefix=/usr/local
        make -j$(nproc)
        sudo make altinstall
        cd /
        rm -rf /tmp/Python-3.11.9*
    }
fi

# Verify Python installation
python3.11 --version || python3 --version

# Install uv (fast Python package installer)
echo "Installing uv..."
curl -LsSf https://astral.sh/uv/install.sh | sh

# Add uv to PATH for all users
echo 'export PATH="$HOME/.local/bin:$PATH"' | sudo tee /etc/profile.d/uv.sh
source /etc/profile.d/uv.sh

# Also add for ec2-user
echo 'export PATH="$HOME/.local/bin:$PATH"' >> /home/ec2-user/.bashrc

# =============================================================================
# Install Ollama
# =============================================================================

echo "[6/8] Installing Ollama..."
curl -fsSL https://ollama.com/install.sh | sh

# Start Ollama service
sudo systemctl start ollama
sudo systemctl enable ollama

# Wait for Ollama to be ready
echo "Waiting for Ollama service to start..."
sleep 15

# Configure Ollama to listen on all interfaces (for API access)
sudo mkdir -p /etc/systemd/system/ollama.service.d
cat <<EOF | sudo tee /etc/systemd/system/ollama.service.d/environment.conf
[Service]
Environment="OLLAMA_HOST=0.0.0.0"
Environment="OLLAMA_ORIGINS=*"
Environment="OLLAMA_KEEP_ALIVE=24h"
EOF

# Reload and restart Ollama with new configuration
sudo systemctl daemon-reload
sudo systemctl restart ollama

# Wait for Ollama to be fully ready
echo "Waiting for Ollama to be fully ready..."
for i in {1..30}; do
    if curl -s http://localhost:11434/api/tags >/dev/null 2>&1; then
        echo "Ollama is ready!"
        break
    fi
    echo "Waiting for Ollama... ($i/30)"
    sleep 5
done

# =============================================================================
# Clone and Setup Shift Left Utils
# =============================================================================

echo "[7/8] Setting up shift_left_utils..."
cd /home/ec2-user

# Clone the repository
git clone https://github.com/jbcodeforce/shift_left_utils.git
chown -R ec2-user:ec2-user shift_left_utils

# Install shift_left CLI
cd /home/ec2-user/shift_left_utils/src/shift_left
sudo -u ec2-user /home/ec2-user/.local/bin/uv sync 2>/dev/null || {
    # Fallback if uv not in path yet
    export PATH="/home/ec2-user/.local/bin:$PATH"
    sudo -u ec2-user uv sync
}

# Create a wrapper script for shift_left
cat <<'WRAPPER' | sudo tee /usr/local/bin/shift_left
#!/bin/bash
cd /home/ec2-user/shift_left_utils/src/shift_left
exec uv run shift_left "$@"
WRAPPER
sudo chmod +x /usr/local/bin/shift_left

# =============================================================================
# Pull Ollama Model (runs in background to not block boot)
# =============================================================================

echo "[8/8] Pulling Ollama model (background)..."
# Pull model in background since it can take 30+ minutes for large models
nohup bash -c 'sleep 60 && ollama pull qwen3-coder:30b' > /var/log/ollama-pull.log 2>&1 &

# =============================================================================
# Create Helper Scripts
# =============================================================================

# Health check script
cat <<'EOF' > /home/ec2-user/check_status.sh
#!/bin/bash
echo "=========================================="
echo "Shift Left GPU Server Status"
echo "=========================================="
echo ""
echo "--- GPU Status ---"
nvidia-smi --query-gpu=name,memory.used,memory.total,utilization.gpu --format=csv,noheader
echo ""
echo "--- Ollama Status ---"
systemctl is-active ollama && echo "Ollama service: Running" || echo "Ollama service: Not running"
echo ""
echo "--- Available Models ---"
curl -s http://localhost:11434/api/tags | jq -r '.models[]?.name // "No models installed yet"' 2>/dev/null || echo "Ollama not responding"
echo ""
echo "--- Model Pull Progress ---"
if pgrep -f "ollama pull" > /dev/null; then
    echo "Model download in progress. Check: tail -f /var/log/ollama-pull.log"
else
    echo "No model download in progress"
fi
echo ""
echo "--- Shift Left Version ---"
shift_left version 2>/dev/null || echo "shift_left not yet available in PATH (try: source ~/.bashrc)"
echo ""
echo "=========================================="
EOF
chmod +x /home/ec2-user/check_status.sh

# Quick test script for Ollama
cat <<'EOF' > /home/ec2-user/test_ollama.sh
#!/bin/bash
echo "Testing Ollama API..."
curl -s http://localhost:11434/api/generate -d '{
  "model": "qwen3-coder:30b",
  "prompt": "Write a simple Python function that adds two numbers",
  "stream": false
}' | jq -r '.response'
EOF
chmod +x /home/ec2-user/test_ollama.sh

# Environment setup script
cat <<'EOF' > /home/ec2-user/set_env.sh
#!/bin/bash
# Source this file to set up the shift_left environment
export PATH="$HOME/.local/bin:$PATH"
export OLLAMA_HOST="http://localhost:11434"
cd $HOME/shift_left_utils
echo "Environment configured. shift_left CLI ready."
EOF
chmod +x /home/ec2-user/set_env.sh

# Set ownership
chown -R ec2-user:ec2-user /home/ec2-user/*.sh

# =============================================================================
# Setup Complete
# =============================================================================

PUBLIC_IP=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "N/A")

echo "=========================================="
echo "Setup completed at $(date)"
echo "=========================================="
echo ""
echo "Instance Information:"
echo "  Public IP: $PUBLIC_IP"
echo "  Ollama API: http://$PUBLIC_IP:11434"
echo ""
echo "Quick Start:"
echo "  1. SSH: ssh -i your-key.pem ec2-user@$PUBLIC_IP"
echo "  2. Check status: ./check_status.sh"
echo "  3. Source environment: source ./set_env.sh"
echo "  4. Run shift_left: shift_left version"
echo ""
echo "Note: The qwen3-coder:30b model is downloading in the background."
echo "This may take 30-60 minutes. Check progress: tail -f /var/log/ollama-pull.log"
echo "=========================================="
