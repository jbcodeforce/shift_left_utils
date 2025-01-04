#!/bin/bash

sudo yum update -y
sudo yum install git
curl -fsSL https://ollama.com/install.sh | sh
ollama pull qwen2.5-coder:32b  # 
ollama serve