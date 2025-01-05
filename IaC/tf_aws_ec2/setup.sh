#!/bin/bash

sudo yum update -y
sudo yum install git
sudo yum install make
curl -fsSL https://ollama.com/install.sh | sh
ollama pull qwen2.5-coder:32b  # 
ollama serve
git clone https://github.com/jbcodeforce/shift_left_utils