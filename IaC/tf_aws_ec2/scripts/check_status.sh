
#!/bin/bash
echo "===== GPU Status ====="
nvidia-smi || echo "nvidia-smi not found or no GPU attached."

echo ""
echo "===== Ollama Service Status ====="
systemctl status ollama --no-pager || echo "Ollama service status not available."

echo ""
echo "===== Available Ollama Models ====="
curl -s http://localhost:11434/api/tags | jq . || echo "Could not query Ollama API or jq not installed."

echo ""
echo "===== Model Download Progress ====="
curl -s http://localhost:11434/api/tags | jq '.models[] | {name, status, size, downloaded, progress}' || echo "No models found or jq not installed."

echo ""
echo "===== shift_left Version ====="
shift_left version || echo "shift_left CLI not found."
