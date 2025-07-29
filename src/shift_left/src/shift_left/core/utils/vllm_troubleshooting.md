# vLLM Troubleshooting Guide

## Error: 'aimv2' is already used by a Transformers config

This error occurs when there's a model name or configuration conflict in the Transformers library when loading the cogito model in vLLM.

### Solution 1: Use Full Model Path

Instead of using just the model name, use the full path or HuggingFace identifier:

```bash
# Instead of:
python -m vllm.entrypoints.openai.api_server --model cogito:32b

# Try:
python -m vllm.entrypoints.openai.api_server --model /path/to/cogito-32b-model

# Or with HuggingFace Hub:
python -m vllm.entrypoints.openai.api_server --model microsoft/cogito-32b
```

### Solution 2: Clear Transformers Cache

Clear the Transformers model cache to resolve conflicts:

```bash
# Clear HuggingFace cache
rm -rf ~/.cache/huggingface/

# Or use HuggingFace CLI
pip install huggingface-hub
huggingface-cli delete-cache

# Clear pip cache
pip cache purge
```

### Solution 3: Use Different Model Name

Use a different model name that doesn't conflict:

```bash
# Try with a different cogito variant
python -m vllm.entrypoints.openai.api_server --model cogito-instruct:32b

# Or use a completely different model for testing
python -m vllm.entrypoints.openai.api_server --model microsoft/DialoGPT-medium
```

### Solution 4: Update Environment Variables

Update the agent configuration to use the correct model name:

```python
# In your code, update the environment variable
import os
os.environ["VLLM_MODEL"] = "microsoft/cogito-32b"  # Use full identifier

# Or update the agent initialization
agent = KsqlToFlinkSqlVllmAgent()
agent.model_name = "microsoft/cogito-32b"  # Override default model name
```

### Solution 5: Use Ollama Instead

If vLLM continues to have issues, you can use Ollama as an alternative:

```bash
# Install Ollama
curl -fsSL https://ollama.ai/install.sh | sh

# Pull and run cogito model
ollama pull cogito:32b
ollama serve

# Update environment variables
export VLLM_BASE_URL="http://localhost:11434/v1"
export VLLM_MODEL="cogito:32b"
```

### Solution 6: Check Model Availability

Verify the cogito model is available and properly configured:

```python
# Check available models
from transformers import AutoConfig

try:
    config = AutoConfig.from_pretrained("microsoft/cogito-32b")
    print("Model config loaded successfully")
    print(f"Model type: {config.model_type}")
except Exception as e:
    print(f"Error loading model config: {e}")
```

### Solution 7: Use Alternative vLLM Startup

Try different vLLM startup options:

```bash
# Option 1: Disable trust_remote_code
python -m vllm.entrypoints.openai.api_server \
  --model cogito:32b \
  --trust-remote-code false

# Option 2: Specify tokenizer explicitly
python -m vllm.entrypoints.openai.api_server \
  --model cogito:32b \
  --tokenizer microsoft/cogito-32b-tokenizer

# Option 3: Use legacy mode
python -m vllm.entrypoints.openai.api_server \
  --model cogito:32b \
  --disable-log-stats \
  --max-model-len 4096
```

### Solution 8: Virtual Environment Reset

Create a fresh virtual environment:

```bash
# Create new virtual environment
python -m venv vllm_env
source vllm_env/bin/activate  # On Windows: vllm_env\Scripts\activate

# Install clean dependencies
pip install vllm transformers torch

# Try starting vLLM
python -m vllm.entrypoints.openai.api_server --model cogito:32b
```

### Solution 9: Update Agent for Alternative Models

If cogito model continues to have issues, update the agent to use alternative models:

```python
# Update the default model in the agent
class KsqlToFlinkSqlVllmAgent:
    def __init__(self):
        # Use a different model that's known to work with vLLM
        self.model_name = os.getenv("VLLM_MODEL", "microsoft/DialoGPT-large")
        # Or use a code-specific model
        # self.model_name = os.getenv("VLLM_MODEL", "Salesforce/codegen-350M-mono")
        self.vllm_base_url = os.getenv("VLLM_BASE_URL", "http://localhost:8000")
        self.vllm_api_key = os.getenv("VLLM_API_KEY", "token")
        self._load_prompts()
```

### Solution 10: Debug Mode

Enable debug mode to get more information:

```bash
# Run with debug logging
VLLM_LOG_LEVEL=DEBUG python -m vllm.entrypoints.openai.api_server \
  --model cogito:32b \
  --host 0.0.0.0 \
  --port 8000

# Or with Python logging
python -c "
import logging
logging.basicConfig(level=logging.DEBUG)
import vllm
# Your vLLM startup code here
"
```

## Quick Fix for Testing

If you need to test the agent immediately, use this quick workaround:

```bash
# Use a different model that's guaranteed to work
export VLLM_MODEL="gpt2"
python -m vllm.entrypoints.openai.api_server --model gpt2 --port 8000

# Test the agent
python test_vllm_agent.py
```

## Additional Resources

- [vLLM Documentation](https://docs.vllm.ai/)
- [Transformers Model Hub](https://huggingface.co/models)
- [vLLM GitHub Issues](https://github.com/vllm-project/vllm/issues)

Try these solutions in order, starting with Solution 1 (using full model path) as it's the most common fix for this type of error. 