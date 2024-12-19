#!/bin/bash

# Start Ollama in the background.
/bin/ollama serve &
# Record Process ID.
pid=$!

# Pause for Ollama to start.
sleep 5

ollama pull qwen2.5-coder:32b

# Wait for Ollama process to finish.
wait $pid