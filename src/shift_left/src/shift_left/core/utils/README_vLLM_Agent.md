# vLLM-based KSQL to Flink SQL Code Agent

This module provides a new implementation of the KSQL to Flink SQL translation agent using vLLM with the cogito model, offering an alternative to the OpenAI-based implementation.

## Overview

The `KsqlToFlinkSqlVllmAgent` class provides the same functionality as the original `KsqlToFlinkSqlAgent` but uses vLLM (Very Large Language Model) inference server with the cogito model instead of OpenAI's API.

## Features

- **vLLM Integration**: Uses vLLM inference server for local or self-hosted model deployment
- **Cogito Model Support**: Optimized for cogito:32b model for SQL translation tasks
- **Structured Output**: JSON-based structured responses using Pydantic models
- **Multi-statement Support**: Handles complex KSQL scripts with multiple CREATE statements
- **Error Refinement**: Iterative error correction based on Confluent Cloud feedback
- **Same API**: Drop-in replacement for the OpenAI-based agent

## Architecture

The agent follows the same multi-agent workflow as the original:

1. **Table Detection Agent**: Analyzes input to detect multiple CREATE statements
2. **Translator Agent**: Converts KSQL to Flink SQL with proper streaming semantics
3. **Mandatory Validation Agent**: Validates and fixes common syntax issues
4. **Refinement Agent**: Handles error feedback and applies targeted fixes

## Setup and Configuration

### 1. Install vLLM

```bash
pip install vllm
```

### 2. Start vLLM Server

```bash
# Start vLLM server with cogito model
python -m vllm.entrypoints.openai.api_server \
  --model cogito:32b \
  --host 0.0.0.0 \
  --port 8000 \
  --api-key token
```

### 3. Environment Variables

```bash
export VLLM_MODEL="cogito:32b"           # Model name
export VLLM_BASE_URL="http://localhost:8000"  # vLLM server URL
export VLLM_API_KEY="token"              # API key for authentication
```

## Usage

### Basic Usage

```python
from shift_left.core.utils.ksql_vllm_code_agent import KsqlToFlinkSqlVllmAgent

# Initialize the agent
agent = KsqlToFlinkSqlVllmAgent()

# KSQL input
ksql_input = """
CREATE STREAM movements (
    person VARCHAR KEY, 
    location VARCHAR
) WITH (
    VALUE_FORMAT='JSON', 
    KAFKA_TOPIC='movements'
);
"""

# Translate to Flink SQL
ddl_statements, dml_statements = agent.translate_from_ksql_to_flink_sql(
    ksql_input, 
    validate=False  # Set to True for Confluent Cloud validation
)

# Output results
for ddl in ddl_statements:
    print("DDL:", ddl)
    
for dml in dml_statements:
    print("DML:", dml)
```

### Advanced Usage with Validation

```python
# Enable Confluent Cloud validation
ddl_statements, dml_statements = agent.translate_from_ksql_to_flink_sql(
    ksql_input, 
    validate=True  # Requires Confluent Cloud configuration
)
```

### Multiple Statements

```python
ksql_input = """
CREATE STREAM raw_events (id VARCHAR KEY, data VARCHAR) 
WITH (VALUE_FORMAT='JSON', KAFKA_TOPIC='raw_events');

CREATE TABLE processed_events WITH (VALUE_FORMAT='AVRO') AS
SELECT id, UCASE(data) as processed_data
FROM raw_events
GROUP BY id
EMIT CHANGES;
"""

ddl_statements, dml_statements = agent.translate_from_ksql_to_flink_sql(ksql_input)
```

## Testing

Run the test suite to verify the setup:

```bash
cd shift_left_utils/src/shift_left/src/shift_left/core/utils/
python test_vllm_agent.py
```

The test script includes:
- vLLM server connectivity check
- Simple KSQL stream translation
- Complex aggregation patterns
- Multiple statement handling

## Prompt Templates

The agent uses specialized prompt templates optimized for vLLM and the cogito model:

- `prompts/ksql_vllm/translator.txt` - Main translation logic
- `prompts/ksql_vllm/table_detection.txt` - Multi-statement detection
- `prompts/ksql_vllm/mandatory_validation.txt` - Syntax validation
- `prompts/ksql_vllm/refinement.txt` - Error correction

## Differences from OpenAI Agent

| Feature | OpenAI Agent | vLLM Agent |
|---------|--------------|------------|
| Model Provider | OpenAI API | Self-hosted vLLM |
| Model | GPT-4, GPT-3.5 | cogito:32b |
| API Format | OpenAI format | OpenAI-compatible |
| Deployment | Cloud-based | Local/self-hosted |
| Cost | Per-token pricing | Infrastructure cost |
| Privacy | Data sent to OpenAI | Data stays local |
| Customization | Limited | Full model control |

## Performance Considerations

- **Model Size**: cogito:32b requires significant GPU memory (recommended: 24GB+ VRAM)
- **Inference Speed**: Depends on hardware; typically 20-100 tokens/second
- **Concurrent Requests**: vLLM supports batched inference for better throughput
- **Memory Usage**: Monitor GPU memory usage during operation

## Troubleshooting

### Common Issues

1. **Connection Errors**
   ```
   vLLM request failed: Connection refused
   ```
   - Ensure vLLM server is running
   - Check VLLM_BASE_URL configuration
   - Verify port accessibility

2. **Model Loading Errors**
   ```
   Model 'cogito:32b' not found
   ```
   - Ensure model is downloaded and available
   - Check model path configuration
   - Verify model compatibility with vLLM

3. **Out of Memory Errors**
   ```
   CUDA out of memory
   ```
   - Reduce batch size in vLLM server
   - Use model quantization (int8/int4)
   - Upgrade GPU memory

4. **JSON Parsing Errors**
   ```
   Failed to parse vLLM response
   ```
   - Check prompt template formatting
   - Verify model response format
   - Review response parsing logic

### Debug Mode

Enable debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

agent = KsqlToFlinkSqlVllmAgent()
```

## Contributing

When modifying the vLLM agent:

1. **Prompt Templates**: Update prompt files in `prompts/ksql_vllm/`
2. **Model Changes**: Update model configuration in agent initialization
3. **API Changes**: Ensure compatibility with vLLM OpenAI API format
4. **Testing**: Run test suite and add new test cases

## License

Copyright 2024-2025 Confluent, Inc.

This implementation follows the same license terms as the original shift_left project. 