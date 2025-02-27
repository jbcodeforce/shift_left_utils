import subprocess

# Run a command and capture output
result = subprocess.run(["confluent", "environment", "list"], capture_output=True, text=True)
print(result.stdout)
print(result.stderr)