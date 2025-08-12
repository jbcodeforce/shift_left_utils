#!/usr/bin/env python3

import os
import re
import subprocess
import sys

def extract_table_name(ddl_file_path):
    """Extract table name from DDL file."""
    try:
        with open(ddl_file_path, 'r') as f:
            content = f.read()
        
        # Pattern to match CREATE TABLE statements with optional backticks
        pattern = r'CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+`?([a-zA-Z_][a-zA-Z0-9_]*)`?\s*\('
        match = re.search(pattern, content, re.IGNORECASE | re.MULTILINE)
        
        if match:
            return match.group(1)
        
        print(f"Debug: Could not extract table name from {ddl_file_path}")
        print(f"First line: {content.split(chr(10))[0]}")
        return None
        
    except Exception as e:
        print(f'Error reading {ddl_file_path}: {e}')
        return None

def count_sql_files_in_tests(folder_path):
    """Count SQL files in tests directory."""
    tests_path = os.path.join(folder_path, 'tests')
    if not os.path.exists(tests_path):
        return 0
    sql_files = [f for f in os.listdir(tests_path) if f.endswith('.sql')]
    return len(sql_files)

def main():
    # Get all directories
    dirs = [d for d in os.listdir('.') if os.path.isdir(d)]
    print(f'Processing {len(dirs)} directories...')

    valid_tables = []
    excluded_tables = []
    failed_tables = []

    for folder in sorted(dirs):
        sql_scripts_path = os.path.join(folder, 'sql-scripts')
        if not os.path.exists(sql_scripts_path):
            failed_tables.append((folder, 'no sql-scripts directory'))
            continue
        
        # Find DDL file
        ddl_files = [f for f in os.listdir(sql_scripts_path) if f.startswith('ddl.') and f.endswith('.sql')]
        if not ddl_files:
            failed_tables.append((folder, 'no DDL file found'))
            continue
        
        ddl_file_path = os.path.join(sql_scripts_path, ddl_files[0])
        table_name = extract_table_name(ddl_file_path)
        
        if not table_name:
            failed_tables.append((folder, f'could not extract table name from {ddl_files[0]}'))
            continue
        
        # Check if tests folder has multiple SQL files
        sql_count = count_sql_files_in_tests(folder)
        if sql_count > 1:
            excluded_tables.append((folder, table_name, sql_count))
            continue
        
        valid_tables.append((folder, table_name))

    # Print results
    print(f'\nResults:')
    print(f'✅ Valid tables: {len(valid_tables)}')
    print(f'❌ Excluded tables (multiple SQL in tests): {len(excluded_tables)}')
    print(f'⚠️  Failed to process: {len(failed_tables)}')

    if excluded_tables:
        print(f'\nExcluded tables:')
        for folder, table_name, sql_count in excluded_tables:
            print(f'  - {folder} (table: {table_name}) - {sql_count} SQL files in tests')

    if failed_tables:
        print(f'\nFailed to process:')
        for folder, reason in failed_tables:
            print(f'  - {folder}: {reason}')

    print(f'\nNow running shift_left commands for {len(valid_tables)} valid tables...')
    print('=' * 60)

    success_count = 0
    failure_count = 0

    for i, (folder, table_name) in enumerate(valid_tables, 1):
        cmd = ['shift_left', 'table', 'init-unit-tests', table_name]
        print(f'[{i}/{len(valid_tables)}] Running: {" ".join(cmd)}')
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            if result.returncode == 0:
                print(f'✅ SUCCESS: {table_name}')
                success_count += 1
            else:
                print(f'❌ FAILED: {table_name}')
                if result.stderr.strip():
                    print(f'   Error: {result.stderr.strip()}')
                if result.stdout.strip():
                    print(f'   Output: {result.stdout.strip()}')
                failure_count += 1
        except subprocess.TimeoutExpired:
            print(f'❌ TIMEOUT: {table_name}')
            failure_count += 1
        except Exception as e:
            print(f'❌ ERROR: {table_name} - {e}')
            failure_count += 1
        
        print()

    print('=' * 60)
    print(f'FINAL SUMMARY:')
    print(f'✅ Successful: {success_count}')
    print(f'❌ Failed: {failure_count}')
    print(f'📊 Total processed: {success_count + failure_count}')

    # Generate a summary file with all commands
    with open('shift_left_commands.txt', 'w') as f:
        f.write("# Commands to run shift_left table init-unit-tests for all valid tables\n")
        f.write(f"# Generated for {len(valid_tables)} valid tables\n\n")
        for folder, table_name in valid_tables:
            f.write(f"shift_left table init-unit-tests {table_name}\n")
    
    print(f'\n📝 All commands saved to: shift_left_commands.txt')

if __name__ == '__main__':
    main()