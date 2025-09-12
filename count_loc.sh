#!/bin/bash

# count_loc.sh - Count lines of code in shift_left_utils project
# This script counts Python (.py) files and their lines of code,
# excluding virtual environments, __pycache__, and other non-source directories

echo "========================================"
echo "Python Lines of Code Counter"
echo "Project: shift_left_utils"
echo "Date: $(date)"
echo "========================================"
echo

# Count files
echo "=== FILE COUNTS ==="
total_py_files=$(find . -name "*.py" -not -path "./.venv/*" -not -path "*/.venv/*" -not -path "*/site-packages/*" -not -path "*/__pycache__/*" | wc -l | tr -d ' ')
source_files=$(find ./src/shift_left/src -name "*.py" | wc -l | tr -d ' ')
test_files=$(find ./src/shift_left/tests -name "*.py" | wc -l | tr -d ' ')
older_utils=$(find ./older-utils-to-clean -name "*.py" | wc -l | tr -d ' ')

echo "Total Python files: $total_py_files"
echo "  - Source files:   $source_files"
echo "  - Test files:     $test_files" 
echo "  - Older utils:    $older_utils"
echo

# Count lines of code
echo "=== LINES OF CODE ==="
total_loc=$(find . -name "*.py" -not -path "./.venv/*" -not -path "*/.venv/*" -not -path "*/site-packages/*" -not -path "*/__pycache__/*" -exec wc -l {} + | tail -1 | awk '{print $1}')
source_loc=$(find ./src/shift_left/src -name "*.py" -exec wc -l {} + | tail -1 | awk '{print $1}')
test_loc=$(find ./src/shift_left/tests -name "*.py" -exec wc -l {} + | tail -1 | awk '{print $1}')
older_loc=$(find ./older-utils-to-clean -name "*.py" -exec wc -l {} + | tail -1 | awk '{print $1}')

echo "Total lines of code: $total_loc"
echo "  - Source files:    $source_loc"
echo "  - Test files:      $test_loc"
echo "  - Older utils:     $older_loc"
echo

# Validation
echo "=== VALIDATION ==="
calculated_total=$((source_loc + test_loc + older_loc))
if [ "$calculated_total" -eq "$total_loc" ]; then
    echo "✅ Line count validation: PASSED"
else
    echo "❌ Line count validation: FAILED (calculated: $calculated_total, actual: $total_loc)"
fi

calculated_files=$((source_files + test_files + older_utils))
if [ "$calculated_files" -eq "$total_py_files" ]; then
    echo "✅ File count validation: PASSED"
else
    echo "❌ File count validation: FAILED (calculated: $calculated_files, actual: $total_py_files)"
fi

echo
echo "=== SUMMARY ==="
echo "The shift_left_utils project contains:"
echo "• $total_py_files Python source files"
echo "• $total_loc total lines of code"
echo "• Average lines per file: $((total_loc / total_py_files))"
echo
echo "Breakdown:"
echo "• Source code: $source_files files, $source_loc lines ($(echo "scale=1; $source_loc * 100 / $total_loc" | bc)%)"
echo "• Test code: $test_files files, $test_loc lines ($(echo "scale=1; $test_loc * 100 / $total_loc" | bc)%)"
echo "• Legacy utils: $older_utils files, $older_loc lines ($(echo "scale=1; $older_loc * 100 / $total_loc" | bc)%)"
