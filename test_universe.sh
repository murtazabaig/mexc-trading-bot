#!/bin/bash
# Test runner for the universe module

set -e

echo "=== Running Universe Tests ==="
source venv/bin/activate
export PYTHONPATH=/home/engine/project:$PYTHONPATH

# Run unit tests
python -m pytest tests/test_universe.py -v

echo ""
echo "=== All Tests Passed ==="
