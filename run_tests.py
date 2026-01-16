#!/usr/bin/env python3
"""Run all integration tests for blockers."""

import sys
import subprocess
import os

def main():
    # Change to project directory
    project_dir = '/home/engine/project'
    os.chdir(project_dir)
    
    # Run the test file directly (it has its own test runner)
    result = subprocess.run([
        sys.executable,
        'tests/test_blockers_integration.py'
    ], cwd=project_dir)
    
    return result.returncode

if __name__ == "__main__":
    sys.exit(main())
