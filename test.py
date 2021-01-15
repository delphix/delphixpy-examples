#!/usr/bin/env python
import glob
import subprocess
import time

python_files = glob.glob("*.py")
for path in (path for path in python_files if path != "test.py"):
    start = time.time()
    out = subprocess.call(
        ["python", path, "-h"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    end = time.time()
    print("| {} | {} | {} |".format(path, out, end - start))
