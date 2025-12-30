#!/usr/bin/env python3
import sys

# Each line: source destination
for line in sys.stdin:
    parts = line.strip().split()
    if len(parts) != 2:
        continue
    src, dest = parts
    # Emit destination node (for in-degree)
    print(f"{dest}\t1")
