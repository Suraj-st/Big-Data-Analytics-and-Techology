#!/usr/bin/env python3
import sys

current_node = None
count = 0

for line in sys.stdin:
    node, val = line.strip().split('\t')
    val = int(val)
    if node == current_node:
        count += val
    else:
        if current_node:
            print(f"{current_node}\t{count}")
        current_node = node
        count = val

if current_node:
    print(f"{current_node}\t{count}")
