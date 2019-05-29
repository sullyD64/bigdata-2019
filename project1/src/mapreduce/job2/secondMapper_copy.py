#!/usr/bin/env python
import sys

def main():
    infile = sys.stdin
    for row in infile:
      print(row.strip())


if __name__ == "__main__":
    main()
