#! /usr/bin/env python
"""Compare two versions. Exits non-zero if the new version is not higher."""

import argparse
import sys

from packaging.version import Version  # 3rd-party, may need to pip install

parser = argparse.ArgumentParser(usage=__doc__)
parser.add_argument('old', type=Version)
parser.add_argument('new', type=Version)

if __name__=="__main__":
    args = parser.parse_args()
    
    print(f"Old Version: {args.old}")
    print(f"New Version: {args.new}")
    if args.new > args.old:
        print('Ok')
        sys.exit(0)

    url = "https://packaging.python.org/en/latest/specifications/version-specifiers/"
    print(
        f"{args.new!s} is not higher than {args.old!s}.",
        f"Read the docs at {url} for details.",
    )
    sys.exit(1)
