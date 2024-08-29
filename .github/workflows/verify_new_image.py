#! /usr/bin/env python
"""Determine if a new docker image should be published for the version."""

import argparse
import os
import sys

from packaging.version import Version  # 3rd-party, may need to pip install


parser = argparse.ArgumentParser(usage=__doc__)
parser.add_argument('version', type=Version)


if __name__=="__main__":
    args = parser.parse_args()
    print(f" Version: {args.version}")

    new_release = 0 \
            if args.version.is_devrelease \
            or args.version.is_prerelease \
            or args.version.is_postrelease \
            else 1
    if env_file := os.getenv('GITHUB_ENV'):
        with open(env_file, "a", encoding='utf-8') as myfile:
            myfile.write("PUBLISH_DOCKER_IMAGE_VALID=1\n")
            myfile.write(f"PUBLISH_DOCKER_IMAGE={new_release}\n")
    else:
        sys.stdout.write("PUBLISH_DOCKER_IMAGE_VALID=1\n")
        sys.stdout.write(f"PUBLISH_DOCKER_IMAGE={new_release}\n")

