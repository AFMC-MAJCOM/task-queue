#! /usr/bin/env python
import re
import sys
import os

# Modified version pattern from https://semver.org/. If the 'patch' number
# is 0 and there is no prerelease or build metadata it indicates that there
# was a minor or a major release
RELEASABLE_VERSION_PATTERN = re.compile(r'^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0)$')


def verify_new_image(version_string: str) -> bool:
    """Verify that if a new docker image should be
    published based on the version.

    Parameters
    ----------
    version: string
        The version being pushed to main.

    Outputs
    ----------
    new_release: bool
        True if a new docker image should be released, False otherwise.
    """
    return bool(RELEASABLE_VERSION_PATTERN.fullmatch(version_string))


if __name__=="__main__":
    print(sys.argv)
    assert len(sys.argv) == 2
    version = sys.argv[1]
    print(f" Version: {version}")

    new_release = verify_new_image(version)
    if env_file := os.getenv('GITHUB_ENV'):
        with open(env_file, "a", encoding='utf-8') as myfile:
            myfile.write("PUBLISH_DOCKER_IMAGE_VALID=1\n")
            myfile.write(f"PUBLISH_DOCKER_IMAGE={1 if new_release else 0}\n")
    else:
        sys.stdout.write("PUBLISH_DOCKER_IMAGE_VALID=1\n")
        sys.stdout.write(f"PUBLISH_DOCKER_IMAGE={1 if new_release else 0}\n")
