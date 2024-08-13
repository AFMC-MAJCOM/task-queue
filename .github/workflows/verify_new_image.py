import sys


def verify_new_image(version: str) -> bool:
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
    version = [int(v) for v in version.split('.')]
    if version[2] == 0:
        # If the last version number is 0 it indicates that there was a minor
        # or a major release
        return True
    return False


if __name__=="__main__":
     assert len(sys.argv) == 3
    version = sys.argv[1]
    print(f" Version: {version}")

    new_release = verify_new_image(version)
    print(new_release)
    sys.exit(False)