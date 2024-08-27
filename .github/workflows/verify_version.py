# /usr/bin/env/ python
"""Compare two versions. Exists non-zero if the new version is not higher.

usage: verify_version.py <old-version> <new-version>
"""
import re
import sys

# Taken from https://packaging.python.org/en/latest/specifications/version-specifiers/.
# Best not to modify or try to understand this
VERSION_PATTERN = r"""
    v?
    (?:
        (?:(?P<epoch>[0-9]+)!)?                           # epoch
        (?P<major>0|[1-9]\d*)    # modified from https://semver.org/
        \.(?P<minor>0|[1-9]\d*)  # modified from https://semver.org/
        \.(?P<patch>0|[1-9]\d*)  # modified from https://semver.org/
        (?P<pre>                                          # pre-release
            [-_\.]?
            (?P<pre_l>(a|b|c|rc|alpha|beta|pre|preview))
            [-_\.]?
            (?P<pre_n>[0-9]+)?
        )?
        (?P<post>                                         # post release
            (?:-(?P<post_n1>[0-9]+))
            |
            (?:
                [-_\.]?
                (?P<post_l>post|rev|r)
                [-_\.]?
                (?P<post_n2>[0-9]+)?
            )
        )?
        (?P<dev>                                          # dev release
            [-_\.]?
            (?P<dev_l>dev)
            [-_\.]?
            (?P<dev_n>[0-9]+)?
        )?
    )
    (?:\+(?P<local>[a-z0-9]+(?:[-_\.][a-z0-9]+)*))?       # local version
"""
_regex = re.compile(
    r"^\s*" + VERSION_PATTERN + r"\s*$",
    re.VERBOSE | re.IGNORECASE,
)

def verify_version(old_vers: str, new_vers: str) -> bool:
    """Verify the new version is valid.

    Parameters
    ----------
    old_vers: string
        Existing version number, likely from main.
    new_vers: string
        New version number from current branch.

    Outputs
    ----------
    validity: bool
        Validity of the new version number.
    """
    if not (old := _regex.fullmatch(old_vers)):
        raise ValueError(f'{old_vers!r} is not a valid version string')
    if not (new := _regex.fullmatch(new_vers)):
        raise ValueError(f'{new_vers!r} is not a valid version string')

    release = _compare_release(old, new)
    if release is not None:
        return release
    pre = _compare_pre(old, new)
    if pre is not None:
        return pre
    post = _compare_post(old, new)
    if post is not None:
        return post

    print('Unknown version comparison!')
    return False


def _compare_release(old: re.Match, new: re.Match) -> bool | None:
    """Check for different simple (release) version."""
    for key in ('major', 'minor', 'patch'):
        if int(old[key]) > int(new[key]):
            return False
        if int(old[key]) < int(new[key]):
            return True
    return None


def _compare_pre(old: re.Match, new: re.Match) -> bool | None:
    """Check for pre-release tags that denote a lower version."""
    for key in ('pre', 'pre_l', 'pre_n', 'dev', 'dev_l', 'dev_n'):
        olds, news = old[key], new[key]
        if news is None and olds is None:
            continue
        if news is not None and olds is None:
            return False  # new is prerelease and old is not
        if olds is not None and news is None:
            return True  # old is prerelease and new is not
        if '_n' in key:  # if both are pre, the higher version wins
            olds, news = int(olds), int(news)
            if olds != news:
                return olds < news
        if olds != news:
            return olds < news
    return None


def _compare_post(old: re.Match, new: re.Match) -> bool | None:
    # check for post-release tags that denote a higher version
    for key in ('post', 'post_n1', 'post_l', 'post_n2'):
        olds, news = old[key], new[key]
        if news is None and olds is None:
            continue
        if news is not None and olds is None:
            return True  # new is post and old is not
        if olds is not None and news is None:
            return False  # old is post and new is not
        if '_n' in key:  # if both are post, the higher version wins
            olds, news = int(olds), int(news)
            if olds != news:
                return olds < news
        if olds != news:
            return olds < news
    return None


if __name__=="__main__":
    assert len(sys.argv) == 3
    _, old_version, new_version = sys.argv
    print(f"Old Version: {old_version}")
    print(f"New Version: {new_version}")

    if not verify_version(old_version, new_version):
        print(f"""
{new_version!r} is not higher than {old_version!r}. Read the docs at
https://packaging.python.org/en/latest/specifications/version-specifiers/
for details 
        """)
        sys.exit(1)
    sys.exit(0)
