"""Module for running s5cmd subcommands.
"""
import subprocess
from functools import partial
import shutil


S5CMD_EXE = "s5cmd"

HAS_S5CMD = shutil.which("s5cmd") is not None

def base_command(subcmd, *main_args, concurrency=None, other_arguments=[]):
    """Runs an S5cmd subcommand with some main arguments and some other
    optional configuration or flags.

    Parameters:
    -----------
    subcmd: str
        subcommand
    *main_args: string(s)
        main arguments
    concurrency: string(s) (default=None)
        concurrency arguments
    other_arguments: List of strings (default=[])
        other arguements to include
    """
    if concurrency:
        concurrency_args = ["--concurrency", str(concurrency)]
    else:
        concurrency_args = []

    args = concurrency_args + other_arguments

    cmd = ["s5cmd", subcmd] + list(main_args) + args

    print(f"Running s5cmd command {cmd}")
    subprocess.run(cmd, check=True)

cp = partial(base_command, "cp")
mv = partial(base_command, "mv")
rm = partial(base_command, "rm")

# Aliases
move = mv
copy = cp
delete = rm

# Also available, but not implemented here:
#    ls              list buckets and objects
#    mb              make bucket
#    rb              remove bucket
#    select          run SQL queries on objects
#    du              show object size usage
#    cat             print remote object content
#    pipe            stream to remote from stdin
#    run             run commands in batch
#    sync            sync objects
#    version         print version
#    bucket-version  configure bucket versioning
#    presign         print remote object presign url
#    help, h         Shows a list of commands or help for one command
