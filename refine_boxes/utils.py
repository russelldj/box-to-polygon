import os
from glob import glob

import ubelt as ub


def get_all_files(
    folder, isfile=False, isdir=False, recursive=True, extension=None, sort_key=None
):
    extension = "*" if extension is None else extension
    files = glob(os.path.join(folder, extension), recursive=recursive)
    if isfile and isdir:
        raise ValueError("Cannot have both isfile and isdir")

    if isfile:
        files = list(filter(os.path.isfile, files))
    elif isdir:
        files = list(filter(os.path.isdir, files))
    return sorted(files, key=sort_key)


def ensure_containing_dir(filename):
    ub.ensuredir(os.path.dirname(filename))
