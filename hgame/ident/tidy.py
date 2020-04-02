"""Tidy a CSV file by loading in pandas and re-saving.

This is a utility file to produce a tidy version of a CSV file that has been
edited in another tool (e.g. Excel).  Different programs write CSV files
with different conventions, for example, the choice of line terminator
(Windows or Unix-style) and the strategy for quoting fields.

This tool loads the file and saves it back straightaway, which will have
the effect of making a canonical version of the file.  This will be helpful
for version control, as it will ensure that changes that show up in the
history of the file will be changes in content, instead of changes
in formatting.
"""

import pathlib

import pandas as pd


def main():
    for fn in sorted(pathlib.Path("data/ident").glob("**/*.csv")):
        print(f"Tidying {fn}")
        (
            pd.read_csv(fn, dtype=str)
            .to_csv(fn, index=False, encoding='utf-8')
        )
