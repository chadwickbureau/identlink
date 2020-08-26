"""Update person ident lists from source records.
"""
import pathlib

import pandas as pd


def extract_with_source(path, source, **kwargs):
    """Helper function to extract a CSV file and add a 'source' column.
    """
    try:
        return pd.read_csv(path, dtype=str, encoding='utf-8', **kwargs) \
                 .assign(source=source)
    except IOError:
        return pd.DataFrame()


def collect_from_averages(path):
    """Collect playing and managing performance records from
    minoraverages repository.
    """
    print("Collecting items from minoraverages dataset.")
    return [pd.concat([extract_with_source(sourcepath/"playing_individual.csv",
                                           "minoraverages/"+sourcepath.name)
                       for sourcepath in (path/"processed").glob("*")] +
                      [extract_with_source(
                          sourcepath/"managing_individual.csv",
                          "minoraverages/" + sourcepath.name
                       )
                       for sourcepath in (path/"processed").glob("*")],
                      sort=True, ignore_index=True)]


def extract_idents(path):
    """Compile the existing ident files. 'path' is the root of the repository
    of person ident files.
    """
    print("Collecting identfiles")
    idents = pd.concat([pd.read_csv(fn, dtype=str, encoding='utf-8')
                        for fn in path.glob("*/*.csv")],
                       ignore_index=True, sort=False)
    for col in ['person.name.given', 'S_STINT']:
        idents[col] = idents[col].fillna("")
    return idents[['ident', 'source', 'league.year', 'league.name',
                   'person.ref', 'person.name.last', 'person.name.given',
                   'S_STINT', 'entry.name']]


def extract_sources():
    """Collect up person references from the various sources.
    """
    avglist = collect_from_averages(pathlib.Path("../hgame-averages"))
    print("Concatenating files...")
    return pd.concat(avglist,
                     sort=False, ignore_index=True)


def clean_sources(df):
    """Clean up source records into a standard format.
    """
    # Fill in an indicator for records which indicate a position played
    # but not games at that position
    df["pos"] = ("B"+df["B_G"]).fillna("") + ("P"+df["P_G"]).fillna("")
    for pos in ["P", "C", "1B", "2B", "3B", "SS", "OF", "LF", "CF", "RF"]:
        if f"F_{pos}_G" in df and f"F_{pos}_POS" in df:
            df[f"F_{pos}_G"] = (
                df[f"F_{pos}_G"].fillna(
                    df[f"F_{pos}_POS"].apply(
                        lambda x:
                        "" if not pd.isnull(x) and int(x) > 0
                        else None
                    )
                )
            )
        df["pos"] += df[f"F_{pos}_G"].apply(lambda x:
                                            pos.lower() + str(x) + ","
                                            if not pd.isnull(x) and x != "0"
                                            else "")
    df["pos"] = df["pos"].str.rstrip(",")
    for col in ['person.name.given', 'S_STINT']:
        df[col] = df[col].fillna("")
    df = df.assign(span=lambda x:
                   (x['S_FIRST'].fillna("").str.replace("-", "") + "/" +
                    x['S_LAST'].fillna("").str.replace("-", ""))
                   .replace("/", ""))
    return df


def merge_idents(df, idents):
    """Apply existing person reference identifications to dataset of sources.
    """
    if not idents.empty:
        df = df.merge(idents, how='left',
                      on=['source', 'league.year', 'league.name',
                          'person.ref',
                          'person.name.last', 'person.name.given',
                          'S_STINT', 'entry.name'])
    else:
        df['ident'] = None
    return (
        df.reindex(columns=[
            'source', 'league.year', 'league.name', 'ident', 'person.ref',
            'person.name.last', 'person.name.given',
            'S_STINT', 'entry.name', 'span', 'pos'
        ])
        .drop_duplicates()
        .sort_values(['league.year', 'league.name',
                      'person.name.last', 'source', 'person.ref'])
    )


def load_idents(df, path):
    """Write out ident files to disk repository.
    """
    print("Writing ident files...")
    for ((year, league), data) in df.groupby(['league.year', 'league.name']):
        # We only generate ident files for leagues where we have
        # either an averages compilation or boxscore data
        sample = data[data['source'].str.startswith('retrosheet/') |
                      data['source'].str.startswith('minoraverages/') |
                      data['source'].str.startswith('boxscores/')]
        if sample.empty:
            continue
        print(year, league)
        (path / year).mkdir(exist_ok=True)
        filepath = (path / year /
                    ("%s%s.csv" %
                     (year, league.replace(" ", "").replace("-", ""))))
        data.to_csv(filepath, index=False, encoding='utf-8')


def main():
    """Update person ident lists from source records.
    """
    ident_path = pathlib.Path("data/ident/people")
    idents = extract_idents(ident_path)
    extract_sources().pipe(clean_sources) \
                     .pipe(merge_idents, idents) \
                     .pipe(load_idents, ident_path)
