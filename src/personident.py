"""Update person ident lists from source records.
"""
from __future__ import print_function
from __future__ import unicode_literals
import argparse

import pathlib2 as pathlib
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
                      [extract_with_source(sourcepath/"managing_individual.csv",
                                           "minoraverages/"+sourcepath.name)
                       for sourcepath in (path/"processed").glob("*")],
                      sort=True, ignore_index=True)]


def collect_from_boxscores(path):
    """Collect people entries from boxscores repository.
    """
    print("Collecting players from boxscores dataset.")
    players = pd.concat([extract_with_source(sourcepath/"players.csv",
                                             "/".join(["boxscores",
                                                       sourcepath.parts[-2],
                                                       sourcepath.parts[-1]]))
                         for sourcepath in (path/"data"/"boxscores"/"processed").glob("*/*")],
                        ignore_index=True)
    umpires = pd.concat([extract_with_source(sourcepath/"umpires.csv",
                                             "/".join(["boxscores",
                                                       sourcepath.parts[-2],
                                                       sourcepath.parts[-1]]))
                         for sourcepath in (path/"data"/"boxscores"/"processed").glob("*/*")],
                        ignore_index=True)
    umpires['entry.name'] = "#umpire"
    return [players, umpires]


def collect_retrosheet_rosters(path_retro, year):
    """Collect the roster files from the Retrosheet repository for 'year'.
    """
    return pd.concat([pd.read_csv(fn, dtype=str, encoding='utf-8',
                                  header=None,
                                  names=['person.ref',
                                         'person.name.last', 'person.name.given',
                                         'bats', 'throws', 'team.key', 'pos'],
                                  usecols=['person.ref', 'team.key',
                                           'person.name.last', 'person.name.given'])
                      for fn in (path_retro/"rosters").glob("*%s.ROS" % year)],
                     sort=False, ignore_index=True)


def collect_from_retrosheet(path_splits, path_retro):
    """Collect playing entries from retrosplits repository.
    """
    print("Collecting items from Retrosheet dataset.")
    dflist = []
    teams = pd.read_csv("support/retroteams.csv", dtype=str, encoding='utf-8')
    for year in range(1906, 1920):
        df = pd.read_csv(path_splits/"daybyday"/("playing-%d.csv" % year),
                         dtype=str, encoding='utf-8',
                         usecols=['person.key', 'team.key', 'game.date']) \
               .groupby(['person.key', 'team.key'])['game.date'] \
               .agg(['min', 'max']) \
               .set_axis(['S_FIRST', 'S_LAST'], axis='columns', inplace=False) \
               .reset_index()
        df['league.year'] = df['S_FIRST'].str.split('-').str[0]
        df = df.merge(teams, how='left', on=['league.year', 'team.key']) \
               .rename(columns={'person.key':  'person.ref'}) \
               .assign(source="retrosheet/%s" % year) \
               .merge(collect_retrosheet_rosters(path_retro, year),
                      how='left', on=['person.ref', 'team.key'])
        dflist.append(df[['source', 'league.year', 'league.name',
                          'person.ref',
                          'person.name.last', 'person.name.given',
                          'entry.name', 'S_FIRST', 'S_LAST']])
    return dflist

def collect_from_researchers(path):
    """Collect engagement records from researchers repository.
    """
    print("Collecting items from researchers dataset.")
    df = pd.read_csv("%s/processed/clubs.csv" % path, encoding='utf-8',
                     dtype=str)
    df.rename(inplace=True,
              columns={'last':       'person.name.last',
                       'first':      'person.name.given',
                       'date':       'league.year',
                       'league':     'league.name',
                       'person':     'person.ref',
                       'club':       'entry.name'})
    df['source'] = 'researchers' + '/' + df['person.ref'].str.split("/").str[0]
    df['person.ref'] = df['person.ref'].str.split("/").str[1]
    df = df[~df['person.name.last'].isnull()].copy()
    return [df]


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
    retrolist = collect_from_retrosheet(path_splits=pathlib.Path("../retrosplits"),
                                        path_retro=pathlib.Path("../retrosheet"))
    avglist = collect_from_averages(pathlib.Path("../minoraverages"))
    boxlist = collect_from_boxscores(pathlib.Path("../boxscores"))
    reslist = collect_from_researchers("../researchers")
    print("Concatenating files...")
    return pd.concat(retrolist + avglist + boxlist + reslist,
                     sort=False, ignore_index=True)


def clean_sources(df):
    """Clean up source records into a standard format.
    """
    # Fill in an indicator for records which indicate a position played
    # but not games at that position
    for pos in ["P", "C", "1B", "2B", "3B", "SS", "OF", "LF", "CF", "RF"]:
        if "F_%s_G" % pos in df and "F_%s_POS" % pos in df:
            df["F_%s_G" % pos] = df["F_%s_G" % pos] \
                                 .fillna(df["F_%s_POS" % pos] \
                                 .apply(lambda x:
                                        "yes" if not pd.isnull(x) and int(x) > 0
                                        else None))
    for col in ['person.name.given', 'S_STINT']:
        df[col] = df[col].fillna("")
    # We convert dates to YYYYMMDD. This way, ident files can be loaded
    # into e.g. Excel for editing, without messing up the formatting.
    # YYYYMMDD is considered a valid ISO date format as well.
    for col in ['S_FIRST', 'S_LAST']:
        df[col] = df[col].str.replace("-", "")
    return df

def merge_idents(df, idents):
    """Apply existing person reference identifications to dataset of sources.
    """
    if not idents.empty:
        df = df.merge(idents, how='left',
                      on=['source', 'league.year', 'league.name',
                          'person.ref', 'person.name.last', 'person.name.given',
                          'S_STINT', 'entry.name'])
    else:
        df['ident'] = None
    return df[['source', 'league.year', 'league.name', 'ident', 'person.ref',
               'person.name.last', 'person.name.given',
               'S_STINT', 'entry.name',
               'S_FIRST', 'S_LAST',
               'B_G', 'P_G', 'F_1B_G', 'F_2B_G', 'F_3B_G', 'F_SS_G',
               'F_OF_G', 'F_LF_G', 'F_CF_G', 'F_RF_G', 'F_C_G', 'F_P_G',
               'F_ALL_G']] \
           .drop_duplicates() \
           .sort_values(['league.year', 'league.name',
                         'person.name.last', 'source', 'person.ref'])


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
        filepath = path / year / \
                   ("%s%s.csv" % (year, league.replace(" ", "").replace("-", "")))
        data.to_csv(filepath, index=False, encoding='utf-8')


def main():
    """Update person ident lists from source records.
    """
    parser = argparse.ArgumentParser(description="Update person ident lists "
                                                 "from source records")
    parser.parse_args()

    ident_path = pathlib.Path("data/ident/people")
    idents = extract_idents(ident_path)
    extract_sources().pipe(clean_sources) \
                     .pipe(merge_idents, idents) \
                     .pipe(load_idents, ident_path)


if __name__ == '__main__':
    main()
