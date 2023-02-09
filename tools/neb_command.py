import os
import shutil
import joblib
import pandas as pd
import constants as c
from scipy.stats.mstats import gmean
from kauffman.data_fetch import bfs, bds, pep


def _format_csv(df):
    return df \
        .astype({'fips': 'str', 'time': 'int'})


def _fetch_data_bfs(geo_level, fetch_data):
    if fetch_data:
        print(f'\tcreating datasets neb/data/temp/bfs_{geo_level}.pkl')
        df = bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], geo_level, annualize=True) \
            .rename(columns={
                'BF_DUR8Q': 'avg_speed_annual', 'BF_SBF8Q': 'bf', 'BA_BA': 'ba'
            })
    else:
        df = pd.read_csv(c.filenamer(f'data/raw_data/bfs_{geo_level}.csv')) \
            .pipe(_format_csv)
    joblib.dump(df, c.filenamer(f'data/temp/bfs_{geo_level}.pkl'))


def _fetch_data_bfs_march_shift(geo_level, fetch_data):
    if fetch_data:
        print(f'\tcreating datasets neb/data/temp/bfs_march_{geo_level}.pkl')
        df = bfs(['BF_SBF8Q'], geo_level, march_shift=True) \
            .rename(columns={'BF_SBF8Q': 'bf_march_shift'})
    else:
        df = pd.read_csv(
                c.filenamer(f'data/raw_data/bfs_march_{geo_level}.csv')
            ) \
            .pipe(_format_csv)
    joblib.dump(df, c.filenamer(f'data/temp/bfs_march_{geo_level}.pkl'))


def _fetch_data_bds(geo_level, fetch_data):
    if fetch_data:
        print(f'\tcreating dataset neb/data/temp/bds_{geo_level}.pkl')
        df = bds(['FIRM'], geo_level=geo_level) \
            .rename(columns={'FIRM': 'firms'})
    else:
        df = pd.read_csv(c.filenamer(f'data/raw_data/bds_{geo_level}.csv')) \
            .pipe(_format_csv)
    joblib.dump(df, c.filenamer(f'data/temp/bds_{geo_level}.pkl'))


def _fetch_data_pep(geo_level, fetch_data):
    if fetch_data:
        print(f'\tcreating dataset neb/data/temp/pep_{geo_level}.pkl')
        df = pep(geo_level) \
            .rename(columns={'POP': 'population'}) \
            .astype({'time': 'int', 'population': 'int'})
    else:
        df = pd.read_csv(c.filenamer(f'data/raw_data/pep_{geo_level}.csv')) \
            .pipe(_format_csv)
    joblib.dump(df, c.filenamer(f'data/temp/pep_{geo_level}.pkl'))


def _raw_data_fetch(fetch_data):
    if os.path.isdir(c.filenamer('data/temp')):
        _raw_data_remove(remove_data=True)
    os.mkdir(c.filenamer('data/temp'))

    for geo_level in ['us', 'state']:
        _fetch_data_bfs(geo_level, fetch_data)
        _fetch_data_bfs_march_shift(geo_level, fetch_data)
        _fetch_data_bds(geo_level, fetch_data)
        _fetch_data_pep(geo_level, fetch_data)


def _raw_data_merge(geo_level):
    return joblib.load(c.filenamer(f'data/temp/bfs_{geo_level}.pkl')) \
        .merge(
            joblib.load(c.filenamer(f'data/temp/pep_{geo_level}.pkl')) \
                .drop(columns='region'), 
            how='left', 
            on=['fips', 'time']
        ) \
        .merge(
            joblib.load(c.filenamer(f'data/temp/bds_{geo_level}.pkl')) \
                .drop(columns='region'),
            how='left', 
            on=['fips', 'time']
        ) \
        .merge(
            joblib.load(c.filenamer(f'data/temp/bfs_march_{geo_level}.pkl')) \
                .drop(columns='region'), 
            how='left', 
            on=['fips', 'time']
        )


def _goalpost(df, index_vars):
    for k, v in index_vars.items():
        if v['polarity'] == 'pos':
            df.loc[:, k + '_normed'] = (
                ((df[k] - (v['ref'] - v['delta'])) / (2 * v['delta'])) * .6 + .7
            )
        else:
            df.loc[:, k + '_normed'] = (
                1.3 \
                - ((df[k] - (v['ref'] - v['delta'])) / (2 * v['delta'])) \
                * .6
            )
    return df


def _normalize(df, index_vars):
    return _goalpost(df, index_vars)


def _aggregator(df, index_vars):
    df['index'] = gmean(df[map(lambda x: x + '_normed', index_vars)], axis=1)
    return df


def index(df, geo_level):
    reference_year = 2017  # minimum of last year of velocity or actualization

    if geo_level == 'state':
        df \
            .query(f'time <= {reference_year}') \
            .pipe(joblib.dump, c.filenamer('data/temp/df_ref.pkl'))
    df_ref = joblib.load(c.filenamer('data/temp/df_ref.pkl'))

    index_vars = {
        'velocity': {
            'polarity': 'neg',
            'delta': (df_ref['velocity'].max() - df_ref['velocity'].min()) / 2,
            'ref': df_ref.query('time == 2005')['velocity'].mean()
        },
        'actualization': {
            'polarity': 'pos',
            'delta': (
                (
                    df_ref['actualization'].max() \
                    - df_ref['actualization'].min()
                ) / 2
            ),
            'ref': df_ref.query('time == 2005')['actualization'].mean()
        },
    }

    return pd.concat(
            [
                df_group[1] \
                    .pipe(_normalize, index_vars) \
                    .pipe(_aggregator, index_vars)
                for df_group in df.groupby(['time'])
            ]
        ) \
        .reset_index(drop=True) \
        .drop(list(map(lambda x: x + '_normed', index_vars)), 1)


def _indicators_create(df, geo_level):
    return df \
        .rename(columns={'avg_speed_annual': 'velocity'}) \
        .assign(
            actualization=lambda x: x['bf'] / x['ba'],
            bf_per_capita=lambda x: x['bf'] / x['population'] * 100,
            newness=lambda x: x['bf_march_shift'] / x['firms'],
        ) \
        .pipe(index, geo_level) \
        [[
            'fips', 'time', 'actualization', 'bf_per_capita', 'velocity', 
            'newness', 'index'
        ]]


def _fips_formatter(df, geo_level):
    if geo_level == 'us':
        return df.assign(fips='00')
    elif geo_level == 'state':
        return df.assign(
                fips=lambda x: x['fips'] \
                    .apply(lambda row: row if len(row) == 2 else '0' + row)
            )
    else:
        return df.assign(
                fips=lambda x: x['fips'].apply(
                    lambda row: '00' + row if len(row) == 3 \
                        else '0' + row if len(row) == 4 
                        else row
                )
            )


def _final_data_transform(df, geo_level):
    return df \
        .pipe(_fips_formatter, geo_level) \
        .assign(
            category='Total',
            type='Total'
        ) \
        .rename(columns={'time': 'year'}) \
        .sort_values(['fips', 'year', 'category']) \
        .reset_index(drop=True) \
        .assign(name=lambda x: x['fips'].map(c.all_fips_name_dict)) \
        [[
            'fips', 'name', 'type', 'category', 'year', 'actualization', 
            'bf_per_capita', 'velocity', 'newness', 'index'
        ]] \
        .query('2005 <= year')


def _create_neb_data(geo_level):
    return _raw_data_merge(geo_level) \
            .pipe(_indicators_create, geo_level) \
            .pipe(_final_data_transform, geo_level)


def _download_csv_save(df, aws_filepath):
    df.to_csv(c.filenamer('data/neb_download.csv'), index=False)
    if aws_filepath:
        df.to_csv(f'{aws_filepath}/neb_download.csv', index=False)
    return df


def _download_to_alley_formatter(df, outcome):
    return df[['fips', 'year', 'type', 'category'] + [outcome]] \
        .pipe(
            pd.pivot_table, 
            index=['fips', 'type', 'category'], 
            columns='year', 
            values=outcome
        ) \
        .reset_index() \
        .replace('Total', '') \
        .rename(columns={
            'type': 'demographic-type', 'category': 'demographic', 
            'fips': 'region'
        })


def _website_csv_save(df, aws_filepath):
    for indicator in [
            'actualization', 'bf_per_capita', 'velocity', 'newness', 'index'
        ]:
        df_out = df.pipe(_download_to_alley_formatter, indicator)
        df_out.to_csv(
            c.filenamer(f'data/neb_website_{indicator}.csv'), index=False
        )
        if aws_filepath:
            df_out.to_csv(
                f'{aws_filepath}/neb_website_{indicator}.csv', index=False
            )


def _raw_data_remove(remove_data=True):
    if remove_data:
        shutil.rmtree(c.filenamer('data/temp'))  # remove unwanted files


def neb_data_create_all(raw_data_fetch, raw_data_remove, aws_filepath=None):
    """
    Create and save NEB data. This is the main function of neb_command.py.

    Fetches raw BDS, PEP, and BFS data, transforms it, and saves it to two 
    csv's: One for user download, and one formatted for upload to the Kauffman 
    site.

    Parameters
    ----------
    raw_data_fetch: bool
        Specifies whether to fetch the data. Allows users to skip raw-data-fetch
        step if they prefer using the csv files in the raw_data subdirectory. 
        If True, then fetches data from the online sources.
    raw_data_remove: bool
        Specifies whether to delete TEMP data at the end.
    aws_filepath: None or str
        S3 bucket for stashing the final output files. All data is saved in S3 
        as a csv file.
    """
    _raw_data_fetch(raw_data_fetch)

    pd.concat(
        [
            _create_neb_data(geo_level) for geo_level in ['state', 'us']
        ]
    ) \
        .pipe(_download_csv_save, aws_filepath) \
        .pipe(_website_csv_save, aws_filepath)

    _raw_data_remove(raw_data_remove)


if __name__ == '__main__':
    neb_data_create_all(
        raw_data_fetch=False,
        raw_data_remove=True
        #aws_filepath='s3://emkf.data.research/indicators/neb/data_outputs'
    )
