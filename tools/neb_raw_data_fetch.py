import joblib
import pandas as pd
import constants as c
from kauffman.tools import file_to_s3
from kauffman.data_fetch import bfs, pep, bds


def raw_data_update():
    joblib.dump(
        str(pd.to_datetime('today')), 
        c.filenamer('data/raw_data/raw_data_fetch_time.pkl')
    )

    for geo_level in ['us', 'state']:
        bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], geo_level, annualize=True) \
            .rename(columns={
                'BF_DUR8Q': 'avg_speed_annual', 'BF_SBF8Q': 'bf', 'BA_BA': 'ba'
            }) \
            .to_csv(
                c.filenamer(f'data/raw_data/bfs_{geo_level}.csv'), index=False
            )

        bfs(['BF_SBF8Q'], geo_level, march_shift=True) \
            .rename(columns={'BF_SBF8Q': 'bf_march_shift'}) \
            .to_csv(
                c.filenamer(f'data/raw_data/bfs_march_{geo_level}.csv'), 
                index=False
            )

        pep(geo_level) \
            .rename(columns={'POP': 'population'}) \
            .astype({'time': 'int', 'population': 'int'}) \
            .to_csv(
                c.filenamer(f'data/raw_data/pep_{geo_level}.csv'), index=False
            )

        bds(['FIRM'], geo_level=geo_level) \
            .rename(columns={'FIRM': 'firms'}) \
            .to_csv(
                c.filenamer(f'data/raw_data/bds_{geo_level}.csv'), index=False
            )


def s3_update():
    files_lst = [
        'raw_data_fetch_time.pkl', 'bfs_us.csv', 'bfs_march_us.csv', 
        'pep_us.csv', 'bds_us.csv', 'bfs_state.csv', 'bfs_march_state.csv', 
        'pep_state.csv', 'bds_state.csv'
    ]

    for file in files_lst:
        file_to_s3(
            c.filenamer(f'data/raw_data/{file}'), 
            'emkf.data.research', 
            f'indicators/neb/raw_data/{file}'
        )


def main():
    raw_data_update()
    # s3_update()


if __name__ == '__main__':
    main()
