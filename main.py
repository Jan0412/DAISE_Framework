import sqlite3 as sql
import os
import yaml
import logging

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from DataAnalysis.analysis import *
from DataProcurement.procurement import *

__DATA_PATH = './Data/'


def download_station_data(con: sql.Connection, config, download_param: str, value: str) -> None:
    if not os.path.isfile(__DATA_PATH + config['dwd_station_description']):
        getStationDescription(url=config['dwd_station_url'] + config['dwd_station_description'], path=__DATA_PATH)

    stationDescriptionToDB(connection=con, path=__DATA_PATH + config['dwd_station_description'],
                           table_name='Beschreibung_Stationen')

    stationsToDB(download_param=download_param, value=value, connection=con, url=config['dwd_station_url'])


def download_grid_data(config, FROM: int, TO: int):
    if not os.path.isdir(__DATA_PATH + 'TRY/'):
        os.mkdir(__DATA_PATH + 'TRY/')

    downloadAllNCs(url=config['dwd_try_url'], path=__DATA_PATH + 'TRY/', start_year=FROM, end_year=TO)


def main() -> None:
    config = open(file='config.yaml', mode='r')
    config = yaml.load(stream=config, Loader=yaml.FullLoader)

    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('-ds', '--download_station_dataset', type=str, default=None,
                        help=f"Download all station tables based on the {config['standard_station_download_param']}")
    parser.add_argument('-dg', '--download_try_dataset', type=str, default=None,
                        help='Download the TRY dataset for a given period. Formal must be \"<from year>-<to year>\"')
    args = vars(parser.parse_args())

    VALUE = args['download_station_dataset']

    from_to = str(args['download_try_dataset']).split(sep='-')
    FROM = int(from_to[0])
    TO = int(from_to[1])

    con = None
    try:
        con = sql.connect(database=__DATA_PATH + config['station_wind_speed_db_name'])

        if VALUE is not None:
            download_station_data(con=con, config=config, download_param=config['standard_station_download_param'], value=VALUE)
        if FROM is not None and TO is not None:
            download_grid_data(config=config, FROM=FROM, TO=TO)

    except Exception as e:
        print(e)

    finally:
        if con:
            con.close()

    pass


if __name__ == '__main__':
    logging.basicConfig(filename='logger.log',
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.INFO)

    main()