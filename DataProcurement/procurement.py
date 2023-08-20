import pandas as pd

import requests
from bs4 import BeautifulSoup

from io import BytesIO
import zipfile
import gzip
import sys
import os
from threading import Thread
from concurrent.futures import ThreadPoolExecutor, as_completed

from DataAnalysis.utilities import *


def getStationDescription(url: str, path: str) -> bool:
    sys.stdout.write(f'\r Download {url} ...')
    sys.stdout.flush()

    h = requests.head(url=url, allow_redirects=True).headers

    if 'text' in h.get('content-type').lower():
        r = requests.get(url=url, allow_redirects=True)
        content = r.content

        filename = url.split(sep='/')[-1]
        try:
            with open(file=path + filename, mode='wb') as file:
                file.write(content)
                file.close()

        except OSError as e:
            print(e)
            return False

    sys.stdout.write(f'\r Download {url} Done\n')
    sys.stdout.flush()

    return True


def checkIfTableExists(cursor: sql.Connection, tablename: str) -> bool:
    tables = cursor.execute(f'''
                SELECT name FROM sqlite_master WHERE type=\'table\' AND name=\'{tablename}\'
            ''')

    if tables.fetchall():
        return True

    return False


def createTable(cursor: sql.Connection, table_name: str) -> None:
    sql_query = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            Stations_id INTEGER PRIMARY KEY,
            von_datum DATE,
            bis_datum DATE,
            Stationshoehe INTEGER,
            geoBreite FLOAT,
            geoLaenge FLOAT,
            Stationsname VARCHAR(50),
            Bundesland VARCHAR(50))
    '''

    cursor.execute(sql_query)


def stationDescriptionToDB(connection, path, table_name) -> None:
    cursor = connection.cursor()

    try:
        if checkIfTableExists(cursor=cursor, tablename=table_name):
            return
        else:
            createTable(cursor=cursor, table_name=table_name)
            connection.commit()

            if not checkIfTableExists(cursor=cursor, tablename=table_name):
                raise f'Error: Not able to create Table {table_name}'

        with open(file=path, mode='r', encoding="ISO-8859-1") as file:
            col_name = file.readline().split(sep=' ')
            _ = file.readline()

            for line in file:
                station_id, start_date, stop_date = line[:24].split()
                stations_hoehe, geo_breite, geo_laenge = line[24:60].split()

                line = removeBrackets(line[60:])
                station_name, bundesland, _ = re.split('\s\s+', line)
                station_name = station_name.split()[0]
                station_name = station_name.replace(',', '')
                station_name = cleanString(s=station_name)
                bundesland = cleanString(s=bundesland)

                sql_query = f'''
                    INSERT INTO {table_name}
                    ({', '.join(col_name)})
                    VALUES 
                    ({int(station_id)}, {dateToTimestamp(start_date)}, {dateToTimestamp(stop_date)}, {int(stations_hoehe)},
                     {float(geo_breite)}, {float(geo_laenge)}, \'{str(station_name)}\', \'{str(bundesland)}\')
                '''

                cursor.execute(sql_query)
                connection.commit()

            file.close()

    except IOError as e:
        print(e)

    finally:
        cursor.close()


def getAllDatasource(url: str, station_id=None) -> list[str]:
    page = requests.get(url=url, allow_redirects=True)

    soup = BeautifulSoup(page.content, 'html.parser')

    if station_id is None:
        return [a['href'] for a in soup.find_all('a') if '.zip' in a['href']]
    else:
        return [a['href'] for a in soup.find_all('a') if '.zip' in a['href'] and str(station_id).zfill(5) in a['href']]


def downloadZipAndUnzip(file_url: str) -> pd.DataFrame:
    sys.stdout.write(f'\rDownload and unzip file: {file_url} ...')
    sys.stdout.flush()

    response = requests.get(url=file_url, stream=True)

    zip_file = BytesIO(response.content)
    files = zipfile.ZipFile(zip_file)

    if len(files.namelist()) > 1:
        raise

    df = pd.read_csv(files.open(files.namelist()[0]), delimiter=';')

    sys.stdout.write(f'\rDownload and unzip file: {file_url} Done\n')
    sys.stdout.flush()

    return df


def thread_helper_station(file_url: str, result: list, thread_idx: int):
    station_histo = downloadZipAndUnzip(file_url=file_url)

    station_histo['time'] = station_histo['MESS_DATUM'].apply(lambda date: dateToDatetime64(date_DWD_format=str(date)))
    station_histo['timestamp'] = station_histo['MESS_DATUM'].apply(lambda date: dateToTimestamp(date_DWD_format=str(date)))
    station_histo.drop(columns='MESS_DATUM', inplace=True)

    result[thread_idx] = station_histo


def getStationDataset(url, station_id: int) -> pd.DataFrame:
    files = getAllDatasource(url=url, station_id=station_id)

    result = np.empty(shape=len(files), dtype=pd.DataFrame)
    threads = []
    for idx, file in enumerate(files):
        thread = Thread(target=thread_helper_station, args=(url + file, result, idx))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    df_histo = pd.concat(objs=result, ignore_index=True)

    return df_histo


def stationsToDB(connection: sql.Connection, url, download_param: str = None, value=None,
                 if_exists: str = 'continue') -> None:
    if if_exists not in ['continue', 'ignore']:
        raise f'Error: invalid argument for \"if_exists\"'

    df = pd.read_sql(sql=f'''SELECT * FROM Beschreibung_Stationen''', con=connection)

    if download_param is not None and value is not None:
        if download_param not in df.columns:
            raise f'Error: invalid download parameter. Expected {df.columns} but got {download_param}'

        df = df.loc[df[download_param] == value]

    station_ids = df[['Stations_id', 'Stationsname', 'Bundesland']].values

    all_existing_tables = getAllTables(connection=connection)

    download_count = 1
    for station_id, name, bundesland in station_ids:
        table_name = f'Station{str(station_id).zfill(5)}_{cleanString(name)}_{cleanString(bundesland)}'

        if if_exists == 'continue' and table_name not in all_existing_tables:
            station_histo: pd.DataFrame = getStationDataset(url=url, station_id=station_id)

            sys.stdout.write(
                f'\rWriting Station {station_id}-{name}-{bundesland} into {table_name} -> [{download_count} / {len(station_ids)}] ...')
            sys.stdout.flush()

            station_histo.to_sql(name=table_name, con=connection, if_exists='fail', index=False)

            sys.stdout.write(
                f'\rWriting Station {station_id}-{name}-{bundesland} into {table_name} -> [{download_count} / {len(station_ids)}] Done\n\n')
            sys.stdout.flush()

            download_count += 1


def databaseToXarray(tables: list, start_date, end_date, connection: sql.Connection):
    if isinstance(start_date, str) and isinstance(end_date, str):
        start_date = datetime.strptime(start_date, '%d.%m.%Y %H:%M').timestamp()
        end_date = datetime.strptime(end_date, '%d.%m.%Y %H:%M').timestamp()

    sql_query = f'''
        SELECT beschreibung.Stations_id, beschreibung.Stationshoehe, beschreibung.geoBreite, beschreibung.geoLaenge,
                station.timestamp, station.time, station.FF_10, station.DD_10 
        FROM Beschreibung_Stationen beschreibung, {{}} station 
        WHERE beschreibung.Stations_id = station.STATIONS_ID
            AND station.timestamp between {start_date} AND  {end_date}
                '''

    ds_stations = []
    for table in tables:
        query = sql_query.format(table)
        tmp_df = pd.read_sql(sql=query, con=connection)

        station_lat = tmp_df['geoBreite'].values[0]
        station_lon = tmp_df['geoLaenge'].values[0]

        tmp_df.rename(columns={'FF_10': 'speed', 'DD_10': 'direction'}, inplace=True)
        tmp_df.drop(columns=['geoBreite', 'geoLaenge', 'Stations_id', 'Stationshoehe', 'timestamp'], inplace=True)

        tmp_df['time'] = pd.to_datetime(tmp_df['time'])

        tmp_df = tmp_df.set_index(keys='time', drop=True)
        ds = tmp_df.to_xarray()
        ds = ds.expand_dims({'latitude': [station_lat], 'longitude': [station_lon]})

        ds_stations.append(ds)

    ds = xr.combine_by_coords(ds_stations)

    ds['longitude'].attrs = {'units': 'degrees_east', 'long_name': 'longitude'}
    ds['latitude'].attrs = {'units': 'degrees_north', 'long_name': 'latitude'}
    ds['speed'].attrs = {'units': 'm/s', 'long_name': 'wind component'}
    ds['direction'].attrs = {'units': 'degrees'}
    ds.attrs = {'creation_date': datetime.now().strftime("%m.%d.%Y, %H:%M:%S"), 'author': 'Dieter',
                'email': 'address@email.com'}

    return ds


def thread_helper_NC(url: str, path: str, file: str):
    sys.stdout.write(f'\rDownload file: {file} ...')
    sys.stdout.flush()

    response = requests.get(url=url + file, stream=True)
    compressed_file = BytesIO(response.content)
    decompressed_file = gzip.GzipFile(fileobj=compressed_file)

    with open(path + file[:-3], 'wb') as outfile:
        ret_val = outfile.write(decompressed_file.read())
        outfile.close()

    sys.stdout.write(f'\rDownload file: {file} Done\n')
    sys.stdout.flush()

    return ret_val


def downloadAllNCs(url: str, path: str, start_year, end_year):
    page = requests.get(url=url)
    soup = BeautifulSoup(page.content, 'html.parser')

    files = []
    for a in soup.find_all('a'):
        file = a['href']
        if '.nc' in file and start_year <= int(file[3:9]) <= end_year:
            if not os.path.isfile(path + file[:-3]):
                files.append(file)

    with ThreadPoolExecutor(max_workers=8) as executor:
        tasks = {executor.submit(thread_helper_NC, url, path, file): file for file in files}
        for task in as_completed(tasks):
            if task.result() <= 0:
                print(f'Failed to download {tasks[task]}')
