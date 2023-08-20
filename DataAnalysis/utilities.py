import math
import re

import sqlite3 as sql
import numpy as np
import xarray as xr

from datetime import datetime


def getAllTables(connection: sql.Connection) -> list:
    cursor = connection.cursor()

    sql_query = '''SELECT * FROM sqlite_master WHERE type=\'table\' '''
    cursor.execute(sql_query)

    result = [row[1] for row in cursor.fetchall()]

    cursor.close()

    return result


def dateToDate(date_DWD_format) -> datetime:
    digits = int(math.log10(int(date_DWD_format)) + 1)

    if digits == 8:
        date = datetime.strptime(date_DWD_format, '%Y%m%d')
    else:
        date = datetime.strptime(date_DWD_format, '%Y%m%d%H%M')

    return date


def dateToDatetime64(date_DWD_format) -> np.datetime64:
    return np.datetime64(dateToDate(date_DWD_format=date_DWD_format))


def dateToTimestamp(date_DWD_format) -> float:
    return dateToDate(date_DWD_format=date_DWD_format).timestamp()


def removeBrackets(s) -> str:
    return re.sub('\(.*?\)', '', s)


def cleanString(s: str) -> str:
    special_char_map = {ord('ä'): 'ae', ord('Ä'): 'Ae',
                        ord('ü'): 'ue', ord('Ü'): 'Ue',
                        ord('ö'): 'oe', ord('Ö'): 'Oe',
                        ord('ß'): 'ss',
                        ord('-'): '_', ord('/'): '_', ord('.'): '_'}

    return s.translate(special_char_map)


def XarrayToNetCDF(path: str, xarray) -> None:
    xarray.to_netcdf(path=path)


def find_nearest(array, value):
    array = np.asarray(array)
    idx = (np.abs(array - value)).argmin()

    y = int(idx / np.shape(array)[0])
    x = idx % np.shape(array)[1]
    return x, y


def calc_mask(dwd_ds, x, y, radius):
    shape = (dwd_ds.dims.get('Y'), dwd_ds.dims.get('X'))
    distances = np.zeros(shape=shape)

    for i, y_ in enumerate(dwd_ds['Y'].values):
        for j, x_ in enumerate(dwd_ds['X'].values):
            distances[i, j] = math.sqrt((x_ - x) ** 2 + (y_ - y) ** 2)

    distances_da = xr.DataArray(data=distances,
                                dims=['Y', 'X'],
                                coords=dict(distances=(['Y', 'X'], distances),
                                            X=dwd_ds['X'],
                                            Y=dwd_ds['Y'])
                                )

    return distances_da < radius


def to_coordinate(lat_station, lon_station, dwd_ds):
    lat_x, lat_y = find_nearest(dwd_ds['lat'].values, lat_station)
    lon_x, lon_y = find_nearest(dwd_ds['lon'].values, lon_station)

    return dwd_ds['X'][lon_x].values, dwd_ds['Y'][lat_y].values


def station_to_dwd_grid(df, lat_station, lon_station, dwd_ds, radius):
    station_grid = []
    shape = (dwd_ds.dims.get('Y'), dwd_ds.dims.get('X'))

    for ff in df['FF_10'].values:
        station_grid.append(np.full(shape=shape, fill_value=ff))

    station_ds = xr.Dataset(dict(FF=(['time', 'Y', 'X'], station_grid)),
                            coords=dict(time=df.index.values,
                                        Y=dwd_ds['Y'].values,
                                        X=dwd_ds['X'].values)
                            )

    x, y = to_coordinate(lat_station=lat_station, lon_station=lon_station, dwd_ds=dwd_ds)

    mask = calc_mask(dwd_ds=dwd_ds, x=x, y=y, radius=radius)

    return station_ds.where(mask, drop=False)


def calc_mean_absolute_deviation(first: xr.DataArray, second: xr.DataArray, x_key: str, y_key: str):
    da_div = calc_absolute_deviation(first=first, second=second, x_key=x_key, y_key=y_key)
    da_mean = da_div.mean(dim='time')
    da_mean = da_mean.rename(AE='MAE')
    return da_mean


def calc_mean_absolute_percentage_deviation(first: xr.DataArray, second: xr.DataArray, x_key: str, y_key: str):
    da_div = calc_absolute_percentage_deviation(first=first, second=second, x_key=x_key, y_key=y_key)
    da_mean = da_div.mean(dim='time')
    da_mean = da_mean.rename(APE='MAPE')
    return da_mean


def calc_mean_square_deviation(first: xr.DataArray, second: xr.DataArray, x_key: str, y_key: str):
    da_div = calc_square_deviation(first=first, second=second, x_key=x_key, y_key=y_key)
    da_mean = da_div.mean(dim='time')
    da_mean = da_mean.rename(SE='MSE')
    return da_mean


def calc_root_mean_square_deviation(first: xr.DataArray, second: xr.DataArray, x_key: str, y_key: str):
    da_div = calc_square_deviation(first=first, second=second, x_key=x_key, y_key=y_key)
    da_mean = da_div.mean(dim='time')
    da_mean = da_mean ** (1/2)
    da_mean = da_mean.rename(SE='RMSE')
    return da_mean


def pre_calc(first: xr.DataArray, second: xr.DataArray, x_key: str, y_key: str):
    if first.sizes.get(x_key) != second.sizes.get(x_key):
        print(f'{np.shape(first)} != {np.shape(second)}')
        raise f'{np.shape(first)} != {np.shape(second)}'

    if first.sizes.get('time') > second.sizes.get('time'):
        first = first.sel(time=second['time'])

    elif first.sizes.get('time') < second.sizes.get('time'):
        second = second.sel(time=first['time'])

    return first, second


def calc_absolute_deviation(first: xr.DataArray, second: xr.DataArray, x_key: str, y_key: str):
    x_coord, y_coord = first[x_key], first[y_key]

    first, second = pre_calc(first=first, second=second, x_key=x_key, y_key=y_key)

    diff_ary = []
    for f, s in zip(first, second):
        diff_ary.append(np.abs(f - s))

    da_div = xr.Dataset(data_vars=dict(AE=(['time', y_key, x_key], diff_ary)),
                        coords={'time': first['time'], x_key: x_coord, y_key: y_coord})

    return da_div


def calc_absolute_percentage_deviation(first: xr.DataArray, second: xr.DataArray, x_key: str, y_key: str):
    x_coord, y_coord = first[x_key], first[y_key]

    first, second = pre_calc(first=first, second=second, x_key=x_key, y_key=y_key)

    diff_ary = []
    for f, s in zip(first, second):
        tmp = (np.abs(f - s) / s) * 100
        diff_ary.append(tmp)

    da_div = xr.Dataset(data_vars=dict(APE=(['time', y_key, x_key], diff_ary)),
                        coords={'time': first['time'], x_key: x_coord, y_key: y_coord})

    return da_div


def calc_square_deviation(first: xr.DataArray, second: xr.DataArray, x_key: str, y_key: str):
    x_coord, y_coord = first[x_key], first[y_key]

    first, second = pre_calc(first=first, second=second, x_key=x_key, y_key=y_key)

    diff_ary = []
    for f, s in zip(first, second):
        diff_ary.append((f - s) ** 2)

    da_div = xr.Dataset(data_vars=dict(SE=(['time', y_key, x_key], diff_ary)),
                        coords={'time': first['time'], x_key: x_coord, y_key: y_coord})

    return da_div


def single_station_to_grid(data, lat, long) -> xr.Dataset:
    shape = (len(lat), len(long))

    new_data = []
    for s in data['speed'].values:
        grid = np.full(shape=shape, fill_value=s)
        new_data.append(grid)

    ds = xr.Dataset(data_vars=dict(speed=(['time', 'latitude', 'longitude'], new_data)),
                    coords=dict(time=data['time'].values,
                                latitude=lat,
                                longitude=long))
    return ds
