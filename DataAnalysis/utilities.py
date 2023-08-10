import datetime
import math
import re
import numpy as np


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