import os
import logging

from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from utilities import *


class AnalysisBuilder(object):
    metrics_build_plan = None

    def __int__(self, parameters: dict) -> None:
        try:
            self.max_time_step = int(timedelta(days=parameters['max_days']).total_seconds())
            self.con = parameters['connection']
            self.try_path = parameters['TRY_path']
            self.result_path = parameters['result_path']

            self.station_tables = parameters['station_tables']

        except KeyError as e:
            raise f'Missing expected Parameter -> ' + str(e)

        if 'thread_count' in parameters:
            self.thread_count = parameters['thread_count']
        else:
            self.thread_count = 8

        self.description = pd.read_sql(sql=f'SELECT Stations_id, geoBreite, geoLaenge FROM Beschreibung_Stationen',
                                       con=self.con)

        self.description.set_index(keys='Stations_id', inplace=True)

    @staticmethod
    def calc_time_step(try_filename: str) -> (np.core.datetime64, np.core.datetime64):
        time_start = np.datetime64(f'{try_filename[3:7]}-{try_filename[7:9]}-01 00:00:00')

        input_dt = datetime(int(try_filename[3:7]), int(try_filename[7:9]), 1)
        next_month = input_dt.replace(day=28) + timedelta(days=4)
        res = next_month - timedelta(days=next_month.day)
        time_end = np.datetime64(f'{res.date()} 23:00:00')

        return time_start, time_end

    @staticmethod
    def build_metrics(metrics: list) -> dict:
        def first_level_func(m: str) -> list:
            str_split = m.split('_')

            def sub_first_level_fuc(m_: str):
                match m_:
                    case 'MAE':
                        return calc_mean_absolute_deviation
                    case 'MAPE':
                        return calc_mean_absolute_percentage_deviation
                    case 'MSE':
                        return calc_mean_square_deviation
                    case 'RMSE':
                        return calc_root_mean_square_deviation
                    case _:
                        raise ''

            if len(str_split) == 1:
                return [None] + second_level_func(m)
            else:
                if str_split[0] == 'mean':
                    return [np.nanmean, sub_first_level_fuc(str_split[1])]
                elif str_split[0] == 'median':
                    return [np.nanmedian, sub_first_level_fuc(str_split[1])]
                elif str_split[0] == 'std':
                    return [np.nanstd, sub_first_level_fuc(str_split[1])]
                else:
                    raise ''

        def second_level_func(m: str) -> list:
            if m[:2] == 'RM':
                return [np.sqrt, np.nanmean] + third_level_func(m[2:])
            elif m[0] == 'M':
                return [np.nanmean] + third_level_func(m[1:])
            else:
                raise ''

        def third_level_func(m: str) -> list:
            match m:
                case 'AE':
                    return [calc_absolute_deviation]
                case 'APE':
                    return [calc_absolute_percentage_deviation]
                case 'SE':
                    return [calc_square_deviation]
                case _:
                    raise ''

        result = {}
        for metric in metrics:
            tmp_result = first_level_func(metric)
            tmp_result = np.asarray(tmp_result)

            result.update({metric: tmp_result.T})

        return result

    def run_analysis(self, func, after_each_station=None, **kwargs):
        self.metrics_build_plan = self.build_metrics(kwargs['metrics'])

        for try_file in sorted(os.listdir(path=self.try_path)):
            time_start, time_end = self.calc_time_step(try_filename=try_file)

            try:
                ds_try = xr.load_dataset(self.try_path + try_file)

                for i, table in enumerate(self.station_tables):
                    df = pd.read_sql(sql=f'SELECT STATIONS_ID, time, FF_10 FROM {table}', con=self.con)
                    df['FF_10'] = df['FF_10'].replace(to_replace=-999.0, value=np.nan)
                    df['time'] = pd.to_datetime(df['time'])
                    df.set_index(keys='time', drop=True, inplace=True)
                    df = df.groupby(pd.Grouper(freq='H')).mean()

                    station_id = int(df['STATIONS_ID'].iloc[0])

                    lon = self.description['geoLaenge'].loc[station_id]
                    lat = self.description['geoBreite'].loc[station_id]

                    df.drop(columns=['STATIONS_ID'], inplace=True)

                    dict_station_result = {}

                    flag = True
                    start = time_start
                    stop = start + self.max_time_step
                    while flag:
                        if stop > time_end:
                            stop = time_end
                            flag = False

                        df_tmp = df.loc[start:stop]
                        if df_tmp.empty:
                            flag = False
                            continue

                        func_param = dict(ds=ds_try,
                                          df=df_tmp,
                                          longitude=lon,
                                          latitude=lat,
                                          )

                        dict_tmp_result = func(func_param, kwargs)

                        if dict_station_result:
                            for key, value in dict_tmp_result.items():
                                dict_station_result[key] = np.concatenate((dict_station_result[key], value))
                        else:
                            dict_station_result = dict_tmp_result

                    if after_each_station is not None:
                        after_each_station(dict_station_result, kwargs)

            except Exception as e:
                logging.error(f'plot_multiple_stations_time() -> {e}')
                continue


