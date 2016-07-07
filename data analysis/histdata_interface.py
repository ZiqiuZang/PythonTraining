###author: Ziqiu###

import zipfile
import os
import pandas as pd
import numpy as np

class histdata:

    def __init__(self):
        self._base_dir = r'E:\FinanceProjects\RawData\Forex\HistDataZips'

        self.currencies = ['eurusd', 'eurchf', 'eurgbp', 'eurjpy',
                           'euraud', 'usdcad', 'usdchf', 'usdjpy',
                           'usdmxn', 'gbpchf', 'gbpjpy', 'gbpusd',
                           'audjpy', 'audusd', 'chfjpy', 'nzdjpy',
                           'nzdusd', 'xauusd', 'eurcad', 'audcad',
                           'cadjpy', 'eurnzd', 'grxeur', 'nzdcad',
                           'sgdjpy', 'usdhkd', 'usdnok', 'usdtry',
                           'xauaud', 'audchf', 'auxaud', 'eurhuf',
                           'eurpln', 'frxeur', 'hkxhkd', 'nzdchf',
                           'spxusd', 'usdhuf', 'usdpln', 'usdzar',
                           'xauchf', 'zarjpy', 'bcousd', 'etxeur',
                           'eurczk', 'eursek', 'gbpaud', 'gbpnzd',
                           'jpxjpy', 'udxusd', 'usdczk', 'usdsek',
                           'wtiusd', 'xaueur', 'audnzd', 'cadchf',
                           'eurdkk', 'eurnok', 'eurtry', 'gbpcad',
                           'nsxusd', 'ukxgbp', 'usddkk', 'usdsgd',
                           'xagusd', 'xaugbp']

        self.tick_types = ['LAST', 'BID', 'ASK']

        self._start_years = [2000, 2002, 2002, 2002,
                             2002, 2000, 2000, 2000,
                             2010, 2002, 2002, 2000,
                             2002, 2000, 2002, 2006,
                             2005, 2009, 2007, 2007,
                             2007, 2008, 2010, 2008,
                             2008, 2008, 2008, 2010,
                             2009, 2008, 2010, 2010,
                             2010, 2010, 2010, 2008,
                             2010, 2010, 2010, 2010,
                             2009, 2010, 2010, 2010,
                             2010, 2008, 2007, 2008,
                             2010, 2010, 2010, 2008,
                             2010, 2009, 2007, 2008,
                             2008, 2008, 2010, 2007,
                             2010, 2010, 2008, 2008,
                             2009, 2009]

        self._start_months = [5, 3, 3, 3,
                              8, 6, 5, 5,
                              11, 8, 5, 5,
                              8, 6, 8, 9,
                              8, 3, 3, 10,
                              3, 3, 11, 3,
                              8, 8, 8, 11,
                              5, 3, 11, 11,
                              11, 11, 11, 3,
                              11, 11, 11, 11,
                              5, 11, 11, 11,
                              11, 8, 9, 3,
                              11, 11, 8, 8,
                              11, 5, 9, 3,
                              8, 8, 11, 9,
                              11, 11, 8, 8,
                              5, 5]

    def get_time_of_data(self, currency):
        index = self.currencies.index(currency)
        start_time = pd.datetime(self._start_years[index], self._start_months[index], 1)
        return start_time

    def get_data_as_dataframe(self, currency, start_time, end_time, type='LAST'):
        start_year = start_time.year
        start_month = start_time.month
        end_year = end_time.year
        end_month = end_time.month
        years = range(start_year, end_year+1)

        base_filename = 'HISTDATA_COM_NT_{}_T_{}{}{}.zip'
        result = pd.DataFrame()

        total_months = (13 - start_month) + (12 * (end_year - start_year) - (12 - end_month))
        i = 0
        sm = 1
        em = 12
        for year in years:
            if year == years[0]:
                sm = start_month
            if year == years[-1]:
                em = end_month

            months = ['0'+str(i) if i < 10 else str(i) for i in list(range(sm, em+1))]
            print(months)
            for month in months:
                full_file_path = os.path.join(self._base_dir, base_filename.format(currency, type, year, month))
                archive = zipfile.ZipFile(full_file_path)

                if result.empty:
                    result = pd.read_csv(archive.open(archive.namelist()[0]), header=None, names=['Date-tick', type],
                                           usecols=[0,1],index_col=['Date-tick'], delimiter=';', parse_dates=True)
                else:
                    temp = pd.read_csv(archive.open(archive.namelist()[0]), header=None, names=['Date-tick', type],
                                           usecols=[0,1],index_col=['Date-tick'], delimiter=';', parse_dates=True)
                    result = result.append(temp)
                print('Added month {} of {} total'.format(i, total_months))
                i += 1

        return result