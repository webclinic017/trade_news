# Some imports for functions

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import math
# import csv
# import re
from datetime import datetime

# import openpyxl

import os.path
from os import path
import time

import eikon as ek






def get_name_of_latest_file_in_folder(folder_name):
    """
    Get all file names in selected direction.
    Return the latest file in that directory.
    """
    files_creation_dates = pd.DataFrame(data = [], columns=[
        "folder_name", "file_name", "num_time", "str_time"
    ])

    files_creation_dates['file_name'] = os.listdir(folder_name)
    files_creation_dates['folder_name'] = folder_name
    files_creation_dates['file_folder_and_name'] = files_creation_dates['folder_name'] + files_creation_dates['file_name']
    files_creation_dates
    for i in range(0, len(files_creation_dates)):
        file_folder_and_name = files_creation_dates['file_folder_and_name'][i]
        files_creation_dates['num_time'][i] = os.path.getctime(file_folder_and_name)
        files_creation_dates['str_time'][i] = time.ctime(os.path.getctime(file_folder_and_name))

    latest_file_dir = files_creation_dates.sort_values(by=['num_time'], ascending=False)['file_folder_and_name'].iloc[0]
    
    return latest_file_dir






def get_files_name_in_folder(folder_name):
    files_creation_dates = pd.DataFrame(data = [], columns=[
        "folder_name", "file_name", "num_time", "str_time"
    ])

    files_creation_dates['file_name'] = os.listdir(folder_name)
    files_creation_dates['folder_name'] = folder_name
    files_creation_dates['file_folder_and_name'] = files_creation_dates['folder_name'] + files_creation_dates['file_name']
    files_creation_dates
    for i in range(0, len(files_creation_dates)):
        file_folder_and_name = files_creation_dates['file_folder_and_name'][i]
        files_creation_dates['num_time'][i] = os.path.getctime(file_folder_and_name)
        files_creation_dates['str_time'][i] = time.ctime(os.path.getctime(file_folder_and_name))
        
    return list(files_creation_dates['file_folder_and_name'])





# Проверим, содержит ли all_headlines_df наш текущий ric
def set_value_of_up_to_date_ric(
        ric_now, # ric to check of existence in df
        df_with_news_to_check,
        column_name_with_rics = 'ric',
        column_name_with_dates = 'versionCreated'):
    """
    This function is checking the existance of ric in chosen df.
    If some news are in df, then function returns the oldest date in df.
    If no news are in df, then function returns 'None' for ek.get_news_headlines.
    """

    is_this_ric_in_all = ric_now in list(df_with_news_to_check[column_name_with_rics])
    print()
    print('is_this_ric_in_all:', is_this_ric_in_all, end=' ')
    if is_this_ric_in_all:
        # Если в нашем датасете присутствует этот ric,
        # то продолжаем по нему выгружать, берём мин имеющуюся дату.
        up_to_date_ric = str(df_with_news_to_check[df_with_news_to_check[column_name_with_rics] == ric_now][column_name_with_dates].min())
        print('  up_to_date_ric:', up_to_date_ric, end=' ')
    else:
        # None означает, что метод ek.get_news_headlines возьмёт текущий момент времени.
        up_to_date_ric = None
        print('  up_to_date_ric:', up_to_date_ric, end=' ')
        
    return up_to_date_ric






def get_news_headlines_with_some_tries(
        ric_now,
        headlines_count_to_request,
        up_to_date_ric,
        max_num_tries=5):
    """
    We trying to get df with headlines from eikon API with several tries
    """
    
    tries_num = 0
    while tries_num < max_num_tries:
        try:
            news_get_df = ek.get_news_headlines(
                query=ric_now,
                count=headlines_count_to_request,
                date_to=up_to_date_ric
            )
            print('got successfully', end=' ')
            break
        except Exception as e:
            print()
            print('Error when trying to get news headlines', tries_num)
            print(e)
            time.sleep(5)
            tries_num = tries_num + 1

    print('news_get_df.shape:', news_get_df.shape[0])

    return news_get_df





def prepare_headlines_df_to_adding_news_story(
        ric_now,
        news_get_df):
    """
    Now: news_get_df is collected df from ek.get_news_headlines method.
    And we want to add news story (full news text) to that df.
    So we should prepare that df for iterations with ek.get_news_story method.
    For that puspose we should add ric and story columns,
    and change the column name with headlines from text to headlines.
    """
    
    news_get_df = news_get_df.reset_index(drop=True) # чтобы юзать .loc()
    news_get_df['ric'] = ric_now
    news_get_df['story'] = np.nan
    news_get_df['headline'] = news_get_df['text']
    news_get_df = news_get_df.drop(columns=['text'])
    
    return news_get_df






def adding_news_stories_to_df_with_headlines(
        news_get_df,
        quasi_logger,
        ric_i,
        ric_now,
        headlines_slice_i,
        headlines_number,
        up_to_date_ric,
        col_name_with_story_id = 'storyId'):
    """
    The function adds full text news story to df with headlines using storyId.
    The function just prints the error in a case of error (they are rare).
    In any case the function logs results of API request to logger df.
    """

    for story_i in range(0, len(news_get_df)): # range(0, len(news_get_df))
        story_id_now = news_get_df['storyId'][story_i]

        try:
            story_now = ek.get_news_story(story_id_now)
            # Удалим из истории все тильды на всякий случай, будет sep='~'
            story_now = story_now.replace("~", "!tilda!")
            # Внесём выкаченную историю в df
            news_get_df.loc[story_i, 'story'] = story_now
            # Пометим, что мы успешно выкачали текст новости
            story_is_success = True
            print(story_i, end='; ')

        except Exception as e:
            story_is_success = False
            # Заменим NA на текст ошибки
            news_get_df.loc[story_i, 'story'] = 'error_when_trying_to_collect_story'
            # Выведем story с ошибкой
            print() # e.message
            print(
                'STORY IS OK:', story_is_success, 
                '  ric_now:', ric_now, # input to function
                '  ric_i:', ric_i, # input to function
                '  story_i:', story_i
                )
            print(e)
            time.sleep(5)

        finally:
            # В любом случае внесём в наш квази-логгер наблюдение story.
            quasi_logger = quasi_logger.append({
                "ric_i": ric_i, # input to function
                "ric_now": ric_now, # input to function
                "headlines_slice_i": headlines_slice_i, # input to function
                "headlines_number": headlines_number, # input to function
                "up_to_date_ric": up_to_date_ric, # input to function
                "story_i": story_i,
                "story_id_now": story_id_now,
                "story_is_success": story_is_success,
                "date_time": str(datetime.today())
            }, ignore_index = True)
    # At that moment the df has full text of news in col story
    return news_get_df, quasi_logger





def save_file_as_new_file_without_replacing(
        path_project_folder,
        folder_name_to_save,
        file_to_save,
        file_short_name_add_to_path):
    """
    Save the file to selected folder without replacing,
    but changing file name as a number of version at date now
    """
    # Сохраним версию all_headlines_df со всеми новостями
    # Подберём такое название, которого нет в dir, чтобы не переписывать файл.
    file_version = 1
    folder_name = path_project_folder + folder_name_to_save
    date_now = datetime.today().strftime("%Y-%m-%d")
    file_name = file_short_name_add_to_path + '_' + str(date_now) + '_v' + str(file_version) + '.csv'
    while file_name in os.listdir(folder_name):
        file_name = file_short_name_add_to_path + '_' + str(date_now) + '_v' + str(file_version) + '.csv'
        file_version = file_version + 1
    # C:/DAN/t_systems/trade_project/backup_headlines/all_headlines_df_2020-12-24_v6.csv
    file_path = folder_name + file_name
    file_to_save.to_csv(file_path, sep = ';', index=False)
    print(file_path)






def get_headlines_and_full_text_news_save(
    rics_to_loop_df,
    column_name_with_rics='ric',
    column_name_with_dates='versionCreated',
    col_name_with_story_id = 'storyId',
    headlines_count_to_request=100,
    max_num_tries=5,
    dont_collect_such_old_news='2010-01-01',
    save_logger=True,
    save_all_headlines_df=False,
    folder_name_to_save_all_headlines_df='data/backup_headlines/',
    folder_name_to_save_save_logger='data/logger/',
    ):
    global all_headlines_df
    global quasi_logger
    
    # Зададим, по какому df мы будет итерировать ric компании (её код на бирже).
    rics_to_loop_df = rics_to_loop_df.reset_index(drop=True) # чтобы юзать .loc()

    # Зададим переменную, котролирующую количество выгружаемых за раз заголовков
    headlines_count_to_request = headlines_count_to_request

    # Необходимо задать переменную до цикла как и headlines_count_to_request, далее она будет изменяться
    headlines_number = headlines_count_to_request

    # Первый цикл будет итерировать компании.
    for ric_i in range(0, len(rics_to_loop_df)): # range(0, len(rics_to_loop_df))

        # Зададим переменную, которая скажет, какой сейчас итерируется ric.
        ric_now = rics_to_loop_df.loc[ric_i, "ric"]
        print(ric_i, ric_now)

        # Цикл, который прогоняет запросы заголовков.
        # Зададим для него параметр. Прибавим +1, если исчерпали новости.
        last_headlines_request = False
        # Будем вести счёт итерацию цикла While для выгрузки заголовков
        headlines_slice_i = 0
        # Для выкачивания заголовков нужно задать дату, до которой делаем запрос.
        # Если такого ric нет в all_headlines_df со всеми новостями,
        # то не будет задавать дату, до которой выкачивать заголовки.
        # Если такой ric есть в all_headlines_df со всеми новостями,
        # то возьмём минимальную дату из all_headlines_df со всеми новостями
        while not last_headlines_request:

            # That function returns the param for ek.get_news_headlines method
            # That param means the datetime slice of news to be collected
            up_to_date_ric = set_value_of_up_to_date_ric(
                ric_now=ric_now,
                df_with_news_to_check=all_headlines_df,
                column_name_with_rics=column_name_with_rics,
                column_name_with_dates=column_name_with_dates
            )

            # Do not collect news which are too old.
            if up_to_date_ric is not None:
                if datetime.strptime(str(up_to_date_ric)[:10], "%Y-%m-%d") < datetime.strptime(dont_collect_such_old_news, "%Y-%m-%d"):
                    break

            # That function returns df with news headlines after several tries.
            news_get_df = get_news_headlines_with_some_tries(
                max_num_tries=max_num_tries,
                ric_now=ric_now,
                headlines_count_to_request=headlines_count_to_request,
                up_to_date_ric=up_to_date_ric
            )

            # Some df changes before adding full news text to that df.
            news_get_df = prepare_headlines_df_to_adding_news_story(
                ric_now, news_get_df)

            # Adding full text of news (stories) to df with headlines
            news_get_df, quasi_logger = adding_news_stories_to_df_with_headlines(
                news_get_df,
                quasi_logger,
                ric_i,
                ric_now,
                headlines_slice_i,
                headlines_number,
                up_to_date_ric,
                col_name_with_story_id=col_name_with_story_id)

            # If at that moment news_get_df has less than 100 news,
            # than it means we have exhausted news for that ric because of dates limitation.
            # And we need to change ric_now that we iterate.
            headlines_number = news_get_df.shape[0]
            if headlines_number < headlines_count_to_request:
                last_headlines_request = True

            headlines_slice_i = headlines_slice_i + 1

            # At that moment news_get_df has the slice with headlines & full-text.
            # Let's add that slice to all_headlines_df that has all news.
            all_headlines_df = all_headlines_df.append(news_get_df, ignore_index = True)
            print()
            print(str(datetime.today()), ric_now, 
                  ' ric_i:', ric_i,
                  ' headline_slice_i:', headlines_slice_i,
                  ' headline_number:', headlines_number,
                  ' not null:', news_get_df['story'].notna().sum(),
                  up_to_date_ric)
            print(all_headlines_df.shape)

            # End of headlines iterations for one selected ric

        # Save the version of all_headlines_df at every ric iteration
        if save_all_headlines_df:
            save_file_as_new_file_without_replacing(
                path_project_folder='',
                folder_name_to_save=folder_name_to_save_all_headlines_df,
                file_to_save=all_headlines_df,
                file_short_name_add_to_path='all_headlines_df')

        # Save the version of quasi_logger at every ric iteration
        if save_logger:
            save_file_as_new_file_without_replacing(
                path_project_folder='',
                folder_name_to_save=folder_name_to_save_save_logger,
                file_to_save=quasi_logger,
                file_short_name_add_to_path='quasi_logger')






def get_timeseries_of_rics_to_folder(
    list_of_rics,
    folder_for_timeseries_file='data/price_timeseries/',
    timeseries_interval=str('daily'),
    timeseries_start_date=str("2010-01-01"),
    timeseries_end_date=str("2021-06-01"),
    timeseries_is_adjusted=True,
    sleep_between_rics=0):
    
    for ric_now in list_of_rics:
        index_of_ric_now = list_of_rics.index(ric_now)
        
        print(index_of_ric_now, ric_now, end=', ')
        
        # Try to get the data via API untill the number of attempts is over
        timeseries_df = None
        number_of_attempts=1
        while (timeseries_df is None) and (number_of_attempts < 3):
            try:
                # Get df via Eikon Data API
                # Possible values of interval: 
                # 'tick', 'minute', 'hour', 
                # 'daily', 'weekly', 'monthly', 'quarterly', 'yearly' (Default 'daily')
                corax_param = 'adjusted' if timeseries_is_adjusted else 'unadjusted'
                timeseries_df = ek.get_timeseries(
                    [ric_now],
                    interval=timeseries_interval,
                    start_date=timeseries_start_date,
                    end_date=timeseries_end_date,
                    fields=['TIMESTAMP', 'VALUE', 'VOLUME', 'HIGH', 'LOW', 'OPEN', 'CLOSE', 'COUNT'],
                    corax=corax_param
                )
            except Exception:
                print('ERROR, Exception. Request fails or if server returns an error.')
                time.sleep(5)
                pass
            except ValueError:
                print('ERROR, ValueError. A parameter type or value is wrong.')
                time.sleep(5)
                pass
            
            print('Attempt', number_of_attempts, end=', ')
            
            if timeseries_df is None: # ERROR
                print('error with that attempt', end=', ')
                time.sleep(5)
            number_of_attempts = number_of_attempts + 1
            
        # End of while
        
        # If it fail several attempts then print the final error message
        if timeseries_df is None:
            print()
            print()
            print(
                '    !!! ERROR WITH THIS RIC:', 
                ric_now, 
                timeseries_interval, 
                timeseries_start_date, 
                timeseries_end_date,
                corax_param
            )
            print()
            continue
        else:
            print('NOT EMPTY', end=', ')
        
        # Add ric and date columns
        timeseries_df['ric'] = timeseries_df.columns.name 
        # Check index type to do not reset_index again
        if type(timeseries_df.index[0]) != int:
            timeseries_df['Date'] = timeseries_df.index
            timeseries_df = timeseries_df.reset_index(drop=True, inplace=False)

        # Save created df with one ric. File name contains ric name
        file_name = 'price_timeseries' + '_' + timeseries_interval + '_' + corax_param + '_' + timeseries_df.columns.name
        file_path =  folder_for_timeseries_file + file_name + '.csv'
        timeseries_df.to_csv(file_path, sep = ';', index=False)
        print()
        print(' '*(len(ric_now) + len(str(index_of_ric_now))), ', ', end='')
        print(file_path, end=', ')
        
        print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        
        if sleep_between_rics > 0:
            time.sleep(sleep_between_rics)








def computeRSI(data, time_window):
    diff = data.diff(1).dropna()        # diff in one field(one day)
 
    #this preservers dimensions off diff values
    up_chg = 0 * diff
    down_chg = 0 * diff
    
    # up change is equal to the positive difference, otherwise equal to zero
    up_chg[diff > 0] = diff[ diff > 0 ]
    
    # down change is equal to negative deifference, otherwise equal to zero
    down_chg[diff < 0] = diff[ diff < 0 ]
    
    # check pandas documentation for ewm
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.ewm.html
    # values are related to exponential decay
    # we set com=time_window-1 so we get decay alpha=1/time_window
    up_chg_avg   = up_chg.ewm(com=time_window-1 , min_periods=time_window).mean()
    down_chg_avg = down_chg.ewm(com=time_window-1 , min_periods=time_window).mean()
    
    rs = abs(up_chg_avg/down_chg_avg)
    rsi = 100 - 100/(1+rs)
    return rsi






def create_features_for_df(data_now, news_too, news_cols, window_of_change=5):
    print(data_now.shape[0], end=', ')
    # Target

    # Create columns with price changes in percent as target
    window_of_change = window_of_change
    new_col_name = 'y_change_' + str(window_of_change)
    data_now[new_col_name] = (data_now['CLOSE'].shift(periods=-window_of_change) - data_now['CLOSE']) / data_now['CLOSE']

    # Price change features

    # Create a column with window=n of price changes in percent
    window_of_change = window_of_change
    data_now['price_change_close_shift_n'] = (data_now['CLOSE'].shift(periods=0) - data_now['CLOSE'].shift(periods=window_of_change+0)) / data_now['CLOSE'].shift(periods=window_of_change+0)
    data_now['price_change_open_shift_n'] = (data_now['OPEN'].shift(periods=0) - data_now['OPEN'].shift(periods=window_of_change+0)) / data_now['OPEN'].shift(periods=window_of_change+0)
    data_now['price_change_low_shift_n'] = (data_now['LOW'].shift(periods=0) - data_now['LOW'].shift(periods=window_of_change+0)) / data_now['LOW'].shift(periods=window_of_change+0)
    data_now['price_change_high_shift_n'] = (data_now['HIGH'].shift(periods=0) - data_now['HIGH'].shift(periods=window_of_change+0)) / data_now['HIGH'].shift(periods=window_of_change+0)

    # Create columns with price changes in percent as features
    price_change_cols = []
    for i in range(0, 30):
        new_col_name = "price_change_" + str(i)
        data_now[new_col_name] = data_now['price_change_close_shift_n'].shift(periods=i)
        price_change_cols.append(new_col_name)

    # Stochastic Oscillator features

    # Create the "L14" column in the DataFrame
    data_now['L10'] = data_now['LOW'].rolling(window=10).min()
    # Create the "H14" column in the DataFrame
    data_now['H10'] = data_now['HIGH'].rolling(window=10).max()
    # Create the "%K" column in the DataFrame
    data_now['stochastic_oscillator'] = 100*((data_now['CLOSE'] - data_now['L10']) / (data_now['H10'] - data_now['L10']))/100
    # Create the "%D" column in the DataFrame
    # data_now['stochastic_oscillator_mean5'] = data_now['stochastic_oscillator'].rolling(window=5).mean()

    # Create columns with Stochastic Oscillator as features
    stochastic_oscillator_cols = []
    for i in range(0, 30):
        new_col_name = "stochastic_oscillator_" + str(i)
        data_now[new_col_name] = data_now['stochastic_oscillator'].shift(periods=i)
        stochastic_oscillator_cols.append(new_col_name)

    # MACD features

    data_now["ema26"] = data_now["CLOSE"].ewm(span = 26, adjust = False).mean()
    data_now["ema12"] = data_now["CLOSE"].ewm(span = 12, adjust = False).mean()
    data_now["macd"] = data_now["ema12"] - data_now["ema26"]
    data_now["macd_percent"] = data_now["macd"]/data_now["CLOSE"]

    # Create columns with MACD as features
    macd_cols = []
    for i in range(0, 30):
        new_col_name = "macd_percent_" + str(i)
        data_now[new_col_name] = data_now['macd_percent'].shift(periods=i)
        macd_cols.append(new_col_name)

    # RSI features

    # Create columns with RSI as features
    data_now["rsi_14"] = computeRSI(data_now["CLOSE"], 14)/100

    # Create columns with RSI as features
    rsi_cols = []
    for i in range(0, 30):
        new_col_name = "rsi_" + str(i)
        data_now[new_col_name] = data_now['rsi_14'].shift(periods=i)
        rsi_cols.append(new_col_name)
    
    features_cols = price_change_cols + stochastic_oscillator_cols + macd_cols + rsi_cols
    
    # News features
    if news_too:
        
        news_features_cols = []
        for col in news_cols:
            new_col_name = col + '_ndays'
            data_now[new_col_name] = 0
            for i in range(window_of_change):
                data_now[new_col_name] = data_now[new_col_name] + data_now[col].shift(periods=i)
            news_features_cols.append(new_col_name)
            
        features_cols = price_change_cols + stochastic_oscillator_cols + macd_cols + rsi_cols + news_features_cols
    
    
    return data_now, features_cols

























