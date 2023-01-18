#v1.0.0

import sqlite3
import datetime
import calendar
from datetime import date
import pandas as pd
import numpy as np
import os

# Indicate if local run or on synology NAS
synology = True

# Path definition
if synology == True:
    mainPath = "/volume1/python_scripts/"
    pathReportData = mainPath + "WeatherReport/Data/"
    pathPegeFroggit = "/volume1/homes/Pege_admin/Python_scripts/" 
else:
    mainPath = r"C:\Users\neo_1\Dropbox\Projects\Programing\\"    
    pathReportData = mainPath + "\WeatherReport\Data\\"
    pathPegeFroggit = mainPath
print(mainPath)

'''-------------------------------------------------------------------------------------------
------------------------- 1 - Define Report Period -------------------------------------------
-------------------------------------------------------------------------------------------'''
# Returns the current local date
today = datetime.date.today().replace(day=1)
# Get first day of current month
# Substract one day to get to last day of previous month
lastMonth = today - datetime.timedelta(days=1)

year=lastMonth.strftime("%Y")
month=lastMonth.strftime("%m")

if len(month)==1:
    month="0"+month
else:
    month=month

dbPegeFroggit = 'pege_db.sqlite'  
try:
    connPegeFroggit = sqlite3.connect(pathPegeFroggit + dbPegeFroggit)
except:
    print("error")

pf_full_df = pd.read_sql_query("SELECT strftime('%Y', date(timestamp)) as year, strftime('%m', date(timestamp)) as month, strftime('%d', date(timestamp)) as day, strftime('%H', datetime(timestamp)) as hour,  outdoor_temp_data, rain_daily_data, solar_radiation_data from pege_froggit_weather_data", connPegeFroggit)

pf_month_df = pf_full_df[(pf_full_df['year'] == str(year))][(pf_full_df['month'] == str(month))]

daily_stats_df = pf_month_df.groupby(['year','month','day']).agg(temp_max=('outdoor_temp_data','max'),temp_min=('outdoor_temp_data','min'),temp_avg=('outdoor_temp_data','mean'),temp_med=('outdoor_temp_data','median'),rain_qty=('rain_daily_data','max'))

solar_stats_df = pf_month_df.groupby(['year','month','day','hour']).agg(solar_radiation =('solar_radiation_data','sum'))

solar_stats_df["sunshine_hour"] = np.where(solar_stats_df["solar_radiation"] >= 15189.87, 1, 0)

solar_stats_df["solar_radiation"] *= 0.0079

solar_stats_df = solar_stats_df.groupby(['year','month','day']).agg(solar_radiation=('solar_radiation','sum'),sunshine_hours=('sunshine_hour','sum'))

daily_stats_df = daily_stats_df.merge(solar_stats_df, how='left', on=['year','month','day'])
 
daily_stats_df = daily_stats_df.round({'temp_med': 1,'temp_avg': 1, 'solar_radiation': 0})
daily_stats_df['year']=year
daily_stats_df['month']=month
daily_stats_df['day'] = range(1, len(daily_stats_df) + 1)

# Report database (pege_froggit_weather_stats)
dbReportData = 'pege_froggit_weather_stats.sqlite'   

try:
    connReportData = sqlite3.connect(pathReportData+dbReportData)
except:
    print("error")
        
# create a temp table to hold the new data that'll be diffed against the final table
daily_stats_df.to_sql('daily_stats_temp', connReportData, if_exists='replace', index=False)
connReportData.execute('''
    INSERT INTO daily_stats(year, month, day, temp_min, temp_max, temp_avg, temp_med, rain_qty, solar_radiation, sunshine_hours)
    SELECT 
        daily_stats_temp.year, 
        daily_stats_temp.month, 
        daily_stats_temp.day, 
        daily_stats_temp.temp_min, 
        daily_stats_temp.temp_max, 
        daily_stats_temp.temp_avg,
        daily_stats_temp.temp_med,
        daily_stats_temp.rain_qty,
        daily_stats_temp.solar_radiation,
        daily_stats_temp.sunshine_hours
    FROM 
        daily_stats_temp
    WHERE NOT EXISTS (
        SELECT 1 FROM daily_stats WHERE daily_stats.day = daily_stats_temp.day AND daily_stats.month = daily_stats_temp.month AND daily_stats.year = daily_stats_temp.year
    )
''')
connReportData.execute('DROP TABLE IF EXISTS daily_stats_temp')
connReportData.commit()