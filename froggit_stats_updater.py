#v1.0.0

#Modules
import sqlite3
import datetime
import calendar
from datetime import datetime
import pandas as pd
import logging

today = datetime.today().strftime("%Y-%m-%d_%Hh%M")
prog_start = datetime.today()


'''-------------------------------------------------------------------------------------------
------------------------------- 0 - Preparation -------------------------------------------
-------------------------------------------------------------------------------------------'''

# Indicate if local run or on synology NAS
synology = False

automatic_report = 1 # If 1 automatic report dates, if 0 manual report dates
manual_year = 2019 # Integer between 2019-202x
manual_month = 9 # Integer between 1-12

# Path definition
if synology == True:
    logging.debug('    Program is running on Synology')
    mainPath = "/volume1/python_scripts/"
    pathReportData = mainPath + "Weather_Report/Data/"
    pathPegeFroggit = "/volume1/homes/Pege_admin/Python_scripts/" 
    logPath = pathReportData+"Logs/"
else:
    logging.debug('    Program is running locally')
    mainPath = r"C:\Users\neo_1\Dropbox\Projects\Programing\\"    
    pathReportData = mainPath + "Dev\Weather Report\Data\\"
    pathPegeFroggit = mainPath
    logPath = pathReportData+"Logs\\"

today = str(datetime.today().strftime('%Y-%m-%d'))
loggerFilePath = logPath + today + '_Froggit stats updater.log'
print(loggerFilePath)

# Add logging to script
logging.basicConfig(filename=loggerFilePath, level=logging.INFO, format=' %(asctime)s - %(levelname)s - %(message)s', force=True)

logging.info("--------------------------------------")
logging.info("PROGRAM START")
logging.info("--------------------------------------")

''' Sequence of steps
1. List all possible report dates
2. Get list of existing report dates
3. Compare list of dates
4. Loop through new report dates
5. Update weather stats
6. Update list of existing report dates
'''

'''-------------------------------------------------------------------------------------------
------------------------- 1 - Connect to DB --------------------------------------------------
-------------------------------------------------------------------------------------------'''

db_start = datetime.today() # for logger

# Report database (pege_froggit_weather_stats)
dbReportData='pege_froggit_weather_stats.sqlite'   
try:
    connReportData = sqlite3.connect(pathReportData+dbReportData)
    logging.info("    Connection to DB "+dbReportData+" successful") # for logger
except:
    logging.error("    Connection to DB "+dbReportData+" failed") # for logger

# Weather station database (pege_froggit)
dbPegeFroggit = 'pege_db.sqlite'  
try:
    connPegeFroggit = sqlite3.connect(pathPegeFroggit + dbPegeFroggit)
    logging.info("    Connection to DB "+dbPegeFroggit+" successful") # for logger
except:
    logging.error("    Connection to DB "+dbPegeFroggit+" failed") # for logger


db_end = datetime.today() # for logger
prog_runtime = db_end - db_start # for logger
logging.info("    Database connection runtime: "+str(prog_runtime)) # for logger

logging.info("1 - Connection to databases successful") # for logger
print("1 - Connection to databases successful")


'''-------------------------------------------------------------------------------------------
------------------------- 2 - Preparation of dates -------------------------------------------
-------------------------------------------------------------------------------------------'''

date_start = datetime.today() # for logger

# Load databases into dataframes
pege_froggit_full_df = pd.read_sql_query("SELECT * from pege_froggit_weather_data", connPegeFroggit)
report_data_df = pd.read_sql_query("SELECT * from report_dates", connReportData)

# List dates in pege_froggit DB

# Define timestamp as date variable
pege_froggit_full_df['timestamp'] = pd.to_datetime(pege_froggit_full_df['timestamp'])
pege_froggit_full_df.sort_values(by='timestamp', inplace=True)

#Add date columns to dataframe
pege_froggit_full_df['year-month'] = pege_froggit_full_df['timestamp'].dt.to_period('M')
pege_froggit_full_df['year-month'] = pege_froggit_full_df['year-month'].astype(str).str.replace(r'-', "")
pege_froggit_full_df['year-month'] = pege_froggit_full_df['year-month'].astype(int)
pege_froggit_full_df['year'] = pege_froggit_full_df['timestamp'].dt.year
pege_froggit_full_df['month'] = pege_froggit_full_df['timestamp'].dt.month 
pege_froggit_full_df['day'] = pege_froggit_full_df['timestamp'].dt.day 

firstYear = pege_froggit_full_df['year'].min()
logging.debug('   First year: '+str(firstYear)) # for logger
lastYear = report_data_df['year'].max()
logging.debug('   Last year: '+str(lastYear)) # for logger
lastMonth = int(str((report_data_df['yearMonth'].max()))[4:6])

years = []
i=firstYear
while i < lastYear+1:
    years.append(str(i))
    i = i + 1

availableYears=lastYear-firstYear
logging.debug('   Available years: '+str(availableYears)) # for logger

firstNewMonth = datetime(int(lastYear),int(lastMonth),int("01"))
pege_froggit_new_df = pege_froggit_full_df[pege_froggit_full_df['timestamp'] > firstNewMonth]

# Get list of all year-month pairs in pege froggit DB
pege_froggit_full_reportMonths = pege_froggit_full_df['year-month'].drop_duplicates(keep='first', inplace=False).tolist()
pege_froggit_new_reportMonths = pege_froggit_new_df['year-month'].drop_duplicates(keep='first', inplace=False).tolist()

# Get list of all year-month pairs in temps stats table
temp_stats_monthly_df = pd.read_sql_query("SELECT * from temp_stats_monthly", connReportData)
temp_stats_monthly_reportMonths_df = temp_stats_monthly_df['yearMonth'].drop_duplicates(keep='first', inplace=False)

# Add number of days in month to report dates table
report_dates_df = pd.read_sql_query("SELECT * from report_dates", connReportData)

for j in pege_froggit_full_reportMonths:
    year = int(str(j)[0:4])
    month = int(str(j)[4:6])
    days_in_month = calendar.monthrange(year, month)[1]
    cur = connReportData.cursor()
    if j not in set(report_dates_df['yearMonth']):
        cur.execute('INSERT INTO report_dates (year, month, yearMonth, daysInMonth) VALUES (?, ?, ?, ?)', (year, month, j, days_in_month))    
        logging.debug('   Data for Year-Month '+str(j)+' inserted') # for logger
    else:
        cur.execute('UPDATE report_dates SET year = ?, month = ?, yearMonth = ?, daysInMonth = ? WHERE year = ? AND month = ?', (year, month, j, days_in_month, year, month))
        logging.debug('   Data for Year-Month '+str(j)+' updated') # for logger
    connReportData.commit()
    j = j + 1

# Create list with months
months = []
i=1
while i < 13:
    months.append(str(i))
    i = i + 1

date_end = datetime.today() # for logger
prog_runtime = date_end - date_start # for logger
logging.info("    Date identification runtime: "+str(prog_runtime)) # for logger

logging.info("2 - Dates prepared") # for logger
print("2 - Dates prepared")


'''-------------------------------------------------------------------------------------------
------------------------- 3 - Create / Update monthly SQL table ------------------------------
-------------------------------------------------------------------------------------------'''

# Create list with years

import froggit_monthly_data_creator

logging.info("3 - Monthly SQL table created")
print("3 - Monthly SQL table created")

'''-------------------------------------------------------------------------------------------
------------------------- 4 - Temperature data -------------------------------------------
-------------------------------------------------------------------------------------------'''

temp_start = datetime.today()

# Open temp stats table
temp_stats_monthly_df = pd.read_sql_query("SELECT * from temp_stats_monthly", connReportData)

# Open report dates table (required to check for new report dates)
report_dates_df = pd.read_sql_query("SELECT * from report_dates", connReportData)

# Loop through each year-month pair
for yearMonth in pege_froggit_new_reportMonths:
    # Subset of whole DF
    temp_df = pege_froggit_new_df[(pege_froggit_new_df['year-month'] == yearMonth)][['year', 'month', 'year-month', 'day',  'outdoor_temp_data']]

    # Extreme temps per year-month
    max_temp_m = temp_df['outdoor_temp_data'].max()
    min_temp_m = temp_df['outdoor_temp_data'].min()
    med_temp_m = temp_df['outdoor_temp_data'].median()
    avg_temp_m= round(temp_df['outdoor_temp_data'].mean(),1)
    
    # Extreme temp days per year-month
    '''
    Tmax ≥ 35 °C    Wüstentag
    Tmax ≥ 30 °C    Heißer Tag
    Tmin ≥ 20 °C    Tropennacht
    Tmax ≥ 25 °C    Sommertag
    Tmed ≥ 5 °C     Vegetationstag
    Tmin < 0 °C     Frosttag
    Tmax < 0 °C     Eistag
    '''

    days_in_month = int(report_dates_df[(report_dates_df['yearMonth'] == yearMonth)]['daysInMonth'])

    Wüstentag = {}
    HeißerTag = {}
    Tropennacht = {}
    Sommertag = {}
    Vegetationstag = {}
    Frosttag = {}
    Eistag = {}
    
    i=1
    while i <= days_in_month:
        maxTempDate=temp_df[temp_df['day'] == i]['outdoor_temp_data'].max()
        minTempDate=temp_df[temp_df['day'] == i]['outdoor_temp_data'].min()
        medTempDate=temp_df[temp_df['day'] == i]['outdoor_temp_data'].median()
        if maxTempDate >= 35:
            Wüstentag.update({i:maxTempDate})
        elif maxTempDate >= 30:
            HeißerTag.update({i:maxTempDate})
        elif maxTempDate >= 25:
            Sommertag.update({i:maxTempDate})
        elif maxTempDate < 0:
            Eistag.update({i:maxTempDate})
        
        if minTempDate >= 20:
            Tropennacht.update({i:minTempDate})
        elif minTempDate <0:
            Frosttag.update({i:minTempDate})

        if medTempDate >= 5:
            Vegetationstag.update({i:medTempDate})
        
        i = i+1

    Wüstentage, HeißeTage, Sommertage, Eistage, Tropennächte, Frosttage, Vegetationstage = str(len(Wüstentag)), str(len(HeißerTag)), str(len(Sommertag)), str(len(Eistag)), str(len(Tropennacht)), str(len(Frosttag)), str(len(Vegetationstag))

    year = str(yearMonth)[0:4]
    month = str(yearMonth)[4:6]

    # Update SQLite table
    cur = connReportData.cursor()
    if str(yearMonth) not in set(temp_stats_monthly_df['yearMonth']):
        cur.execute('INSERT INTO temp_stats_monthly (year, month, yearMonth, max_temp_m, min_temp_m, med_temp_m, avg_temp_m, wuestentage, heisseTage, tropennaechte, sommertage, vegetationstage, frosttage, eistage) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (year, month, yearMonth, max_temp_m, min_temp_m, med_temp_m, avg_temp_m, Wüstentage,HeißeTage, Tropennächte, Sommertage, Vegetationstage, Frosttage, Eistage))
    else:
        cur.execute('UPDATE temp_stats_monthly SET year = ?, month = ?, yearMonth = ?, max_temp_m = ?, min_temp_m = ?, med_temp_m = ?, avg_temp_m = ?, wuestentage = ?, heisseTage = ?, tropennaechte = ?, sommertage = ?, vegetationstage = ?, frosttage = ?, eistage = ? WHERE yearMonth = ?', (year, month, yearMonth, max_temp_m, min_temp_m, med_temp_m, avg_temp_m, Wüstentage, HeißeTage, Tropennächte, Sommertage, Vegetationstage, Frosttage, Eistage, yearMonth))
    connReportData.commit()

    yearMonth = yearMonth + 1

temp_end = datetime.today()
prog_runtime = temp_end - temp_start
logging.info("    Temperature data runtime: "+str(prog_runtime))

logging.info("4 - Temp data prepared")
print("4 - Temp data prepared")


'''-------------------------------------------------------------------------------------------
--------------------------------- 5 - Rain data ----------------------------------------------
-------------------------------------------------------------------------------------------'''

rain_start = datetime.today()

# Open temp stats table
rain_stats_monthly_df = pd.read_sql_query("SELECT * from rain_stats_monthly", connReportData)

# Open report dates table (required to check for new report dates)
report_dates_df = pd.read_sql_query("SELECT * from report_dates", connReportData)

# Loop through each year-month pair
for yearMonth in pege_froggit_new_reportMonths:
    # Subset of whole DF
    temp_df = pege_froggit_new_df[(pege_froggit_new_df['year-month'] == yearMonth)][['year', 'month', 'year-month', 'day',  'rain_monthly_data', 'rain_daily_data']]

    year = str(yearMonth)[0:4]
    month = str(yearMonth)[4:6]

    # Rain quantity per year-month
    max_rain_m = temp_df['rain_monthly_data'].max()

    rain_days = {}
    max_rain_d_all = {}
    # Retrieve number of rainy days for current month for each year
    i=1
    rainy_days = 0
    while i <= days_in_month:
        if temp_df[temp_df['day'] == i]['rain_daily_data'].max() > 0:
            rainy_days = rainy_days + 1
        i = i + 1

    # Update SQLite table
    cur = connReportData.cursor()
    if str(yearMonth) not in set(rain_stats_monthly_df['yearMonth']):
        cur.execute('INSERT INTO rain_stats_monthly (year, month, yearMonth, max_rain_m, rainy_days) VALUES (?, ?, ?, ?, ?)', (year, month, yearMonth, max_rain_m, rainy_days))
    else:
        cur.execute('UPDATE rain_stats_monthly SET year = ?, month = ?, yearMonth = ?, max_rain_m = ?, rainy_days = ? WHERE yearMonth = ?', (year, month, yearMonth, max_rain_m, rainy_days, yearMonth))
    connReportData.commit()

    yearMonth = yearMonth + 1 

rain_end = datetime.today()
prog_runtime = rain_end - rain_start
logging.info("    Rain data runtime: "+str(prog_runtime))

logging.info("5 - Rain data prepared")    
print("5 - Rain data prepared") 

'''-------------------------------------------------------------------------------------------
--------------------------------- 6 - Solar data ---------------------------------------------
-------------------------------------------------------------------------------------------'''

solar_start = datetime.today()

# Open temp stats table
sun_stats_monthly_df = pd.read_sql_query("SELECT * from sun_stats_monthly", connReportData)

# Open report dates table (required to check for new report dates)
report_dates_df = pd.read_sql_query("SELECT * from report_dates", connReportData)

# Loop through each year-month pair
for yearMonth in pege_froggit_new_reportMonths:
    # Subset of whole DF
    temp_df = pege_froggit_new_df[(pege_froggit_new_df['year-month'] == yearMonth)][['year', 'month', 'year-month', 'day', 'timestamp',  'solar_radiation_data']]

    year = str(yearMonth)[0:4]
    month = str(yearMonth)[4:6]

    # Sunshine hours per year-month
    day=1
    sunshine_hours_m = 0
    while day <= days_in_month:
        sunshine_hours_d = 0
        hour = 0
        while hour < 24:
            sunshine_hour = (temp_df.loc[(temp_df['day'] == day) & (temp_df['year-month'] == yearMonth) & (temp_df['timestamp'].dt.hour == hour)]['solar_radiation_data'].mean()) * 0.0079
            if sunshine_hour > 120:
                sunshine_hours_d = sunshine_hours_d + 1
            hour = hour + 1
        day=day+1
        sunshine_hours_m = sunshine_hours_m + sunshine_hours_d

    # Update SQLite table
    cur = connReportData.cursor()
    if str(yearMonth) not in set(sun_stats_monthly_df['yearMonth']):
        cur.execute('INSERT INTO sun_stats_monthly (year, month, yearMonth, sunshine_hours_m) VALUES (?, ?, ?, ?)', (year, month, yearMonth, sunshine_hours_m))
    else:
        cur.execute('UPDATE sun_stats_monthly SET year = ?, month = ?, yearMonth = ?, sunshine_hours_m = ? WHERE yearMonth = ?', (year, month, yearMonth, sunshine_hours_m, yearMonth))
    connReportData.commit()

    yearMonth = yearMonth + 1 


solar_end = datetime.today()
prog_runtime = solar_end - solar_start
logging.info("    Solar data runtime: "+str(prog_runtime))

logging.info("6 - Solar data prepared")
print("6 - Solar data prepared")


prog_end = datetime.today()
prog_runtime = prog_end - prog_start
logging.info("    Total program runtime: "+str(prog_runtime))

logging.info("--------------------------------------")
logging.info("PROGRAM ENDED")
logging.info("--------------------------------------")
