#Import the required libraries
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from configparser import ConfigParser
from selenium.webdriver.common.by import By
from selenium.webdriver import DesiredCapabilities 
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import *
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import StaleElementReferenceException

import pandas as pd
import numpy as np
import os
import pathlib

import time
from time import sleep
from datetime import timedelta, datetime
from dateutil import parser
import glob
import stat
from functools import wraps

from tqdm import tqdm_notebook
from tqdm import tqdm
import sys
import gzip
import shutil

#Set working directory
os.chdir(r'D:\TD\FMF\Strategies')
##################################################################################################################

# Local configurations to use chrome login information
os.environ['USER_DATA_DIR'] = r'C:\\Users\\{USER}\\AppData\\Local\\Google\\Chrome\\User Data'
os.environ['DEFAULT_DOWNLOAD_DIR'] = 'D:\\TD\\FMF\\Strategies\\temp'
os.environ['CHART_URL'] = 'https://www.tradingview.com/chart/h9CR9nMd/'

# Set options
options = Options()
options.add_argument('--user-data-dir=C:\\Users\\{USER}\\AppData\\Local\\Google\\Chrome\\User Data')
options.add_argument("--start-maximized")
options.add_experimental_option("prefs", {
    "download.default_directory": os.getenv('DEFAULT_DOWNLOAD_DIR'),
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})

# Set capabilities
capabilities = DesiredCapabilities.CHROME.copy()

# Initialize webdriver
chromeVariablePath = r'.\chromedriver.exe'

try:
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options, desired_capabilities=capabilities)
except ValueError:
    driver = webdriver.Chrome(executable_path=chromeVariablePath, options=options, desired_capabilities=capabilities)

driver.implicitly_wait(1)

# Open URL, retry if needed
for _ in range(2):
    try:
        driver.get(os.getenv('CHART_URL'))
        break
    except WebDriverException:
        time.sleep(5)

wait = WebDriverWait(driver, 5)

##########################################	PRELIMINARY FUNCTIONS

##########################################  REPEAT FUNCTION
def retry(max_tries=3, delay_seconds=2):
    def decorator_retry(func):
        @wraps(func)
        def wrapper_retry(*args, **kwargs):
            tries = 0
            while tries < max_tries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    tries += 1
                    if tries == max_tries:
                        raise e
                    time.sleep(delay_seconds)
        return wrapper_retry
    return decorator_retry

#######################################  GET THE ROTATION NUMBER FOR THE CHANGING ELEMENTS
rotation = None  # Initialize rotation outside the function

l1 = [
    '//*[@id="bottom-area"]/div[3]/div/div[1]/div[2]/div/div/div/div/button[3]',
    '//*[@id="bottom-area"]/div[4]/div/div[1]/div[2]/div/div/div/div/button[3]',
    '//*[@id="bottom-area"]/div[5]/div/div[1]/div[2]/div/div/div/div/button[3]',
    '//*[@id="bottom-area"]/div[6]/div/div[1]/div[2]/div/div/div/div/button[3]']

@retry(max_tries=3, delay_seconds=2)
def getTheRotationNumber(whichElement):
    try:
        #Here we open the bottom TAB to see the Strategy Tester
        driver.find_element(By.XPATH, '//*[@id="footer-chart-panel"]/div[1]/div[1]/div[3]').click()
    except Exception:
        try:
            driver.find_element(By.XPATH, '//*[@id="footer-chart-panel"]/div[1]/div[1]/div[3]/div/span').click()
        except Exception:
            pass

    rotation = next((idx for idx, b in enumerate(whichElement) if driver.find_elements(By.XPATH, b)), None)
    
    while rotation is None:
        continue

    return rotation


rotation = getTheRotationNumber(l1)

driver.find_element(By.XPATH, l1[rotation]).click()

#print(rotation) - to check if it is not None

############################### SPECIFY THE VERSIONS OF THE LISTS TO USE FOR THE CHANGING ELEMENTS
#These are the variants for the strategy selector
l2 = ['//*[@id="bottom-area"]/div[3]/div/div[1]/div[1]/div[1]/button/span[3]',
    '//*[@id="bottom-area"]/div[4]/div/div[1]/div[1]/div[1]/button/span[3]',
    '//*[@id="bottom-area"]/div[5]/div/div[1]/div[1]/div[1]/button/span[3]',
     '//*[@id="bottom-area"]/div[6]/div/div[1]/div[1]/div[1]/button/span[3]']

#list for the strategy selector
l2a = ['//*[@id="bottom-area"]/div[3]/div/div[1]/div[1]/div[1]/button/span[2]',
    '//*[@id="bottom-area"]/div[4]/div/div[1]/div[1]/div[1]/button/span[2]',
    '//*[@id="bottom-area"]/div[5]/div/div[1]/div[1]/div[1]/button/span[2]',
      '//*[@id="bottom-area"]/div[6]/div/div[1]/div[1]/div[1]/button/span[2]']

#list of selectors of the 'No trades' text
l3 = ['//*[@id="bottom-area"]/div[3]/div/div[2]/div[1]/div[2]', 
      '//*[@id="bottom-area"]/div[4]/div/div[2]/div[1]/div[2]',
      '//*[@id="bottom-area"]/div[5]/div/div[2]/div[1]/div[2]',
     '//*[@id="bottom-area"]/div[6]/div/div[2]/div[1]/div[2]']

#list of download csv button selector
l4 = ['//*[@id="bottom-area"]/div[3]/div/div[1]/div[1]/div[1]/div/button[3]',
      '//*[@id="bottom-area"]/div[4]/div/div[1]/div[1]/div[1]/div/button[3]',
      '//*[@id="bottom-area"]/div[5]/div/div[1]/div[1]/div[1]/div/button[3]',
     '//*[@id="bottom-area"]/div[6]/div/div[1]/div[1]/div[1]/div/button[3]']

##############################################################################################################	 
#Define the functions
fncsv = 'StrategyTestingResults v2.csv.gz'
tempdir = pathlib.Path(r'D:\TD\FMF\Strategies\temp')
tdir = r'D:\TD\FMF\Strategies\temp'

#Function to delete the temporary downloaded files
def deleter(d):
    for f in os.listdir(d):
        try:
            os.chmod(f,stat.S_IRUSR|stat.S_IRGRP|stat.S_IROTH|stat.S_IXUSR|stat.S_IRUSR|stab9t.S_IWUSR|stat.S_IWGRP|stat.S_IXGRP)
            os.remove(os.path.join(d, f))
        except:
            pass

#Function to identify the last csv file downloaded
def getlastcsv(l):
    files = glob.glob(l) # * means all if need specific format then *.csv
    p = max(files, key=os.path.getctime)
    return(p)

#Retry function when the Rotation is changed during script execution
def dl(whichList):
    global rotation
    try: 
        driver.find_element(By.XPATH, whichList[rotation]).click() 
    except:
        rotation = getTheRotationNumber()
        driver.find_element(By.XPATH, whichList[rotation]).click()                     
    time.sleep(2)
    try:
        lastcsv = r'D:\TD\FMF\Strategies\temp\*.csv'
        filepath = getlastcsv(lastcsv)
    except ValueError:
        time.sleep(2)
        return None
    return(filepath)

#Preprocessing every downloaded file, returns a df
def processFile(filepath, results, t, timeframe, strategy):
    totalres = pd.DataFrame()
    
    if filepath is None:
        print('Filepath is empty')
        return results
    
    try: 
        data = pd.read_csv(filepath, low_memory=False, parse_dates=['Date/Time'], dayfirst=True, index_col=None)
        data = data.dropna()
        data['Date/Time'] = pd.to_datetime(data['Date/Time'], dayfirst=True)
        data['ticker'] = t
        data['timeframe'] = timeframe
        data['strategy'] = strategy
        results = pd.concat([results, data], ignore_index=True, axis=0)

    except Exception:
        print('There is an error when processing the data')
    return results

#function to send the characters for each PAIR to the driver
def send_keys_with_retry(element, text):
    max_retries = 3
    retries = 0
    while retries < max_retries:
        try:
            element.send_keys(Keys.CONTROL + "a")  # Select all contents
            element.send_keys(Keys.BACKSPACE)  # Clear contents
            element.send_keys(text)
            return
        except StaleElementReferenceException:
            retries += 1
            time.sleep(1)

##################################### DEFINE THE MAIN EXTRACT FUNCTION.
def extract(t, tm, sl):
    global rotation
    results = pd.DataFrame()
    output = pd.DataFrame()
    data = pd.DataFrame()

    #ticker/PAIR block - looping each when executing 
    ticker = driver.find_element(By.XPATH, '//*[@id="header-toolbar-symbol-search"]')
    
    #print(t) - FOR CHECKING
    
	#INPUT THE TICKER INTO TV
    with webdriver.common.action_chains.ActionChains(driver) as action:
        action.move_to_element(ticker).click()
        for i in t:
            send_keys_with_retry(action, i)
        action.send_keys(Keys.RETURN).perform()

    #timeframe block loop
    for ii in tm:
        tt = driver.find_element(By.XPATH, '//*[@id="header-toolbar-intervals"]/button') #click on the timeframe icon
        driver.execute_script("$(arguments[0]).click();", tt)
        time.sleep(1)
        tlr = driver.find_element(By.XPATH, ii) #select the timeframe
        driver.execute_script("$(arguments[0]).click();", tlr)

        timeframe = driver.find_element(By.XPATH, '//*[@id="header-toolbar-intervals"]/button').text #save the timeframe
        time.sleep(3)

        outputs = []  # Initialize an empty list to store output DataFrames
        #strategy block loop
        for s in sl:
            st = driver.find_element(By.XPATH, l2[rotation])
            driver.execute_script("$(arguments[0]).click();", st)
            time.sleep(1)
            stl = driver.find_element(By.XPATH, s) # Select each strategy
            driver.execute_script("$(arguments[0]).click();", stl)
            strategy = driver.find_element(By.XPATH, l2a[rotation]).text
			
			#Skip if no trades
            NoTrades = ''
            try:
                try:
                    NoTrades = driver.find_element(By.CLASS_NAME, 'text-yrIMi47q').text
                except:
                    rotation = getTheRotationNumber()
                    NoTrades = driver.find_element(By.XPATH, l3[rotation]).text              
            except:
                NoTrades = '0'

            if NoTrades == 'No data':
                pass
            
            else:
                os.chdir(tdir)
                lastcsv = r'D:\TD\FMF\Strategies\temp\*.csv'
                deleter(tdir)
                
                try:
                    filepath = dl(l4)

                    while filepath is None or not os.path.isfile(filepath):
                        time.sleep(5)
                        filepath = dl(l4)

                    if os.path.isfile(filepath):
                        output = processFile(filepath, results, t, timeframe, strategy)
                        output.columns = ['Trade #', 'Type', 'exit_signal', 'exit_time', 'exit_price', 'contracts', 'profit', 'profit%', 
                                           'Cum. Profit', 'Cum. Profit %', 'runup', 'runup%', 'drawdown', 'drawdown%', 'ticker', 
                                           'timeframe', 'strategy']
                        cols0 = ['Trade #', 'ticker', 'timeframe', 'strategy', 'exit_signal', 'exit_time', 'exit_price', 'profit%', 'runup%', 'drawdown%']
                        output = output.reindex(columns=cols0)
                        time.sleep(5)
                        outputs.append(output)  # Append output DataFrame to the list
                except Exception:
                    print('Some error')
    time.sleep(2)
    totalOutput = pd.concat(outputs, ignore_index=True)  # Concatenate all output DataFrames at once
    return totalOutput

#Function to restructure the data
def transform(results):
    
    try:
        results = results.assign(entry_price=None)
        results = results.assign(entry_time=None)
        results['entry_signal'] = ''
        
        # Convert the entry_price and exit_price columns to float64
        results['entry_price'] = results['entry_price'].astype('float64')
        results['exit_price'] = results['exit_price'].astype('float64')

        # Convert the entry_time and exit_time columns to datetime64[s]
        results['entry_time'] = results['entry_time'].astype('datetime64[s]')
        results['exit_time'] = results['exit_time'].astype('datetime64[s]')

        results['entry_time'] = pd.to_datetime(results['entry_time'])#, dayfirst=True) # tricky sometimes it works, sometimes it doesn't
        results['exit_time'] = pd.to_datetime(results['exit_time'])#, dayfirst=True) # tricky sometimes it works, sometimes it doesn't
        
        # Select the relevant columns
        cols = ['ticker', 'timeframe', 'strategy', 'entry_price', 'entry_time', 'entry_signal', 'exit_price', 'exit_time', 'exit_signal', 
                'runup%', 'drawdown%']
        results = results.reindex(columns=cols)

        ########################################################################################################################################
        # Create a new DataFrame with the transformed values
        r = results.iloc[::-1]
        r['entry_price'] = r['exit_price']
        r['entry_time'] = r['exit_time']
        r['entry_signal'] = r['exit_signal']
        r['exit_price'] = r['exit_price'].shift(-1)
        r['exit_time'] = r['exit_time'].shift(-1)
        r['exit_signal'] = r['exit_signal'].shift(-1)
        res = r.iloc[::2].copy()

        ########################################################################################################################################
            
        # Use the function to transform the DataFrame

        res = res.drop_duplicates(subset=['ticker', 'timeframe', 'strategy', 'entry_price', 'entry_time', 
                                          'exit_price', 'exit_time'], keep='last', inplace=False)
        
    except Exception as e:
        print(e)
        res = pd.DataFrame()
    
    return res

#Function to save the transform data / append it to the existing file
def savecsv(df, file):
    os.chdir(r'D:\TD\FMF\Strategies')
    if os.path.isfile(file):
        # If file exists, append the DataFrame to the existing file
        with gzip.open(file, 'at', compresslevel=5) as ffile:
            df.to_csv(ffile, header=False, index=False)
    else:
        # If file does not exist, save the DataFrame to a new file
        df.to_csv(file, index=False, compression='gzip')
    return

#List of tickers
ticker = ['BINANCE:BTCUSDT.P', 
          'BINANCE:ETHUSDT.P', 
          'BINANCE:BNBUSDT.P',
         ]

#List of timeframes - SELECT YOURS
tf = ['//*[@id="overlap-manager-root"]/div/span/div[1]/div/div/div/div[2]/div/span[1]/span', # 1 second
       '//*[@id="overlap-manager-root"]/div/span/div[1]/div/div/div/div[3]/div/span[1]/span', # 5 seconds
           '//*[@id="overlap-manager-root"]/div/span/div[1]/div/div/div/div[4]/div/span[1]/span', # 10 seconds
           '//*[@id="overlap-manager-root"]/div/span/div[1]/div/div/div/div[5]/div/span[1]/span', # 15 seconds
           '//*[@id="overlap-manager-root"]/div/span/div[1]/div/div/div/div[6]/div/span[1]/span', # 30 seconds
           '//*[@id="overlap-manager-root"]/div/span/div[1]/div/div/div/div[9]/div/span[1]/span'] # 1 minute

#List of strategies - DEPENDING ON HOW MANY YOU HAVE, THESE ARE JUST THE FIRST 5 IN THE LIST
strategy = ['//*[@id="overlap-manager-root"]/div/span/div[1]/div/div/div[1]/span/span', 
              '//*[@id="overlap-manager-root"]/div/span/div[1]/div/div/div[2]/span/span',
              '//*[@id="overlap-manager-root"]/div/span/div[1]/div/div/div[3]/span/span',
              '//*[@id="overlap-manager-root"]/div/span/div[1]/div/div/div[4]/span/span',
              '//*[@id="overlap-manager-root"]/div/span/div[1]/div/div/div[5]/span/span']

#################RUN THE STRATEGY
#Save the start time
start_time = datetime.now()
start_time_s = start_time.strftime("%d/%m/%Y %H:%M:%S")
print("Start Time =", start_time_s)    

collectedOutput = pd.DataFrame()
with tqdm(total=len(ticker2), file=sys.stdout) as pbar:
    for tticker in ticker:
    
        localOutput = extract(tticker, tf, strategy)
        try:
            
            collectedOutput = collectedOutput.append(localOutput, ignore_index=True)
        
        except:
            collectedOutput = collectedOutput
        pbar.update(1)

finalOutput = transform(collectedOutput)
savecsv(finalOutput, fncsv)


###############################resave the file after cleaning empty rows and duplicates 
#This is to deal with the complications added by saving to gzip instead of simple csv
def cleanSave(file):
    tempOutputFile = 'cleaned_' + file  # New file to save cleaned data
    with gzip.open(file, 'rt') as f:
        os.chdir(r'D:\TD\FMF\Strategies')
        data = pd.read_csv(f, parse_dates=['entry_time', 'exit_time'], index_col=None)
    data = data.sort_values(['entry_time', 'exit_time'], ascending = (True, True))
    data['entry_time'] = pd.to_datetime(data['entry_time'], dayfirst=True)
    data['exit_time'] = pd.to_datetime(data['exit_time'], dayfirst=True)
    data = data.drop_duplicates(subset=['ticker', 'timeframe', 'strategy', 'entry_price', 'entry_time', 
                    'exit_price', 'exit_time'], keep='last', inplace=False)
    data.dropna(axis=0, how='all', inplace=True)
    return(data)

def replaceFile(data):
    # Save DataFrame to temporary uncompressed CSV file
    tempFile = 'temp.csv'
    data.to_csv(tempFile, index=False)
    
    with open(tempFile, 'rb') as f_in, gzip.open(file, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

file = 'StrategyTestingResults v2.csv.gz'
data = cleanSave(file)
replaceFile(data)

#close the bottom tab
try:
    driver.find_element(By.XPATH, '//*[@id="footer-chart-panel"]/div[1]/div[1]/div[3]').click()
except Exception:
    try:
        driver.find_element(By.XPATH, '//*[@id="footer-chart-panel"]/div[1]/div[1]/div[3]/div/span').click()
    except Exception:
        pass

#save the end time
end_time = datetime.now()
end_time_s = end_time.strftime("%d/%m/%Y %H:%M:%S")
print("End Time =", end_time_s)

#Compute duration and print to screen
d = end_time - start_time
duration = d.total_seconds()
duration = duration / 60
print('The job took ' + str(duration) + ' minutes.')
time.sleep(5)

#Close the browser
driver.close()