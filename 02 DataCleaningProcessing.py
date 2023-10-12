#Import the packages

from datetime import datetime
import os
import gzip
import pandas as pd
import numpy as np

today = datetime.today().strftime('%Y-%m-%d')

os.chdir(r'D:\TD\FMF\Strategies')
filename = 'StrategyTestingResults v2.csv.gz'

resultsOverallStrategy = 'AllResultsStrategyLevel ' + today + '.csv'
resultsStrategyAndDirection = 'AllResults ' + today + '.csv'

###Load the data

with gzip.open(filename, 'rt') as file:
    data = pd.read_csv(file, parse_dates=['entry_time', 'exit_time'], index_col=None, dayfirst=True)
    data['entry_time'] = pd.to_datetime(data['entry_time'])#, dayfirst=True)
    data['exit_time'] = pd.to_datetime(data['exit_time'])#, dayfirst=True)
    data = data.sort_values(['entry_time', 'exit_time'], ascending = (True, True))
    data.dropna(axis=0, how='all', inplace=True)
    data = data.drop_duplicates(subset=['ticker', 'timeframe', 'strategy', 'entry_price', 'entry_time', 
                'exit_price', 'exit_time'], keep='last', inplace=False)#, ignore_index=True)
    #data['profit%'] = np.where(data['entry_signal'] == 'BUY', (((((10000 * 0.992) / data['entry_price']) * data['exit_price']) - (10000 * 0.996)) / 100), (((10000 * 0.996) - (((10000 * 0.992) / data['entry_price']) * data['exit_price'])) / 100)) #I am using 0.992 since the second trasaction uses the amount after the first commission and a second .004 commission is added.
	data['profit%'] = np.where(data['entry_signal'] == 'BUY', ((data['exit_price'] - data['entry_price']) * 10000 - 8) / 10000, ((data['entry_price'] - data['exit_price']) * 10000 - 8) / 10000) #made it simpler here: Binance is charging me 8 USD for each 10k transaction

#Computing the cummulative profit (includes the trade direction: BUY or SELL). This basically adds up the profit after each trade.
data['CumulativeProfit%'] = data.groupby(['ticker', 'strategy', 'timeframe', 'entry_signal'])['profit%'].cumsum() / 100
data['CumulativeProfit%'] = data['CumulativeProfit%'].apply(lambda x: f'{x:.2%}')

#Computing the cumulative profit by strategy without the direction of the trade.
data['CumulativeProfitStrategy%'] = data.groupby(['ticker', 'strategy', 'timeframe'])['profit%'].cumsum() / 100
data['CumulativeProfitStrategy%'] = data['CumulativeProfitStrategy%'].apply(lambda x: f'{x:.2%}')

#Computing how long the trade was in the market
data['TimeInMarket'] = (data['exit_time'] - data['entry_time']).dt.total_seconds() / 3600.0

#Getting the day of the week when the trade was placed
data['WeekDayEntry'] = data['entry_time'].dt.dayofweek

#Getting the hour when the trade was placed
data['HourOfEntry'] = data['entry_time'].dt.hour

#Cleaning the data so that the COMMENTS ARE ALWAYS BUY/SELL, CLOSE BUY/CLOSE SELL
data['entry_signal'] = data['entry_signal'].apply(lambda x: x.replace('Entry Long', 'BUY'))
data['entry_signal'] = data['entry_signal'].apply(lambda x: x.replace('Entry Short', 'SELL'))
data['exit_signal'] = data['exit_signal'].apply(lambda x: x.replace('Exit Long', 'CLOSE BUY'))
data['exit_signal'] = data['exit_signal'].apply(lambda x: x.replace('Exit Short', 'CLOSE SELL'))

#####RESAVE DATA TO CSV
os.chdir(r'D:\TD\FMF\Strategies')
file = 'StrategyTestingResults v2 for Analysis.csv.gz'
data.to_csv(file, index=False, compression='gzip')

#############################################OverallStrategy Assessment#####################################
######Summing up the profit for each tuple ticker, strategy and timeframe in order to compare them. 
totalprofit2 = pd.DataFrame()
totalprofit2 = data.groupby(['ticker', 'strategy', 'timeframe'])['profit%'].sum().reset_index()
totalprofit2.columns = ['ticker', 'strategy', 'timeframe', 'TotalProfit%']

#Getting the average profit per trade for each tuple of ticker, strategy, timeframe
avgprofit2 = data.groupby(['ticker', 'strategy', 'timeframe'])['profit%'].mean().reset_index()#.apply(lambda x: f'{x:.2f}').reset_index()
avgprofit2.columns = ['ticker', 'strategy', 'timeframe', 'AvgProfitPerTrade%']
prof12 = avgprofit2.merge(totalprofit2, how = 'outer', on = ['ticker', 'strategy', 'timeframe'])

#Computing the lowest value of the cumulative profit (after each trade)
mincumprofitp2 = data.groupby(['ticker', 'strategy', 'timeframe'])['CumulativeProfitStrategy%'].min().reset_index()
mincumprofitp2.columns = ['ticker', 'strategy', 'timeframe', 'MinCumulative%']
prof22 = mincumprofitp2.merge(prof12, how = 'outer', on = ['ticker', 'strategy', 'timeframe'])

#Computing the maximum value of the cumulative profit (after each trade)
maxcumprofitp2 = data.groupby(['ticker', 'strategy', 'timeframe'])['CumulativeProfitStrategy%'].max().reset_index()
maxcumprofitp2.columns = ['ticker', 'strategy', 'timeframe', 'MaxCumulative%']
profit2 = maxcumprofitp2.merge(prof22, how = 'outer', on = ['ticker', 'strategy', 'timeframe'])

################################################################################################################################
#Determine the interval in which the Strategy has been active for the ticker and timeframe and get the result in days
MinDate2 = data.groupby(['ticker', 'strategy', 'timeframe'])['entry_time'].min().reset_index()
MaxDate2 = data.groupby(['ticker', 'strategy', 'timeframe'])['exit_time'].max().reset_index()
m2 = MaxDate2.merge(profit2, how = 'outer', on = ['ticker', 'strategy', 'timeframe'])
m2 = MinDate2.merge(m2, how = 'outer', on = ['ticker', 'strategy', 'timeframe'])
m2['TimeInDays'] = (m2.exit_time - m2.entry_time).dt.total_seconds() / 86400

################################################################################################################################
#Compute the number of trades the strategy has placed for the ticker and timeframe within the interval
Trades2 = pd.DataFrame()
Trades2 = data.groupby(['ticker', 'strategy', 'timeframe']).size().reset_index()
Trades2.columns = ['ticker', 'strategy', 'timeframe', 'NoOfTrades']
m2 = Trades2.merge(m2, how = 'outer', on = ['ticker', 'strategy', 'timeframe'])

################################################################################################################################
#Get the average drawdown of the strategy for the tuple/timeframe
drawdown2  = pd.DataFrame()
drawdown2 = data.groupby(['ticker', 'strategy', 'timeframe'])['drawdown%'].mean().apply(lambda x: f'{x:.1f}').reset_index()
m2 = drawdown2.merge(m2, how = 'outer', on = ['ticker', 'strategy', 'timeframe'])

################################################################################################################################
#Get the average runup of the strategy for the tuple/timeframe
runup2 = pd.DataFrame()
runup2 = data.groupby(['ticker', 'strategy', 'timeframe'])['runup%'].mean().apply(lambda x: f'{x:.1f}').reset_index()
runup2.columns = ['ticker', 'strategy', 'timeframe', 'runup%']
m2 = runup2.merge(m2, how = 'outer', on = ['ticker', 'strategy', 'timeframe'])

################################################################################################################################
#Get the average time in market (in hours) of the strategy for the ticker/timeframe
avgtimeinmkt2 = pd.DataFrame()
avgtimeinmkt2 = data.groupby(['ticker', 'strategy', 'timeframe'])['TimeInMarket'].mean().reset_index()
avgtimeinmkt2.columns = ['ticker', 'strategy', 'timeframe', 'AvgTimeInMarketHours']
avgtimeinmkt2['AvgTimeInMarketHours'] = (avgtimeinmkt2['AvgTimeInMarketHours'] / 60)
m2 = avgtimeinmkt2.merge(m2, how = 'outer', on = ['ticker', 'strategy', 'timeframe'])
m2


################################################################################################################################
#Get the standard deviation of the time in market (in hours) of the strategy for the tuple/timeframe
stdtimeinmkt2 = pd.DataFrame()
stdtimeinmkt2 = data.groupby(['ticker', 'strategy', 'timeframe'])['TimeInMarket'].std().reset_index()
stdtimeinmkt2.columns = ['ticker', 'strategy', 'timeframe', 'StdTimeInMarket']
stdtimeinmkt2['StdTimeInMarket'] = (stdtimeinmkt2['StdTimeInMarket'] / 60)
m2 = stdtimeinmkt2.merge(m2, how = 'outer', on = ['ticker', 'strategy', 'timeframe'])

################################################################################################################################
#Get the min and max price within the interval (for volatility)
minentry2 = data.groupby(['ticker', 'strategy', 'timeframe'])['entry_price'].min().reset_index()
minentry2.columns = ['ticker', 'strategy', 'timeframe', 'MinPriceEntry']
m2 = minentry2.merge(m2, how = 'outer', on = ['ticker', 'strategy', 'timeframe'])
maxentry2 = data.groupby(['ticker', 'strategy', 'timeframe'])['entry_price'].max().reset_index()
maxentry2.columns = ['ticker', 'strategy', 'timeframe', 'MaxPriceEntry']
m2 = maxentry2.merge(m2, how = 'outer', on = ['ticker', 'strategy', 'timeframe'])
minexit2 = data.groupby(['ticker', 'strategy', 'timeframe'])['entry_price'].min().reset_index()
minexit2.columns = ['ticker', 'strategy', 'timeframe', 'MinPriceExit']
m2 = minexit2.merge(m2, how = 'outer', on = ['ticker', 'strategy', 'timeframe'])
maxexit2 = data.groupby(['ticker', 'strategy', 'timeframe'])['entry_price'].max().reset_index()
maxexit2.columns = ['ticker', 'strategy', 'timeframe', 'MaxPriceExit']
m2 = maxexit2.merge(m2, how = 'outer', on = ['ticker', 'strategy', 'timeframe'])
m2['MinPrice'] = np.where(m2['MinPriceEntry'] <= m2['MinPriceExit'], m2['MinPriceEntry'], m2['MinPriceExit'])
m2['MaxPrice'] = np.where(m2['MaxPriceEntry'] >= m2['MaxPriceExit'], m2['MaxPriceEntry'], m2['MaxPriceExit'])
m2['var'] = np.where(m2['MinPrice'] <= m2['MaxPrice'], ((m2['MaxPrice'] - m2['MinPrice']) / m2['MinPrice']), 1000)
m2['var'] = m2['var'].apply(lambda x: f'{x:.1f}%')

################################################################################################################################
#Compute the number of trades with positive and negative outcome 
negativeonly2 = pd.DataFrame()
positiveonly2 = pd.DataFrame()
negativeonly2 = data[data['profit%'] <= 0]
positiveonly2 = data[data['profit%'] > 0]

################################################################################################################################
#Compute percentage of negative trades for benchmarking
NoNegative2 = pd.DataFrame()
NoNegative2 = negativeonly2.groupby(['ticker', 'strategy', 'timeframe']).size().reset_index()
NoNegative2.columns = ['ticker', 'strategy', 'timeframe', 'NoOfNegative']
ProfitNegative2 = negativeonly2.groupby(['ticker', 'strategy', 'timeframe'])['profit%'].sum().reset_index()
ProfitNegative2.columns = ['ticker', 'strategy', 'timeframe', 'ProfitNegative%']
OnNegative2 = NoNegative2.merge(ProfitNegative2, how = 'outer', on = ['ticker', 'strategy', 'timeframe'])
m2 = OnNegative2.merge(m2, how = 'outer', on = ['ticker', 'strategy', 'timeframe'])

################################################################################################################################
#Compute percentage of positive trades for benchmarking
NoPositive2 = pd.DataFrame()
NoPositive2 = positiveonly2.groupby(['ticker', 'strategy', 'timeframe']).size().reset_index()
NoPositive2.columns = ['ticker', 'strategy', 'timeframe', 'NoOfPositive']
ProfitPositive2 = positiveonly2.groupby(['ticker', 'strategy', 'timeframe'])['profit%'].sum().reset_index()
ProfitPositive2.columns = ['ticker', 'strategy', 'timeframe', 'ProfitPositive%']
OnPositive2 = NoPositive2.merge(ProfitPositive2, how = 'outer', on = ['ticker', 'strategy', 'timeframe'])
m2 = OnPositive2.merge(m2, how = 'outer', on = ['ticker', 'strategy', 'timeframe'])
m2['NoOfPositive'] = m2['NoOfPositive'].fillna(0)
m2['ProfitPositive%'] = m2['ProfitPositive%'].fillna(0)
m2['NoOfNegative'] = m2['NoOfNegative'].fillna(0)
m2['ProfitNegative%'] = m2['ProfitNegative%'].fillna(0)

################################################################################################################################

#Compute ratio [-100 to 100] as division of profit made from positive trades divided by profit made from negative trades. If not negative trades, the ratio is 100
#Similar to Profit Factor in TradingView Statistics
m2['ratio'] = np.where(m2['ProfitNegative%'] != 0.00, m2['ProfitPositive%'] / (m2['ProfitNegative%'] * -1), 100)
m2['ratio'] = np.where(m2['ProfitPositive%'] == 0, -100, m2['ratio'])
m2['ratio'] = m2['ratio'].apply(lambda x: f'{x:.1f}')

################################################################################################################################
#Ratio of negative trades out of total number of trades
m2['RatioNegative'] = np.where(m2['NoOfTrades'] > 0, m2['NoOfNegative'] / m2['NoOfTrades'], 0)
m2['RatioNegative'] = m2['RatioNegative'].apply(lambda x: f'{x:.0%}')

################################################################################################################################
#Adjustment in case of using leverage: settings are x100 for FX and x10 for crypto
m2['AdjustedProfit%'] = np.where(m2['ticker'].str.contains('FX:'), m2['TotalProfit%'] * 100, m2['TotalProfit%'])
m2['AdjustedProfit%'] = np.where(m2['ticker'].str.contains('BINANCE:') | m2['ticker'].str.contains('BITMEX:'), m2['TotalProfit%'] * 10, m2['AdjustedProfit%'])

#Compute the average adjusted profit% per day
m2['APPD'] = m2.apply(lambda row: row['AdjustedProfit%'] / row.TimeInDays, axis = 1)
m2['TradesPerDay'] = np.where(m2['TimeInDays'] >= 1, m2['NoOfTrades'] / m2['TimeInDays'], 0)
m2['TradesPerDay'] = m2['TradesPerDay'].apply(lambda x: f'{x:.1f}')

################################################################################################################################
#Reorder the columns
cols2 = ['ticker', 'strategy', 'timeframe', 'entry_time', 'exit_time', 'TimeInDays', 
        'AvgTimeInMarketHours', 'StdTimeInMarket', 'NoOfTrades', 'RatioNegative', 'TotalProfit%', 'AvgProfitPerTrade%', 
        'ProfitPositive%', 'ProfitNegative%', 'ratio', 'MinCumulative%', 'MaxCumulative%', 'runup%', 'drawdown%', 
        'MinPrice', 'MaxPrice', 'var', 'TradesPerDay', 'AdjustedProfit%', 'APPD']

################################################################################################################################
#slide the df
df2 = m2[cols2]

################################################################################################################################
#sort by ratio
df2.sort_values(by=['ratio'], ascending=[False])

################################################################################################################################
#Save the analysis daily
os.chdir(r'D:\TD\FMF\Strategies\Results')
df2.to_csv('AllResults_OverallStrategy ' + today + '.csv', sep=',', index=False, header=True)
