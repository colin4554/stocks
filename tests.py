# from datetime import datetime
# file = open("records.txt", "a")
# file.write("\n" + str(datetime.now().date()) + ":")
# file.write("\nError " + str(datetime.now().replace(microsecond=0)))
#
#
#
# def ban(num, num2):
#     file.write("\nbruh")
#     return num * num2
#
#
# ban(8,6)
#
#
# file.close()
#
# import pandas as pd
# # df = pd.read_csv('S&P500.csv')
# # print(df['ticker'][df['ticker'] == 'MJH'].sum())
# tickerList = []
# df = pd.read_csv('S&P500.csv')
# mainTickerList = [df['ticker'], df['newsDateLength']]
# #oldDf = oldDf.sort_values(by=['date', 'time'], ascending=[False, False])
#
# for i in range(len(mainTickerList[0])):
#     ticker = mainTickerList[0][i]
#     length = mainTickerList[1][i]
#     print(ticker + ": " + str(length))
#     if i > 20:
#         break


#
# file = open("records.txt", "a")
# file.write("\n" + str(datetime.now().date()) + " ("  + str(datetime.now().time().replace(microsecond=0)) + "):")
#



import pandas as pd
df = pd.read_csv('S&P500.csv')
df = df.sort_values(by=['newsDateLength'], ascending=[False])
tickerList = df['ticker'].tolist()

i = 0
while i < len(tickerList):
    tempTickers = tickerList[i: i+10]
    i += 10
    for ticker in tempTickers:
        print(ticker)
        #print(tempTickers.get_loc(ticker))
        print(tempTickers.index(ticker))
        print(ticker + " (" + str(tempTickers.index(ticker) + 1) + "/" + str(len(tempTickers)) + "): ")# + str(i) + "/100\t" + str(nextTime - startTime) + " elapsed")
