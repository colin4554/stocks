# from datetime import datetime
# file = open("errors.txt", "a")
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

import pandas as pd
df = pd.read_csv('S&P500_Tickers')
print(df)