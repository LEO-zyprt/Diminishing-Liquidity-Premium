import pandas as pd

df_AM = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\combined_data\df_AM.csv', parse_dates=['date']).drop(['Unnamed: 0'], axis=1)

df_AM_71_81 = df_AM[(df_AM['date']>=pd.to_datetime('1971-1-1'))&(df_AM['date']<=pd.to_datetime('1981-12-31'))]
df_AM_82_92 = df_AM[(df_AM['date']>=pd.to_datetime('1982-1-1'))&(df_AM['date']<=pd.to_datetime('1992-12-31'))]
df_AM_93_03 = df_AM[(df_AM['date']>=pd.to_datetime('1993-1-1'))&(df_AM['date']<=pd.to_datetime('2003-12-31'))]
df_AM_04_14 = df_AM[(df_AM['date']>=pd.to_datetime('2004-1-1'))&(df_AM['date']<=pd.to_datetime('2014-12-31'))]

print(df_AM_71_81['Amihud'].mean())
print(df_AM_82_92['Amihud'].mean())
print(df_AM_93_03['Amihud'].mean())
print(df_AM_04_14['Amihud'].mean())