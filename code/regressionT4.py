import pandas as pd
import numpy as np
import statsmodels.formula.api as smf

df_AM = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\combined_data\df_AM.csv', parse_dates=['date']).drop(['Unnamed: 0'], axis=1)
df_NA = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\combined_data\df_NA.csv', parse_dates=['date']).drop(['Unnamed: 0'], axis=1)
df_NY = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\combined_data\df_NY.csv', parse_dates=['date']).drop(['Unnamed: 0'], axis=1)
df_AM['date'] = pd.to_datetime(df_AM['date'])
df_NA['date'] = pd.to_datetime(df_NA['date'])
df_NY['date'] = pd.to_datetime(df_NY['date'])

def fmreg(x,formula):
    return smf.ols(formula,data=x).fit().params

def fm_summary(p):
    s = p.describe().T
    s['std_error'] = s['std']/np.sqrt(s['count'])
    s['tstat'] = s['mean']/s['std_error']
    return s[['mean','std_error','tstat']]

pint('--------------------------------------')
#AM
df_AM_71_81 = df_AM[(df_AM['date']>=pd.to_datetime('1971-1-1'))&(df_AM['date']<=pd.to_datetime('1981-12-31'))]
df_AM_82_92 = df_AM[(df_AM['date']>=pd.to_datetime('1982-1-1'))&(df_AM['date']<=pd.to_datetime('1992-12-31'))]
df_AM_93_03 = df_AM[(df_AM['date']>=pd.to_datetime('1993-1-1'))&(df_AM['date']<=pd.to_datetime('2003-12-31'))]
df_AM_04_14 = df_AM[(df_AM['date']>=pd.to_datetime('2004-1-1'))&(df_AM['date']<=pd.to_datetime('2014-12-31'))]
#Univariate
#AM 71-81
AM_71_81 = (df_AM_71_81.groupby('date').apply(fmreg,'ret_rf ~ Amihud'))
print(fm_summary(AM_71_81))
#AM 82-92
AM_82_92 = (df_AM_82_92.groupby('date').apply(fmreg,'ret_rf ~ Amihud'))
print(fm_summary(AM_82_92))
#AM 93-03
AM_93_03 = (df_AM_93_03.groupby('date').apply(fmreg,'ret_rf ~ Amihud'))
print(fm_summary(AM_93_03))
#AM 04-14
AM_04_14 = (df_AM_04_14.groupby('date').apply(fmreg,'ret_rf ~ Amihud'))
print(fm_summary(AM_04_14))
#Multivariate
#AM 71-81
AM_71_81 = (df_AM_71_81.groupby('date').apply(fmreg,'ret_rf ~ Amihud + LnSize + mkt_rf + smb + hml + umd + SDRET + mom4 + mom8 + DIVYLD + ln_bm + bm_dum'))
print(fm_summary(AM_71_81))
#AM 82-92
AM_82_92 = (df_AM_82_92.groupby('date').apply(fmreg,'ret_rf ~ Amihud + LnSize + mkt_rf + smb + hml + umd + SDRET + mom4 + mom8 + DIVYLD + ln_bm + bm_dum'))
print(fm_summary(AM_82_92))
#AM 93-03
AM_93_03 = (df_AM_93_03.groupby('date').apply(fmreg,'ret_rf ~ Amihud + LnSize + mkt_rf + smb + hml + umd + SDRET + mom4 + mom8 + DIVYLD + ln_bm + bm_dum'))
print(fm_summary(AM_93_03))
#AM 93-03
AM_04_14 = (df_AM_04_14.groupby('date').apply(fmreg,'ret_rf ~ Amihud + LnSize + mkt_rf + smb + hml + umd + SDRET + mom4 + mom8 + DIVYLD + ln_bm + bm_dum'))
print(fm_summary(AM_04_14))



pint('--------------------------------------')
# NA
df_NA_82_92 = df_NA[(df_NA['date']>=pd.to_datetime('1982-1-1'))&(df_NA['date']<=pd.to_datetime('1992-12-31'))]
df_NA_93_03 = df_NA[(df_NA['date']>=pd.to_datetime('1993-1-1'))&(df_NA['date']<=pd.to_datetime('2003-12-31'))]
df_NA_04_14 = df_NA[(df_NA['date']>=pd.to_datetime('2004-1-1'))&(df_NA['date']<=pd.to_datetime('2014-12-31'))]
#Univariate
#NA 82-92
NA_82_92 = (df_NA_82_92.groupby('date').apply(fmreg,'ret_rf ~ Amihud'))
print(fm_summary(NA_82_92))
#NA 93-03
NA_93_03 = (df_NA_93_03.groupby('date').apply(fmreg,'ret_rf ~ Amihud'))
print(fm_summary(NA_93_03))
#NA 04-14
NA_04_14 = (df_NA_04_14.groupby('date').apply(fmreg,'ret_rf ~ Amihud'))
print(fm_summary(NA_04_14))
#Multivariate
#NA 82-92
NA_82_92 = (df_NA_82_92.groupby('date').apply(fmreg,'ret_rf ~ Amihud + LnSize + mkt_rf + smb + hml + umd + SDRET + mom4 + mom8 + DIVYLD + ln_bm + bm_dum'))
print(fm_summary(NA_82_92))
#NA 93-03
NA_93_03 = (df_NA_93_03.groupby('date').apply(fmreg,'ret_rf ~ Amihud + LnSize + mkt_rf + smb + hml + umd + SDRET + mom4 + mom8 + DIVYLD + ln_bm + bm_dum'))
print(fm_summary(NA_93_03))
#NA 94-04
NA_04_14 = (df_NA_04_14.groupby('date').apply(fmreg,'ret_rf ~ Amihud + LnSize + mkt_rf + smb + hml + umd + SDRET + mom4 + mom8 + DIVYLD + ln_bm + bm_dum'))
print(fm_summary(NA_04_14))


pint('--------------------------------------')
#NY
df_NY_71_81 = df_NY[(df_NY['date']>=pd.to_datetime('1971-1-1'))&(df_NY['date']<=pd.to_datetime('1981-12-31'))]
df_NY_82_92 = df_NY[(df_NY['date']>=pd.to_datetime('1982-1-1'))&(df_NY['date']<=pd.to_datetime('1992-12-31'))]
df_NY_93_03 = df_NY[(df_NY['date']>=pd.to_datetime('1993-1-1'))&(df_NY['date']<=pd.to_datetime('2003-12-31'))]
df_NY_04_14 = df_NY[(df_NY['date']>=pd.to_datetime('2004-1-1'))&(df_NY['date']<=pd.to_datetime('2014-12-31'))]
#Univariate
#NY 71-81
NY_71_81 = (df_NY_71_81.groupby('date').apply(fmreg,'ret_rf ~ Amihud'))
print(fm_summary(NY_71_81))
#NY 82-92
NY_82_92 = (df_NY_82_92.groupby('date').apply(fmreg,'ret_rf ~ Amihud'))
print(fm_summary(NY_82_92))
#NY 93-03
NY_93_03 = (df_NY_93_03.groupby('date').apply(fmreg,'ret_rf ~ Amihud'))
print(fm_summary(NY_93_03))
#NY 04-14
NY_04_14 = (df_NY_04_14.groupby('date').apply(fmreg,'ret_rf ~ Amihud'))
print(fm_summary(NY_04_14))
#Multivariate
#NY 71-81
NY_71_81 = (df_NY_71_81.groupby('date').apply(fmreg,'ret_rf ~ Amihud + LnSize + mkt_rf + smb + hml + umd + SDRET + mom4 + mom8 + DIVYLD + ln_bm + bm_dum'))
print(fm_summary(NY_71_81))
#NY 82-92
NY_82_92 = (df_NY_82_92.groupby('date').apply(fmreg,'ret_rf ~ Amihud + LnSize + mkt_rf + smb + hml + umd + SDRET + mom4 + mom8 + DIVYLD + ln_bm + bm_dum'))
print(fm_summary(NY_82_92))
#NY 93-03
NY_93_03 = (df_NY_93_03.groupby('date').apply(fmreg,'ret_rf ~ Amihud + LnSize + mkt_rf + smb + hml + umd + SDRET + mom4 + mom8 + DIVYLD + ln_bm + bm_dum'))
print(fm_summary(NY_93_03))
#NY 93-03
NY_04_14 = (df_NY_04_14.groupby('date').apply(fmreg,'ret_rf ~ Amihud + LnSize + mkt_rf + smb + hml + umd + SDRET + mom4 + mom8 + DIVYLD + ln_bm + bm_dum'))
print(fm_summary(NY_04_14))

