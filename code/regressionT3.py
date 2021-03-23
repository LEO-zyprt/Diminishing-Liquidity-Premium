# -*- coding: utf-8 -*-

import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt  
import statsmodels.formula.api as smf

df_AM = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\combined_data\df_AM.csv', parse_dates=['date']).drop(['Unnamed: 0'], axis=1)
df_NA = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\combined_data\df_NA.csv', parse_dates=['date']).drop(['Unnamed: 0'], axis=1)
df_NY = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\combined_data\df_NY.csv', parse_dates=['date']).drop(['Unnamed: 0'], axis=1)


def fmreg(x,formula):
    return smf.ols(formula,data=x).fit().params

def fm_summary(p):
    s = p.describe().T
    s['std_error'] = s['std']/np.sqrt(s['count'])
    s['tstat'] = s['mean']/s['std_error']
    return s[['mean','std_error','tstat']]


AM_liq = (df_AM.groupby('date').apply(fmreg,'ret_rf ~ Amihud'))
print('-----AM------')
print(fm_summary(AM_liq))
AM_all = (df_AM.groupby('date').apply(fmreg,'ret_rf ~ Amihud + LnSize + mkt_rf + smb + hml + umd + SDRET + mom4 + mom8 + DIVYLD + ln_bm + bm_dum'))
print(fm_summary(AM_all))


NA_liq = (df_NA.groupby('date').apply(fmreg,'ret_rf ~ Amihud'))
print('-----NA------')
print(fm_summary(NA_liq))
NA_all = (df_NA.groupby('date').apply(fmreg,'ret_rf ~ Amihud + LnSize + mkt_rf + smb + hml + umd + SDRET + mom4 + mom8 + DIVYLD + ln_bm + bm_dum'))
print(fm_summary(NA_all))

NY_liq = (df_NY.groupby('date').apply(fmreg,'ret_rf ~ Amihud'))
print('-----NY------')
print(fm_summary(NY_liq))
NY_all = (df_NY.groupby('date').apply(fmreg,'ret_rf ~ Amihud + LnSize + mkt_rf + smb + hml + umd + SDRET + mom4 + mom8 + DIVYLD + ln_bm + bm_dum'))
print(fm_summary(NY_all))