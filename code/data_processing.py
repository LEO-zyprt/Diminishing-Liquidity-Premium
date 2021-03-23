import pandas as pd
import numpy as np
import datetime

# the author finally take average of the coefficients, we assueme that every stock is equally weighted

file1 = pd.read_parquet(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\original_data\crsp_msf.parquet',engine="pyarrow")[['permno','hexcd','date','prc','vol','ret','shrout','retx']].sort_values(by=['permno','date'])
file2 = pd.read_parquet(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\original_data\ff_four_factor_monthly.parquet',engine="pyarrow")
file3 = pd.read_parquet(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\original_data\liquidity_monthly.parquet',engine="pyarrow")[['permno','turnover','amihud_illiquidity','date']].sort_values(by=['permno','date'])

# transform the date format
file1['date'] = pd.to_datetime(file1['date'])
file2['date'] = pd.to_datetime(file2['date'])
file3['date'] = pd.to_datetime(file3['date'])

'''
# filter data (under 60 trading days and less than 2 dollars), here, we only have monthly data, so we see if the trading month less than 2.5 months
def filter_prc(DF):
    DF['year'] = DF['date'].dt.year
    DF_copy = DF.set_index(['permno','year'])
    DF_slice = DF.groupby(['permno','year']).tail(1)
    DF_slice_copy = DF_slice.set_index(['permno','year'])
    
    for tuple_index in DF_slice_copy.index.tolist():
        print(tuple_index)
        if (DF_slice_copy.loc[tuple_index,'prc'] <= 2):
            DF_pri2 = DF_copy.drop(tuple_index) 
    return DF_pri2

def day_60(DF_pri2):
    df_filter_all = DF_pri2[DF_pri2.groupby(['PERMNO','year']).size()>2]
    return df_filter_all

# do filter process to file1
file1 = day_60(filter_prc(file1))
file1.to_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\file1.csv')
'''

# match the exchange code
file4 = pd.merge(file1, file3, how='inner', left_on=['permno','date'], right_on=['permno','date']).sort_values(by=['permno','date'])
#print(file4.head(50))
file4_NY = file4[file4['hexcd']==1]
file4_AM = file4[file4['hexcd']==2]
file4_NA = file4[file4['hexcd']==3]

# calculate the illiquidity value according to date   three different exchanges have different illiquidity value
file4_NY_slice_illiq = file4_NY.sort_values(by=['permno','date']).rename(columns={'amihud_illiquidity':'Amihud'}).dropna(subset=['Amihud'])[['permno','date','Amihud']]
file4_AM_slice_illiq = file4_AM.sort_values(by=['permno','date']).rename(columns={'amihud_illiquidity':'Amihud'}).dropna(subset=['Amihud'])[['permno','date','Amihud']]
file4_NA_slice_illiq = file4_NA.sort_values(by=['permno','date']).rename(columns={'amihud_illiquidity':'Amihud'}).dropna(subset=['Amihud'])[['permno','date','Amihud']]
# example
file4_NY_slice_illiq.to_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NY_Amihud.csv')


# get B_MKT, B_SMB, B_HML, B_UMD
four_factor = file2[['date','mkt_rf','smb','hml','mom']].rename(columns={'mom':'umd'})
four_factor.to_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\four factor.csv')

# get the std, it is hard to process massive daily data, so we use cross-sectional stock return to get the std of different exchange, defferent year and month
# NY
file4_NY = file4_NY[['permno','date','ret']]
file4_NY_std = file4_NY.groupby(['permno','date'])['ret'].std().reset_index().rename(columns={'ret':'SDRET'})
# AM
file4_AM = file4_AM[['permno','date','ret']]
file4_AM_std = file4_AM.groupby(['permno','date'])['ret'].std().reset_index().rename(columns={'ret':'SDRET'})
# NA
file4_NA = file4_NA[['permno','date','ret']]
file4_NA_std = file4_NA.groupby(['permno','date'])['ret'].std().reset_index().rename(columns={'ret':'SDRET'})
# example
file4_NY_std.to_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NY_SDRET.csv')


# calculate the mcap according to date  three different exchanges have different market cap
file4_NY['LNmcap'] = file4_NY['prc']*file4_NY['shrout']
file4_NY['LnSize'] = np.log(file4_NY.groupby('permno')['LNmcap'].shift(1))
file4_NY_slice_macp_lag1 = file4_NY[['permno','date','LnSize']].dropna(subset=['LnSize'])
file4_AM['LNmcap'] = file4_AM['prc']*file4_AM['shrout']
file4_AM['LnSize'] = np.log(file4_AM.groupby('permno')['LNmcap'].shift(1))
file4_AM_slice_macp_lag1 = file4_AM[['permno','date','LnSize']].dropna(subset=['LnSize'])
file4_NA['LNmcap'] = file4_NA['prc']*file4_NA['shrout']
file4_NA['LnSize'] = np.log(file4_NA.groupby('permno')['LNmcap'].shift(1))
file4_NA_slice_macp_lag1 = file4_NA[['permno','date','LnSize']].dropna(subset=['LnSize'])
# example
file4_AM_slice_macp_lag1.to_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\AM_LNsize.csv')


# calculate the mom4 groupby date, take average of return according to date|exchanges|cumproduct
def cal_mom4(Series):
    mom4 = []
    for i in range(len(Series)):
        if i < 4:
            mom4.append(np.nan)
        else:
            mom = (Series.iloc[i-4:i]).prod()-1
            mom4.append(mom)
    return pd.Series(mom4, index=Series.index)

def cal_mom8(Series):
    mom8 = []
    for i in range(len(Series)):
        if i < 12:
            mom8.append(np.nan)
        else:
            mom = (Series.iloc[i-12:i-4]).prod()-1
            mom8.append(mom)
    return pd.Series(mom8, index=Series.index)

# NY exchange
file4_NY['RET'] = file4_NY['ret'] + 1
file4_NY_slice_mom4 = file4_NY.groupby(['permno'])['RET'].apply(cal_mom4)
file4_NY_mom4 = pd.concat([file4_NY[['permno','date']],file4_NY_slice_mom4],axis=1).rename(columns={'RET':'mom4'})
file4_NY_slice_mom8 = file4_NY.groupby(['permno'])['RET'].apply(cal_mom8)
file4_NY_mom8 = pd.concat([file4_NY[['permno','date']],file4_NY_slice_mom8],axis=1).rename(columns={'RET':'mom8'})
# AM exchange
file4_AM['RET'] = file4_AM['ret'] + 1
file4_AM_slice_mom4 = file4_AM.groupby(['permno'])['RET'].apply(cal_mom4)
file4_AM_mom4 = pd.concat([file4_AM[['permno','date']],file4_AM_slice_mom4],axis=1).rename(columns={'RET':'mom4'})
file4_AM_slice_mom8 = file4_AM.groupby(['permno'])['RET'].apply(cal_mom8)
file4_AM_mom8 = pd.concat([file4_AM[['permno','date']],file4_AM_slice_mom8],axis=1).rename(columns={'RET':'mom8'})
# NA exchange
file4_NA['RET'] = file4_NA['ret'] + 1
file4_NA_slice_mom4 = file4_NA.groupby(['permno'])['RET'].apply(cal_mom4)
file4_NA_mom4 = pd.concat([file4_NA[['permno','date']],file4_NA_slice_mom4],axis=1).rename(columns={'RET':'mom4'})
file4_NA_slice_mom8 = file4_NA.groupby(['permno'])['RET'].apply(cal_mom8)
file4_NA_mom8 = pd.concat([file4_NA[['permno','date']],file4_NA_slice_mom8],axis=1).rename(columns={'RET':'mom8'})
#  example 
file4_NA_mom4.to_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NA_MOM4.csv')
file4_NA_mom8.to_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NA_MOM8.csv')

# calculate DIVYLD defined as cash dividends diveded by the price, so it's a ratio
file4_NY['DIVYLD'] = file4_NY['ret']-file4_NY['retx']
file4_NY_slice_DIVYLD = file4_NY[['permno','date','DIVYLD']]
file4_AM['DIVYLD'] = file4_AM['ret']-file4_AM['retx']
file4_AM_slice_DIVYLD = file4_AM[['permno','date','DIVYLD']]
file4_NA['DIVYLD'] = file4_NA['ret']-file4_NA['retx']
file4_NA_slice_DIVYLD = file4_NA[['permno','date','DIVYLD']]
# example
file4_NY_slice_DIVYLD.to_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NY_DIVYLD.csv')
file4_AM_slice_DIVYLD.to_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\AM_DIVYLD.csv')
file4_NA_slice_DIVYLD.to_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NA_DIVYLD.csv')

# calculate the LnBM and bm_dum
comp_finratios = pd.read_parquet(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\original_data\comp_finratios.parquet',engine="pyarrow")[['gvkey','public_date','bm']]
monthly_gvkey_permno_link = pd.read_parquet(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\original_data\monthly_gvkey_permno_link.parquet',engine="pyarrow")[['permno','gvkey']].drop_duplicates()
bm_combine = pd.merge(comp_finratios, monthly_gvkey_permno_link, how='inner', left_on='gvkey', right_on='gvkey')
bm_combine['bm_dum'] = bm_combine['bm'].apply(lambda x: 1 if x != np.nan else 0)
bm_combine['ln_bm']=np.log(bm_combine['bm']) #for ln bm
bm_combine['ln_bm']= bm_combine['ln_bm'].replace(np.float('-inf'),0) #for bm not existing, adjust its value
bm_combine = bm_combine[['permno','public_date','bm_dum','ln_bm']].rename(columns={'public_date':'date'})
bm_combine['date'] = pd.to_datetime(bm_combine['date'])
#NY
bm_combine_bm_NY = pd.merge(file4_NY, bm_combine, how='inner', left_on=['permno','date'], right_on=['permno','date'])
bm_combine_bm_NY_ln_dum = bm_combine_bm_NY[['permno','date','ln_bm','bm_dum']]
#AM
bm_combine_bm_AM = pd.merge(file4_AM, bm_combine, how='inner', left_on=['permno','date'], right_on=['permno','date'])
bm_combine_bm_AM_ln_dum = bm_combine_bm_AM[['permno','date','ln_bm','bm_dum']]
#NA
bm_combine_bm_NA = pd.merge(file4_NA, bm_combine, how='inner', left_on=['permno','date'], right_on=['permno','date'])
bm_combine_bm_NA_ln_dum = bm_combine_bm_NA[['permno','date','ln_bm','bm_dum']]
# example
bm_combine_bm_NY_ln_dum.to_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NY_LnBM_DmBM.csv')
bm_combine_bm_AM_ln_dum.to_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\AM_LnBM_DmBM.csv')
bm_combine_bm_NA_ln_dum.to_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NA_LnBM_DmBM.csv')


































