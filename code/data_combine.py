import pandas as pd


### AM exchange
AM_Amihud = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\AM\AM_Amihud.csv')
four_factor = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\AM\four_factor_date.csv')
AM_SDRET_date = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\AM\AM_SDRET_date.csv')
AM_ret_rf_date = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\AM\AM_ret_rf_date.csv')


# in four_factor and SDRET, there are only date rows(no permno rows)
Amihud_four_factor = pd.merge(AM_Amihud, four_factor, how='left', left_on='date', right_on='date')
Amihud_four_factor_SDRET = pd.merge(Amihud_four_factor, AM_SDRET_date, how='left', left_on='date', right_on='date')
Amihud_four_factor_SDRET_ret_rf = pd.merge(Amihud_four_factor_SDRET, AM_ret_rf_date, how='left', left_on='date', right_on='date')

# inner merge
AM_DIVYLD = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\AM\AM_DIVYLD.csv') 
AM_LnBM_DmBM = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\AM\AM_LnBM_DmBM.csv')
AM_LNsize = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\AM\AM_LNsize.csv')
AM_MOM4 = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\AM\AM_MOM4.csv')
AM_MOM8 = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\AM\AM_MOM8.csv')

Amihud_four_factor_SDRET_ret_rf_DIVYLD = pd.merge(Amihud_four_factor_SDRET_ret_rf, AM_DIVYLD, how='inner', left_on=['permno','date'], right_on=['permno','date'])
Amihud_four_factor_SDRET_ret_rf_DIVYLD_AM_LnBM_DmBM = pd.merge(Amihud_four_factor_SDRET_ret_rf_DIVYLD, AM_LnBM_DmBM, how='inner', left_on=['permno','date'], right_on=['permno','date'])
Amihud_four_factor_SDRET_ret_rf_DIVYLD_AM_LnBM_DmBM_AM_LNsize = pd.merge(Amihud_four_factor_SDRET_ret_rf_DIVYLD_AM_LnBM_DmBM, AM_LNsize, how='inner', left_on=['permno','date'], right_on=['permno','date'])
Amihud_four_factor_SDRET_ret_rf_DIVYLD_AM_LnBM_DmBM_AM_LNsize_MOM4 = pd.merge(Amihud_four_factor_SDRET_ret_rf_DIVYLD_AM_LnBM_DmBM_AM_LNsize, AM_MOM4, how='inner', left_on=['permno','date'], right_on=['permno','date'])
Amihud_four_factor_SDRET_ret_rf_DIVYLD_AM_LnBM_DmBM_AM_LNsize_MOM4_MOM8 = pd.merge(Amihud_four_factor_SDRET_ret_rf_DIVYLD_AM_LnBM_DmBM_AM_LNsize_MOM4, AM_MOM8, how='inner', left_on=['permno','date'], right_on=['permno','date'])
Amihud_four_factor_SDRET_ret_rf_DIVYLD_AM_LnBM_DmBM_AM_LNsize_MOM4_MOM8['Amihud'] = Amihud_four_factor_SDRET_ret_rf_DIVYLD_AM_LnBM_DmBM_AM_LNsize_MOM4_MOM8['Amihud']*100000000
Amihud_four_factor_SDRET_ret_rf_DIVYLD_AM_LnBM_DmBM_AM_LNsize_MOM4_MOM8.fillna(0).to_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\combined_data\df_AM.csv')






###### NA exchange
NA_Amihud = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NA\NA_Amihud.csv')
four_factor = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NA\four_factor_date.csv')
NA_SDRET_date = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NA\NA_SDRET_date.csv')
NA_ret_rf_date = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NA\NA_ret_rf_date.csv')


# in four_factor and SDRET, there are only date rows(no permno rows)
Amihud_four_factor = pd.merge(NA_Amihud, four_factor, how='left', left_on='date', right_on='date')
Amihud_four_factor_SDRET = pd.merge(Amihud_four_factor, NA_SDRET_date, how='left', left_on='date', right_on='date')
Amihud_four_factor_SDRET_ret_rf = pd.merge(Amihud_four_factor_SDRET, NA_ret_rf_date, how='left', left_on='date', right_on='date')

# inner merge
NA_DIVYLD = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NA\NA_DIVYLD.csv') 
NA_LnBM_DmBM = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NA\NA_LnBM_DmBM.csv')
NA_LNsize = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NA\NA_LNsize.csv')
NA_MOM4 = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NA\NA_MOM4.csv')
NA_MOM8 = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NA\NA_MOM8.csv')

Amihud_four_factor_SDRET_ret_rf_DIVYLD = pd.merge(Amihud_four_factor_SDRET_ret_rf, NA_DIVYLD, how='inner', left_on=['permno','date'], right_on=['permno','date'])
Amihud_four_factor_SDRET_ret_rf_DIVYLD_NA_LnBM_DmBM = pd.merge(Amihud_four_factor_SDRET_ret_rf_DIVYLD, NA_LnBM_DmBM, how='inner', left_on=['permno','date'], right_on=['permno','date'])
Amihud_four_factor_SDRET_ret_rf_DIVYLD_NA_LnBM_DmBM_NA_LNsize = pd.merge(Amihud_four_factor_SDRET_ret_rf_DIVYLD_NA_LnBM_DmBM, NA_LNsize, how='inner', left_on=['permno','date'], right_on=['permno','date'])
Amihud_four_factor_SDRET_ret_rf_DIVYLD_NA_LnBM_DmBM_NA_LNsize_MOM4 = pd.merge(Amihud_four_factor_SDRET_ret_rf_DIVYLD_NA_LnBM_DmBM_NA_LNsize, NA_MOM4, how='inner', left_on=['permno','date'], right_on=['permno','date'])
Amihud_four_factor_SDRET_ret_rf_DIVYLD_NA_LnBM_DmBM_NA_LNsize_MOM4_MOM8 = pd.merge(Amihud_four_factor_SDRET_ret_rf_DIVYLD_NA_LnBM_DmBM_NA_LNsize_MOM4, NA_MOM8, how='inner', left_on=['permno','date'], right_on=['permno','date'])
Amihud_four_factor_SDRET_ret_rf_DIVYLD_NA_LnBM_DmBM_NA_LNsize_MOM4_MOM8['Amihud'] = Amihud_four_factor_SDRET_ret_rf_DIVYLD_NA_LnBM_DmBM_NA_LNsize_MOM4_MOM8['Amihud']*100000000
Amihud_four_factor_SDRET_ret_rf_DIVYLD_NA_LnBM_DmBM_NA_LNsize_MOM4_MOM8.fillna(0).to_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\combined_data\df_NA.csv')



#### NY exchange
NY_Amihud = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NY\NY_Amihud.csv')
four_factor = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NY\four_factor_date.csv')
NY_SDRET_date = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NY\NY_SDRET_date.csv')
NY_ret_rf_date = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NY\NY_ret_rf_date.csv')


# in four_factor and SDRET, there are only date rows(no permno rows)
Amihud_four_factor = pd.merge(NY_Amihud, four_factor, how='left', left_on='date', right_on='date')
Amihud_four_factor_SDRET = pd.merge(Amihud_four_factor, NY_SDRET_date, how='left', left_on='date', right_on='date')
Amihud_four_factor_SDRET_ret_rf = pd.merge(Amihud_four_factor_SDRET, NY_ret_rf_date, how='left', left_on='date', right_on='date')

# inner merge
NY_DIVYLD = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NY\NY_DIVYLD.csv') 
NY_LnBM_DmBM = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NY\NY_LnBM_DmBM.csv')
NY_LNsize = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NY\NY_LNsize.csv')
NY_MOM4 = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NY\NY_MOM4.csv')
NY_MOM8 = pd.read_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\variables\NY\NY_MOM8.csv')

Amihud_four_factor_SDRET_ret_rf_DIVYLD = pd.merge(Amihud_four_factor_SDRET_ret_rf, NY_DIVYLD, how='inner', left_on=['permno','date'], right_on=['permno','date'])
Amihud_four_factor_SDRET_ret_rf_DIVYLD_NY_LnBM_DmBM = pd.merge(Amihud_four_factor_SDRET_ret_rf_DIVYLD, NY_LnBM_DmBM, how='inner', left_on=['permno','date'], right_on=['permno','date'])
Amihud_four_factor_SDRET_ret_rf_DIVYLD_NY_LnBM_DmBM_NY_LNsize = pd.merge(Amihud_four_factor_SDRET_ret_rf_DIVYLD_NY_LnBM_DmBM, NY_LNsize, how='inner', left_on=['permno','date'], right_on=['permno','date'])
Amihud_four_factor_SDRET_ret_rf_DIVYLD_NY_LnBM_DmBM_NY_LNsize_MOM4 = pd.merge(Amihud_four_factor_SDRET_ret_rf_DIVYLD_NY_LnBM_DmBM_NY_LNsize, NY_MOM4, how='inner', left_on=['permno','date'], right_on=['permno','date'])
Amihud_four_factor_SDRET_ret_rf_DIVYLD_NY_LnBM_DmBM_NY_LNsize_MOM4_MOM8 = pd.merge(Amihud_four_factor_SDRET_ret_rf_DIVYLD_NY_LnBM_DmBM_NY_LNsize_MOM4, NY_MOM8, how='inner', left_on=['permno','date'], right_on=['permno','date'])
Amihud_four_factor_SDRET_ret_rf_DIVYLD_NY_LnBM_DmBM_NY_LNsize_MOM4_MOM8['Amihud'] = Amihud_four_factor_SDRET_ret_rf_DIVYLD_NY_LnBM_DmBM_NY_LNsize_MOM4_MOM8['Amihud']*100000000
Amihud_four_factor_SDRET_ret_rf_DIVYLD_NY_LnBM_DmBM_NY_LNsize_MOM4_MOM8.fillna(0).to_csv(r'D:\gitrepo\Diminishing_Liquidity_Premium\data\combined_data\df_NY.csv')