# -*- coding: utf-8 -*-
"""
Created on Tue Jan  4 03:37:44 2022
@author: Daria Ulybina
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re, os
#import pdb
import glob
#import pandas.api.types as ptypes

pd.set_option('display.max_columns', 12)
pd.set_option("display.expand_frame_repr", False)
#pd.set_option('display.width', 1000)
#pd.set_option("display.precision", 1)

def define_datetime():
    """Create timestamp extension FORMAT: _DDMMMYYYY"""
    from datetime import datetime
    dateTimeObj = datetime.now()
    timestampStr = dateTimeObj.strftime("%d%b%Y")
    assert isinstance(timestampStr, str)
    datetime_ext = "_"+timestampStr
    return datetime_ext

def define_traffic(nameoffile):
    """Check and make sure that the traffic type in the name of the file is GLOBAL"""
    if re.search("GLOBAL", nameoffile, flags = re.I) and not re.search("reg", nameoffile):
        traffic = "(global)"
    elif re.search("reg", nameoffile) and not re.search("glob", nameoffile):
        traffic = "(regional)"
    else:
        traffic = "(all traffic)"
    return traffic

def get_inputs(nameoffile):
    #df=pd.read_pickle(nameoffile)
    df=pd.read_csv(nameoffile)
    return df

def clean_inputs(df):
    """
    Step 1. Rename the columns to be more descriptives _CUR --> Destinations and _PREV --> Origins 
    Step 2. Drop nulls
    Step 3. Double check (drop) that there are no records with origin/destination at the same port
    Step 4. Filter out records with invalid capacity
    Return: clean dataframe and a dictionary of column transformations from step 1
    """
    filter1=['PORT_ID_CUR', 'PORT_ID_PREV', 'Dep2_YearMonth','SHIP_ID count', 'TEU sum', 'diff_hrs median']
    filter2 = ['PORT_ID Destination', 'PORT_ID Origin','Departure_YearMonth', 'SHIP_ID count', 'TEUs sum','time difference (median hrs)']
    f = dict(zip(filter2, filter1))
    f_reversed = dict(map(reversed, f.items()))
    df = df.rename(columns=f)
    df=df.filter(filter1)
    df.dropna(inplace=True)
    df=df.loc[df['PORT_ID_CUR']!=df['PORT_ID_PREV']]
    df=df.loc[df['TEU sum']>0]
    return df, f_reversed
    
def shangai_long_beach_show(df):
    """Show activity for example of Shanghai - Long Beach """
    filter1=(df['PORT_ID_CUR']==2727)
    filter2=(df['PORT_ID_PREV']==1253)
    dfex=df.loc[filter1&filter2]
    dfex['diff_hrs median'].plot.hist()
    plt.show()
    dfex.groupby("Dep2_YearMonth")['diff_hrs median'].sum().plot()
    dfex.plot.line(x='Dep2_YearMonth',y='diff_hrs median')
    plt.show()
    lst=dfex['diff_hrs median'].values
    (a,b,c)=(np.median(lst), np.quantile(lst,.25),np.quantile(lst,.75))
    print("Median: ",a,"25th percentile: ", b, "75th percentile: ", c, "3*25th percentile - 2*75th percentile: ", 3*a-2*b)
    return

def activity_by_lane(df, aggtype, verbose):
    """Create the statistics of activity by lane"""
    df_lane=df.groupby(['PORT_ID_CUR', 'PORT_ID_PREV']).agg({'diff_hrs median':'median','TEU sum':'median','SHIP_ID count':aggtype})
    df_lane.columns = ["_".join(x) for x in df_lane.columns]
    df_lane.columns =['diff_hrs median','TEU sum',f'SHIP_ID {aggtype}']
    df_lane = df_lane.reset_index()
    #df0.sort_values(by='TEU sum',axis=1, ascending=False, inplace=True)
    if verbose:
        df_lane[f'SHIP_ID {aggtype}'].plot.hist()
        plt.show()
        print(df_lane.head())
    return df_lane

def reference_lead_time(dataframe, df_lane):
    """ 3 * median - 2* 1st quartile 
    Augment the original dataframe with a reference lead time by pair of OD ports
    Filter bilateral lane dataframe  to keep count of period and join with df1"""
    df = dataframe.copy()
    df0 = df_lane.copy()
   
    k=2
    df['diff_trs median'] = pd.to_numeric(df['diff_hrs median'])
    df1=df.groupby(['PORT_ID_CUR', 'PORT_ID_PREV'])['diff_hrs median'].agg([('diff_hrs median', lambda x: (k+1)*np.quantile(x,.5)-k*np.quantile(x,.1))]).reset_index()
    
    df0=df0.filter(['PORT_ID_CUR', 'PORT_ID_PREV','SHIP_ID count'])
    df1=df1.set_index(['PORT_ID_CUR', 'PORT_ID_PREV']).join(df0.set_index(['PORT_ID_CUR', 'PORT_ID_PREV']), on=['PORT_ID_CUR', 'PORT_ID_PREV'],how='left').reset_index()
    df1.rename(columns={'diff_hrs median':'reference_lead_time','SHIP_ID count':'period count' },inplace=True)
    return df1

def join_ref(dataframe, dataframe1):
    #Do a join of the reference dataframe df1 with df and  Keep the lanes with minimal activity (above 10)
    frame=dataframe.set_index(['PORT_ID_CUR', 'PORT_ID_PREV']).join(dataframe1.set_index(['PORT_ID_CUR', 'PORT_ID_PREV']), on=['PORT_ID_CUR', 'PORT_ID_PREV'],how='left')
    frame=frame.reset_index()
    frame=frame.loc[frame['period count']>10]
    return frame

def stalled_capacity_pairs(dataframe):
    df = dataframe.copy()
    # Do the estimate of stalled capacity by pair
    df['delay']=np.heaviside(df['diff_hrs median']-df['reference_lead_time'],0.5)*(df['diff_hrs median']-df['reference_lead_time'])
    df['delayed_ship']=df['SHIP_ID count']*df['delay']/730
    df['delayed_capacity']=df['TEU sum']*df['delay']/730
    #df['delayed_ship sum']=df['SHIP_ID count']*df['delay']/730
    #df['delayed_capacity median']=df['TEU median']*df['delay']/730
    # Aggregate stalled capacity by port of arrival and month
    df2=df.groupby(['Dep2_YearMonth','PORT_ID_CUR']).agg({'delayed_ship':'sum','delayed_capacity':'sum','TEU sum':'sum'}).reset_index()
    df2['port_delay']=730*df2['delayed_capacity']/df2['TEU sum']
    df2.rename(columns={'PORT_ID_CUR':'PORT_ID'},inplace=True)#rename column with port id to match port file
    return df2

def ports_metadata(ports_file):
    #retrieve port data
    df3=pd.read_csv(ports_file,usecols=['PORT_ID', 'PORT_NAME', 'Latitude','Longitude', 'un_code', 'country_3', 'Economy name','Maritime_Region']) 
    # dfports['PORT_ID'] = dfports['PORT_ID'].astype('Int64')
    #assert df3['PORT_ID'].dtypes == df2['PORT_ID'].dtypes
    #df3[df3.isna().any(axis=1)]
    df3.at[52, 'country_3'] = 'IDN'
    df3.at[249, 'country_3'] = 'JPN'
    df3.at[321, 'country_3'] = 'BRA'
    df3.at[1756, 'country_3'] = 'GLP'
    df3.at[1757, 'country_3'] = 'AIA'

    df3.at[52, 'Maritime_Region'] = 'South East Asia'
    df3.at[249, 'Maritime_Region'] = 'North Asia'
    df3.at[321, 'Maritime_Region'] = 'South America East Coast'
    df3.at[1756, 'Maritime_Region'] = 'Carribean Sea & Central America'
    df3.at[1757, 'Maritime_Region'] = 'Carribean Sea & Central America'
    dfports = df3.copy()
    
    prev = dfports.copy()
    cols = [x+"_PREV" for x in prev.columns]
    prev.columns = cols
    cur = dfports.copy()
    cols = [x+"_CUR" for x in cur.columns]
    cur.columns = cols
    return  prev, cur

def stalled_capacity(dataframe):
    #estimation of stalled capacity by maritime region and export to csv
    prev, cur = ports_metadata('Y:\\mt\\ports.csv')
    dataframe = dataframe.copy() 
    #cols_to_check = [cur['PORT_ID_CUR'], prev['PORT_ID_PREV'], dataframe['PORT_ID']] 
    #assert all(ptypes.is_numeric_dtype(dataframe[col]) for col in cols_to_check)

    dataframe1 = pd.merge(dataframe, cur,how='left',left_on='PORT_ID',right_on='PORT_ID_CUR')
    #dataframe2 = pd.merge(dataframe1,prev,how='left',left_on='PORT_ID_PREV',right_on='PORT_ID_PREV')
    #df=df.set_index(['PORT_ID']).join(dfports.set_index(['PORT_ID']), on=['PORT_ID'],how='left').reset_index() # dataframe for further processing
    dfagg=dataframe1.groupby(['Dep2_YearMonth','Maritime_Region_CUR']).agg({'delayed_ship':'sum','delayed_capacity':'sum'}).reset_index()
    #dfbilat=dataframe.groupby(['Dep2_YearMonth','Maritime_Region_CUR','Maritime_Region_PREV']).agg({'delayed_ship':'sum','delayed_capacity':'sum'}).reset_index()
    return dfagg, dataframe1

def save_files(dfagg, dataframe, datetime_ext, traffic,f_reversed):
    dfagg.to_csv('stressdata {}_{}.csv'.format(datetime_ext, traffic))
    #pdb.set_trace()
    tosave=dataframe.copy()
    tosave = dataframe.filter(['PORT_NAME_CUR', 'Dep2_YearMonth','country_3_CUR','Economy name_CUR','delayed_capacity','port_delay','Latitude_CUR','Longitude_CUR'])
    tosave.rename(columns = f_reversed,inplace=True)
    tosave.to_csv('stress_by_port {}_{}.csv'.format(datetime_ext, traffic))
    return

def plot_trends(df2):
    dfplot=df2.groupby('Dep2_YearMonth').agg({'delayed_ship':'sum','delayed_capacity':'sum'}).reset_index()
    dfplot.plot.line(x='Dep2_YearMonth',y='delayed_capacity')
    plt.show()
    dfplot.to_csv("forexport_stress_global.csv")
    return

def get_latest_file():
    """Extract the files with the latest date with the name following the pattern: de2dep_"""
    list_of_files = glob.glob('Y:\\mt\\Dep2Dep_GLOBAL_ports_monthly_agg_*') # * means all if need specific format then *.csv
    latest_file = max(list_of_files, key=os.path.getctime)
    return latest_file
 
#####################################################################
def main(): 
    
    nameoffile = get_latest_file()
    print("Processing file: {}".format(nameoffile))
    datetime_ext = define_datetime()
    traffic = define_traffic(nameoffile)
    
    print("Processing dats for {} traffic".format(traffic))
    print("Saved file extensions: {}".format(datetime_ext))
    
    df = get_inputs(nameoffile)
    df, f_reversed = clean_inputs(df)
    print(df.head())
    
    shangai_long_beach_show(df)
    
    ## 2nd argument (sum or count) - method of aggregating count of ships, teu is medianed, time as well - questionable reasoning... why counts are counted instead of neing summed.
    adf_lane = activity_by_lane(df,'sum', verbose=True)
    adf_lane_count = activity_by_lane(df, 'count',verbose=True)
    adf_lane = pd.merge(adf_lane, adf_lane_count[['SHIP_ID count']], how='inner',left_index=True, right_index=True)
    ###############################################
    
    df1 = reference_lead_time(df, adf_lane) 
    joined = join_ref(df, df1)
    df2 = stalled_capacity_pairs(joined)
    
    dfagg, dfsave = stalled_capacity(df2)
    save_files(dfagg, dfsave, datetime_ext, traffic, f_reversed)
    print("saved both output files")
    plot_trends(dfagg)
###############################################################################
###############################################################################

if __name__ == "__main__": 
    print("Stress index production") 
    main()