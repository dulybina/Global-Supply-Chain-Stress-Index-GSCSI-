# -*- coding: utf-8 -*-
#### SEQ # 001. Inital processing script of Marine Traffic port calls data.

import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date
import os, csv, re

#### View settings and paths #####       
pd.set_option('display.max_columns', 12)
#pd.set_option('display.width', 1000)
pd.set_option("display.precision", 3)
pd.set_option("display.expand_frame_repr", False)
os.chdir("Y:\\mt")
# Path to historical files
path = "Y:\\PortCalls\\WorldBank\\"
# Path  to new files
new_path = "Y:\\Marine Traffic (sFTP weekly)\\"
OUTPATH =  "Y:\\mt\\"
DRY_OUTPATH = "Y:\\bulkcargo\\work\\"


def get_weekly_drybulk_data(drybulk_files):
    """Process and consolidate weekly data data files for drybulk ships"""
    junedata = pd.read_csv("Y:\\bulkcargo\\weekly\\SAL-5645-worldbank-out-port-calls.csv", sep=';')
    hist_ = []
    hist_.append(junedata)
    for i,file in enumerate(drybulk_files):
        df = pd.read_csv(file, compression='gzip', sep=',')
        #df['source_file'] = file
        hist_.append(df)
    
    h = pd.concat(hist_)
    h['Datetime'] = pd.to_datetime(h['TIMESTAMP_UTC']).dt.tz_localize(None)
    h = h.copy()
    return h

def get_historical_data(path):
    """Process and consolidate historical data files"""
    files = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            files.append(os.path.join(r, file))

    hist_ = []
    for i,file in enumerate(files):
        df = pd.read_csv(file, sep=';',decimal=',',
                         parse_dates=['TIMESTAMP_UTC'])
        df['source_file'] = file
        hist_.append(df)

    h = pd.concat(hist_)
    h['Datetime'] = pd.to_datetime(h['TIMESTAMP_UTC']).dt.tz_localize(None)
    hist = h.copy()
    return hist

def get_filenames(new_path):
    """Process all files from weekly update repository"""
    files = []
    drybulk_files = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(new_path):
        for file in f:
            if not file.startswith("worldBank_bulk"):
                files.append(os.path.join(r, file))
            else:
                drybulk_files.append(os.path.join(r, file))
                
    print(f"total number of container files is {len(files)}")
    print(f"total number of drybulk files is {len(drybulk_files)}")
    return files, drybulk_files

def containers_data(files):   
    """Iterate through repositry of files,open each, concatenate all in 1 DF
    +record file's size (N rows) and exact path of the source file"""
      
    main_=[]
    for i,file in enumerate(files):
        df = pd.read_csv(file,compression='gzip',
                         parse_dates=['TIMESTAMP_UTC'],)
        df.rename(columns = {'DRAUGHT': 'DRAUGHT_METERSX10'},inplace=True)
        df['source_file'] = file
        print("File {} contains {} rows\n".format(file,len(df)))
        main_.append(df)

    dframe = pd.concat(main_)
    dframe['Datetime'] = pd.to_datetime(dframe['TIMESTAMP_UTC']).dt.tz_localize(None)
    new = dframe.copy()
    return new

def timestamp_saved_file(fnm):
    extension = ".pkl"
    """Create timestamped fiename with csv extension FORMAT: _DDMMMYYYY.csv"""
    dateTimeObj = datetime.now()
    timestampStr = dateTimeObj.strftime("%d%b%Y")
    assert isinstance(timestampStr, str)
    filename = fnm+"_"+timestampStr+extension
    return filename

def check_weekday():
    """
    Purpose: find the filename from last week using Monday as an anchor timestamp. 
    Procedure: Check if today is Monday. 
    TRUE- use as a anchor to find the filename from last week 
    FALSE - get the date of the last Monday and use that instead
    """
    if date.today().weekday() == 0:
        dlw = date.today() - timedelta(days=7)
    else:
        today = date.today()
        d = today - timedelta(days=today.weekday())
        if d.weekday() == 0:
            #print("Monday of this week:",d)
            dlw = d - timedelta(days=7)
        else:
            raise Exception("The date is : {}. The anchor date has to be Monday".format(d))
    return dlw
    
def saved_last_week(fnm, extension=".pkl"):
    """Figure out the filename with data from last week"""

    d = check_weekday()
    dayStr = d.strftime("%d%b%Y")
    filename = fnm + "_"+dayStr+extension
    print(filename)
    return filename

def save_unseen(df1, aports, aves):
    """Get full records of ports and ships not seen previosuly and save results to csv """
    df = df1.copy()
    if aports:
        df['getit'] = np.where(df['PORT_ID'].isin(aports),1,0)
        gotit = df[df['getit']==1]
        print(gotit)
        gotit = gotit.copy()
        gotit.drop(columns=['getit'],inplace=True)
        gotit.to_csv(os.path.join(OUTPATH,timestamp_saved_file("new_records_of_ports")))
    else:
        print("No new ports")
    if aves:
        df['getit'] = np.where(df['SHIP_ID'].isin(aves),1,0)
        gotit = df[df['getit']==1]
        print(gotit)
        gotit = gotit.copy()
        gotit.drop(columns=['getit'],inplace=True)
        gotit.to_csv(os.path.join(OUTPATH,timestamp_saved_file("new_records_of_vessesls")))
    else:
        print("No new vessels")
    
def comparison(older, df):
    """
    Compare new and historical data to identify/print new ports and vessels (unique to this week's dataset.
    Clarification: NOT comparing most recent weekly update with evrth provided before,
    RATHER: everything processed last week with everything processed today 
    [Reason: check for potential data revisions]
    """ 
    aves, aports = None, None
    ports_hist = list(set(list(zip(older['PORT_ID'], older['PORT_NAME']))))
    ports_new = list(set(list(zip(df['PORT_ID'], df['PORT_NAME']))))
    added_ports = [x for x in ports_new if x not in ports_hist]
    if len(added_ports) > 0: 
        aports = [x[0] for x in added_ports]
        print("NEW PORTS ADDED:")
        print(added_ports)
  
    #Added vessels
    hist_ves = list(set(list(zip(older['SHIP_ID'], older['IMO']))))
    new_ves = list(set(list(zip(df['SHIP_ID'], df['IMO']))))
    added_ves = [x for x in new_ves if x not in hist_ves]
    if len(added_ves)> 0:
        aves = [x[0] for x in added_ves]
        print("NEW VESSELS ADDED:")
        print(aves)
        
    return aves, aports

def main():
    ###################################
    #### Process data (new and old) ###
    ###################################
    files, drybulk_files = get_filenames(new_path)

    # CONTAINERSHIPS
    print("Processing containerships\n")
    filename = "Saved_data_with_missing"
    hist = get_historical_data(path)
    new = containers_data(files)
    frame=pd.concat([hist,new])
    frame.sort_values(by=['SHIP_ID','Datetime'], ascending=[True,True], inplace=True)
    df =frame.drop_duplicates(subset=['SHIP_ID','IMO','TIMESTAMP_UTC','PORT_ID','MOVE_TYPE'],keep='first')
    print(df.describe())
    print(len(df), len(frame) - len(df))
    df.to_pickle(os.path.join(OUTPATH,timestamp_saved_file(filename)[:-4]+".pkl"))

    print("File saved to {} at {}".format(timestamp_saved_file(filename)[:-4]+".pkl",OUTPATH))
    
    # DRYBULK 
    print("Processing drybulk\n")
    filename = "Saved_drybulk_all_"
    hist_dry = pd.read_csv("Y:\\bulkcargo\\data\\SAL-5426-out-port-calls.csv", sep=";")
    drybulk_weekly = get_weekly_drybulk_data( drybulk_files)

    hist_dry['Datetime'] = pd.to_datetime(hist_dry['TIMESTAMP_UTC']).dt.tz_localize(None)

    frame= pd.concat([hist_dry,drybulk_weekly])
    frame.sort_values(by=['SHIP_ID','Datetime'], ascending=[True,True], inplace=True)
    df = frame.drop_duplicates(subset=['SHIP_ID','IMO','TIMESTAMP_UTC','PORT_ID','MOVE_TYPE'],keep='first')
    print(len(df), len(frame) - len(df))
    df.to_pickle(os.path.join(DRY_OUTPATH,timestamp_saved_file(filename)+".pkl"))
    assert len(df[df['COMFLEET_GROUPEDTYPE'] =='DRY BULK'])==len(df)

    #Compare with last week 
    # #last_file = saved_last_week(filename)
    # try:
    #     svd = saved_last_week(filename)
    # except FileNotFoundError():
    #     svd=False
       
    # if svd:
    #     older = pd.read_csv(os.path.join(OUTPATH, svd),low_memory=False)
    #     print(older.dtypes)
    #     aves, aports = comparison(older,df)

    # aves, aports = comparison(older,df)
    # save_unseen(df, aports, aves)

    #Derive weekly indicators
    #df.set_index('TIMESTAMP_UTC').resample('W-MON',label='left',closed='left').size().plot(title="Number of observations per week")
    #plt.show()

if __name__ == "__main__":
    print("Running script...")
    main()    

