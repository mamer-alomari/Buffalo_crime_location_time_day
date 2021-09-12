url="data.buffalony.gov"
db="d6g9-xbgu"

def get_data(url,db):
    from sodapy import Socrata
    import json
    import pandas as pd
    import numpy as np 
    client = Socrata(url, None)
    results = client.get(db, limit=253370)
    df=pd.DataFrame.from_records(results,columns=['incident_datetime','latitude','longitude','day_of_week','parent_incident_type'])
    df.incident_datetime=pd.to_datetime(df.incident_datetime)
    return df 

def data_cleaning(df):
    df=df.dropna()
    df.latitude=df.latitude.astype(float)
    df.longitude=df.longitude.astype(float)
    df.day_of_week=df.day_of_week.str.replace('SATURDAY','Saturday')
    df.day_of_week=df.day_of_week.str.replace('FRIDAY','Friday')
    df.day_of_week=df.day_of_week.str.replace('SUNDAY', 'Sunday')
    df.day_of_week=df.day_of_week.str.replace('MONDAY','Monday')
    df.day_of_week=df.day_of_week.str.replace('TUESDAY','Tuesday')
    df.day_of_week=df.day_of_week.str.replace('WEDNESDAY','Wednesday')
    df.day_of_week=df.day_of_week.str.replace('THURSDAY','Thursday')
    return df 

def sliced(df):
    df=df.sort_values('incident_datetime')
    df=df.set_index('incident_datetime')
    df=df.loc['01-01-2007':]
    train=df[:'01-01-2021']
    test=df['01-02-2021':]
    return train,test

def date_test_split(train,test):
    date_train=train.reset_index().incident_datetime
    date_test=test.reset_index().incident_datetime
    return date_train,date_test

def loc_train_test(train,test):
    loc_train=train.drop(columns=['day_of_week','parent_incident_type'])
    loc_test=test.drop(columns=['day_of_week','parent_incident_type'])
    return loc_train,loc_test

def type_train_test(train,test):
    type_train=train
    type_test=test
    return type_train,type_test

def date_full_pipeline(url,db): 
    df=get_data(url,db)
    df=data_cleaning(df)
    train,test=sliced(df)
    date_train,date_test=date_test_split(train,test)
    return date_train,date_test

def loc_full_pipeline(url,db): 
    df=get_data(url,db)
    df=data_cleaning(df)
    train,test=sliced(df)
    loc_train,loc_test=loc_train_test(train,test)
    return loc_train,loc_test
    
def type_full_pipeline(url,db):
    df=get_data(url,db)
    df=data_cleaning(df)
    train,test=sliced(df)
    type_train,type_test=type_train_test(train,test)
    return type_train,type_test

def date_quick_pipeline(df):
    df=data_cleaning(df)
    train,test=sliced(df)
    date_train,date_test=date_test_split(train,test)
    return date_train,date_test

def loc_quick_pipeline(df):
    df=data_cleaning(df)
    train,test=sliced(df)
    loc_train,loc_test=loc_train_test(train,test)
    return loc_train,loc_test

def type_quick_pipeline(df):
    df=data_cleaning(df)
    train,test=sliced(df)
    type_train,type_test=type_train_test(train,test)
    return type_train,type_test

def type_quick_pipeline(df):
    df=data_cleaning(df)
    train,test=sliced(df)
    type_train,type_test=type_train_test(train,test)
    return type_train,type_test

def sliced_advanced(df,train_date,test_date):
    df=df.sort_values('incident_datetime')
    df=df.set_index('incident_datetime')
    df=df.loc['01-01-2007':]
    train=df[:train_date]
    test=df[test_date:]
    return train,test

def date_advanced_pipeline(df,train_date,test_date):
    #require dates for train and test in paranthasies
    df=data_cleaning(df)
    train,test=sliced_advanced(df,train_date,test_date)
    date_train,date_test=date_test_split(train,test)
    return date_train,date_test

def loc_advanced_pipeline(df,train_date,test_date):
    df=data_cleaning(df)
    train,test=sliced_advanced(df,train_date,test_date)
    loc_train,loc_test=loc_train_test(train,test)
    return loc_train,loc_test

def type_advanced_pipeline(df,train_date,test_date):
    import pandas as pd 
    df=data_cleaning(df)
    df1=pd.get_dummies(df)
    train,test=sliced_advanced(df1,train_date,test_date)
    type_train,type_test=type_train_test(train,test)
    return type_train,type_test

