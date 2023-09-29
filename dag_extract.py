from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import dag, task


import requests
import json
import pandas as pd
import pyarrow
import s3fs

study_cols = ['NCTId','OfficialTitle', 'StudyType','StartDate', 'CompletionDate', 'OverallStatus', 'LeadSponsorName','CollaboratorName','ResponsiblePartyType', 'ResponsiblePartyInvestigatorFullName','ResponsiblePartyInvestigatorAffiliation','LocationCountry','SeeAlsoLinkURL']

pulling_date = '20232601'
total_records = 467212
n = int(total_records/1000)

url_base = "https://clinicaltrials.gov/api/query/study_fields?fields="
last_part = "&min_rnk=1&max_rnk=10&fmt=json"

@dag(
    schedule=None,
    start_date=datetime(2023, 9, 25), 
    catchup=False,
    tags=["clinicaltrial_api"],
)


def clinicaltrial_api():

    
    @task()
    def list_to_str(list_cols):
        """
        Make study_columns which is list of string to string parameter
        """
        str_cols = ",".join(list_cols)
        return str_cols

    
    @task() 
    def get_n_rows(study_cols_str):
        """
        Pull first n rows
        """
        df = pd.DataFrame()
        i = 1
        while i <=n:
            stop  = (i)* 1000
            start = (stop-1000) + 1
            url = url_base  + study_cols_str +"&min_rnk=" + str(start)+ "&max_rnk="+ str(stop)+ "&fmt=json"
            resp = requests.get(url).json()
            resp_wanted = resp['StudyFieldsResponse']['StudyFields']
            df_temp = pd.DataFrame(resp_wanted) 
            df = pd.concat([df, df_temp], ignore_index=True)
            i += 1     
        return df

    
    @task()
    def get_the_rest_rows(study_cols_str):
        """
        Pull the rest rows 
        """
        url = url_base  + study_cols_str  \
                    +"&min_rnk=" \
                    + str((n*1000)+1) \
                    + "&max_rnk=" \
                    + str(total_records) \
                    + "&fmt=json" 
        resp = requests.get(url).json()
        resp_wanted = resp['StudyFieldsResponse']['StudyFields']
        df_temp = pd.DataFrame(resp_wanted)    
        return df_temp

    
    @task()
    def combine_dfs(df1,df2):
        """
        Combine dataframes
        """
        df_total = pd.concat([df1,df2], ignore_index=True )
        return df_total

    
    @task()
    def remove_list_cols(df):
        """
        Change column NCT from list to string
        """
        cols = list(df.columns)
        for i in range(1,len(cols) ):
            df[cols[i]] = df[cols[i]].apply(', '.join)
        return df

    
    @task()
    def load_raw(df):
        """
        Load raw csv data to local or s3 bucket
        """
        #df.to_csv("~/airflow/data/raw/study_df.csv", index = False)
        df.to_csv("s3://clinicaltrial-september2023/raw/study_df.csv", index = False)


    # Flow
    """
    study_cols_str = list_to_str(study_cols)
    study_df = get_n_rows(study_cols_str)
    study_df_total = remove_list_cols(study_df)
    load_to_local(study_df_total)
    """
    #############################
    study_cols_str = list_to_str(study_cols)
    study_df1  = get_n_rows(study_cols_str)
    study_df2 = get_the_rest_rows(study_cols_str) 
    study_df_combine = combine_dfs(study_df1,study_df2)
    study_df_total = remove_list_cols(study_df_combine)
    load_raw(study_df_total)
    
clinicaltrial_api()