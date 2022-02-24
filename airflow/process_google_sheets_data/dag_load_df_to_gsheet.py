# first LOAD DATAFRAME INTO GOOGLE SHEETS and then conquer the world
# =====================================================================================================================
# ASSUMPTIONS:
#       - You must have a Google Sheet created and the Service Account JSON credentials stored locally or the dictionary
# =====================================================================================================================
import pygsheets
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

from airflow import DAG
from airflow.utils.edgemodifier import Label
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
##########################################################
# global variables definition
##########################################################
var_dag_settings = Variable.get("mysettings", deserialize_json=True) #get my dag contexts on the dictionary
keyfile = var_dag_settings["json_sa_file"] #google drive JSON keyfile
developer_email = var_dag_settings["developer_email"] #the dag responsible developer to get dag notifications
gsheet_url = var_dag_settings["gsheet_url"] #your google sheet URL
sheet_name = var_dag_settings["sheet_name"] #your google sheet name
#the following variables are optional, just in case if you want to load the dataframe to a database table using sqlalchemy
myuser = ''#database user name
mypass = ''#database password
myhost = ''#database hostname
mydbname = ''#database name
myschema = ''#database schema
#Variables to define DAG parameters
dag_name = 'dag_load_df_to_gsheet'
dag_description = "Load DF to Google Sheet"
dag_run_schedule = '0 10 * * 1-5' 
start_date = datetime(2022, 2, 22)
##########################################################
# functions section
##########################################################
def expt_df_to_gsheets(sheet_link, sheet_name, cell_init, df):
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive'] #scope list that will be responsible to authorize the API request to google drive
    client = pygsheets.authorize(service_file=keyfile) #authorize the program to open our google drive
    sheet = client.open_by_url(gsheet_url)  #open google drive sheet link
    sheet = sheet.worksheet_by_title(sheet_name) #define which sheet we will load the data from the dataframe
    sheet.clear(cell_init) #in case if we want to eliminated data stored in the sheet we just indicate that we want to clear all the cells after the cell_init (e.g. "A1")
    sheet.set_dataframe(df, cell_init)

df = pd.DataFrame({'id': [1, 2, 3, 4],
                    'notes': ['Airflow','Python','Open-Source','Awesome']})

with DAG(dag_id=dag_name,
          description=dag_description,
          schedule_interval=dag_run_schedule,
          catchup=False,
          start_date=start_date,
          tags=["public"]
          ) as dag:
    
    setup_task = DummyOperator(task_id='Share_and_Learn_approach') #just a dummy operator to indicates that we are moving on :D
    
    #Our task to load google sheet data into database table through a dataframe
    load_df_to_gsheet = PythonOperator(
       task_id="load_df_to_gsheet",
       python_callable=expt_df_to_gsheets,
       provide_context=True,
       op_kwargs={"sheet_link": gsheet_url
                  ,"sheet_name": sheet_name
                  ,"cell_init": "A1"
                  ,"df": df},
       email_on_failure=True,
       email= developer_email)

    #great advice from Marc Lamberti -> the labels! They are very useful to have all the steps very clear and the dag tasks with some comments
    setup_task >> Label("Load DF to Google Sheet") >> load_df_to_gsheet