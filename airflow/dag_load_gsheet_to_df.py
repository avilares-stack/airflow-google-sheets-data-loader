# first LOAD GOOGLE SHEETS DATA INTO A DATAFRAME and then conquer the world
# =====================================================================================================================
# ASSUMPTIONS:
#       - You must have a Google Sheet created and the Service Account JSON credentials stored locally or the dictionary
# =====================================================================================================================
import gspread
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
dag_name = 'dag_load_gsheet_to_df'
dag_description = "Load Google Sheet to DF"
dag_run_schedule = '0 10 * * 1-5' 
start_date = datetime(2022, 2, 22)
##########################################################
# functions section
##########################################################
def load_gsheets_to_df(sheet_link, sheet_name, skip_n_rows): #function that based on 3 parameters will load google sheets data into a table
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive'] #scope list that will be responsible to authorize the API request to google drive
    creds_file = ServiceAccountCredentials.from_json_keyfile_name(keyfile, scope) #service account credential for OAuth 2.0 using 
    #in case you have the JSON data instead of the file, use from_json_keyfile_dict()
    client = gspread.authorize(creds_file) #authorize the program to open our google drive
    sheet = client.open_by_url(sheet_link).worksheet(sheet_name) #open google drive sheet by the sheet link and the sheet name
    data = sheet.get_all_records(head=skip_n_rows) #return the google sheet data as a list of dictionaries
    df = pd.DataFrame(data) #dataframe creation based on the list of dictionaries
    if df.shape[0] == 0: #first check: to get the number of rows
        print("Your sheet is empty")
    else:
        print(df)
    ##########################################################
    # load df to database [postgres]
    ##########################################################
    conn = "postgresql+psycopg2://{}:{}@{}/{}".format(myuser, mypass, myhost, mydbname) #build the connection string to connect to the database
    engine = create_engine(conn)#sqlalchemy connection creation
    df.to_sql("gsheet_table", schema=myschema, con=engine, if_exists='replace', index=False) #load dataframe to your database table. PS: Just change the table name :)

with DAG(dag_id=dag_name,
          description=dag_description,
          schedule_interval=dag_run_schedule,
          catchup=False,
          start_date=start_date,
          tags=["public"]
          ) as dag:
    
    setup_task = DummyOperator(task_id='Share_and_Learn_approach') #just a dummy operator to indicates that we are moving on :D
    
    #Our task to load google sheet data into database table through a dataframe
    load_gsheet_to_df = PythonOperator(
       task_id="load_gsheet_to_df",
       python_callable=load_gsheets_to_df,
       provide_context=True,
       op_kwargs={"sheet_link": gsheet_url
                  ,"sheet_name": sheet_name
                  ,"skip_n_rows": 1},
       email_on_failure=True,
       email= developer_email)

    #great advice from Marc Lamberti -> the labels! They are very useful to have all the steps very clear and the dag tasks with some comments
    setup_task >> Label("Load Google Sheet to DF") >> load_gsheet_to_df