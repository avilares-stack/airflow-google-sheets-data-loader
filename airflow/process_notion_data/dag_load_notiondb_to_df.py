# first LOAD NOTION DATA INTO A DATAFRAME and then conquer the world
# =====================================================================================================================
# ASSUMPTIONS:
#       - You must have a Notion account and some database created and share with the internal integration
# =====================================================================================================================
from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine
from airflow.decorators import dag, task
from airflow.models import Variable
##########################################################
# global variables definition
##########################################################
var_dag_settings = Variable.get("mysettings", deserialize_json=True)  #get my dag contexts on the dictionary

notion_token=var_dag_settings["notion_token"] #notion token to authorize the script to open your notion pages (https://developers.notion.com/docs/authorization)
notion_db_id=var_dag_settings["notion_db_id"] #notion database id. For example, www.notion.so/temp/[notion_db_id]?v=...
myuser = ''#database user name
mypass = ''#database password
myhost = ''#database hostname
mydbname = ''#database name
myschema = ''#database schema


@dag(start_date=datetime(2022, 2, 20),max_active_runs=1,schedule_interval='10 * * *',catchup=False,tags=["public"]) #dag decorator
def notion_db_ingestion():

    @task
    def notion_data(notion_token: str,notion_db_id: str):
        headers = {"Authorization": "Bearer " + notion_token, "Notion-Version": "2021-05-13","Content-Type": "application/json",} #headers for the API request
        database_url = f"https://api.notion.com/v1/databases/{notion_db_id}/query" #create the notion database url
        response = requests.request("POST",database_url, headers=headers) #build python request
        if response.status_code != 200:
            print(f'Response Status: {response.status_code}') #maybe notion_token or notion_db_id are wrong :)
        else:
            print('Response Status: Success')
            df = pd.DataFrame()
            data_json = response.json() #store request response into a variable in JSON format
            p_key = []
            p_keys_inc = [i['properties'] for i in data_json["results"] if 'properties' in i] #get database headers
            for key in p_keys_inc:
                p_key.extend(key.keys()) #aggregate database headers into a list
            projects = list(set(p_key))
            projects_data = {}
            for p in projects: #as you can see in readme file we have 6 columns (Title, Value, Category, Date, Comment, Date Load)
                if p.strip().lower() =="category": #multi-select type
                    projects_data[p] = [[i['name'] for i in data_json["results"][i]["properties"][p]['multi_select'] if 'name' in i] if (p in data_json["results"][i]["properties"] and  len(data_json["results"][i]["properties"][p]['multi_select'])) > 0 else "" for i in range(len(data_json["results"]))]
                elif p.strip().lower() =="date load": #created_time type
                    projects_data[p] = [data_json["results"][i]["properties"][p]['created_time'] if (p in data_json["results"][i]["properties"] and 'created_time' in data_json["results"][i]["properties"][p]) else "" for i in range(len(data_json["results"]))]
                elif p.strip().lower() in ("comment"): #text type
                    projects_data[p] = [data_json["results"][i]["properties"][p]['rich_text'][0]['text']['content'] if (p in data_json["results"][i]["properties"] and len(data_json["results"][i]["properties"][p]['rich_text']) > 0) else "" for i in range(len(data_json["results"]))]
                elif p.strip().lower() in ("title"): #title type
                    projects_data[p] = [data_json["results"][i]["properties"][p]['title'][0]['plain_text'] if (p in data_json["results"][i]["properties"] and len(data_json["results"][i]["properties"][p]['title']) > 0) else "" for i in range(len(data_json["results"]))]
                elif p.strip().lower() =="date":#date type
                    projects_data[p] = [data_json["results"][i]["properties"][p]['date']['start'] if (p in data_json["results"][i]["properties"] and ('start' in data_json["results"][i]["properties"][p]['date'])) else "" for i in range(len(data_json["results"]))]
                elif p.strip().lower() =='value':#number type
                    projects_data[p] = [data_json["results"][i]["properties"][p]['number'] if (p in data_json["results"][i]["properties"] and 'number' in data_json["results"][i]["properties"][p]) else -1 for i in range(len(data_json["results"]))]
            df = pd.DataFrame(projects_data) #load json file into a pandas dataframe
            ##########################################################
            # load df to database [postgres]
            ##########################################################
            conn = "postgresql+psycopg2://{}:{}@{}/{}".format(myuser, mypass, myhost, mydbname) #build the connection string to connect to the database
            engine = create_engine(conn)#sqlalchemy connection creation
            df.to_sql("notion_data", schema=myschema, con=engine, if_exists='replace', index=False) #load dataframe to your database table. PS: Just change the table name :)

    notion_data(notion_token,notion_db_id) #task creation

pipeline = notion_db_ingestion()