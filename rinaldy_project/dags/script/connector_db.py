import pandas as pd
import requests
import mysql.connector
from mysql.connector.constants import ClientFlag
from sqlalchemy import create_engine



def covidjson_to_stagging():
    engine = create_engine('mysql+mysqlconnector://user_aldy:password@35.224.124.193:3314/db_staging', echo=False)
    url = "https://covid19-public.digitalservice.id/api/v1/rekapitulasi_v2/jabar/harian?level=kab"
    resp = requests.get(url)
    # return resp.json()['data']['content']
    df_data = pd.DataFrame(resp.json()['data']['content'])

    df_data.to_sql(name='data_covid_staging', con=engine, if_exists = 'replace', index=False)
    

def data_to_prod():
    engine_pg = create_engine('postgresql+psycopg2://project_aldy:project_password@35.224.124.193:5444/project_database', echo=False)
    engine = create_engine('mysql+mysqlconnector://user_aldy:password@35.224.124.193:3314/db_staging', echo=False)

    data_covid = pd.read_sql('SELECT * FROM data_covid_staging', engine)

    data_covid.to_sql(name='data_covid', con=engine_pg, if_exists = 'replace', index=False)

