import pandas as pd
import requests
import mysql.connector
from mysql.connector.constants import ClientFlag
from sqlalchemy import create_engine



def dim_case():
    engine_pg = create_engine('postgresql+psycopg2://project_aldy:project_password@35.224.124.193:5444/project_database', echo=False)
    data_covid_pg = pd.read_sql('SELECT * FROM data_covid', engine_pg)

    temp = data_covid_pg.columns
    status_name = []
    status_detail = []
    for column in temp:
        if column.isupper():
            status_name.append(column)
        else:
            status_detail.append(column)
    merge = []
    id = 0
    for word in status_name:
        for sentence in status_detail:
            split = sentence.split("_")
            if word.lower() in split:
                id = id + 1
                merge.append([id,split[0].lower(),split[1]])

    result = pd.DataFrame(merge, columns= ['id', 'status_nama', 'status_detail'])
    
    result.to_sql(name='dim_case_table', con=engine_pg, if_exists = 'replace', index=False)

