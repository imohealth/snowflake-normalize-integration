import pandas as pd
import json
from sqlalchemy import create_engine

print("- App started.")

# GET CREDENTIALS FROM CONFIG
with open('config.json', 'r') as config_file:
    config = json.load(config_file)
    snowflake_conn = config['snowflake_connection']

config_download_location = config['download_location']
config_snowflake_user = snowflake_conn['USER']
config_snowflake_pwd = snowflake_conn['PWD']
config_snowflake_acct = snowflake_conn['ACCOUNT']
config_snowflake_database = snowflake_conn['DATABASE']
config_snowflake_schema = snowflake_conn['SCHEMA']
config_snowflake_table = snowflake_conn['TABLE']

# Connection to snowflake
engine = create_engine(
    'snowflake://{user}:{password}@{account}/{database}/{schema}'.format(
        user=config_snowflake_user,
        password=config_snowflake_pwd,
        account=config_snowflake_acct,
        database= config_snowflake_database,
        schema=config_snowflake_schema
    )
)
cnn = engine.connect()

sql = "GET @%{table} file://{location}".format(
    table=config_snowflake_table,
    location=config_download_location
)
pd.read_sql(sql, cnn)
print("File downloaded to {}/".format(config_download_location))
cnn.close()
