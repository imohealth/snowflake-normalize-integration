import snowflake.connector
import pandas as pd
import json
import requests
import io
from snowflake.connector.pandas_tools import write_pandas, pd_writer
from sqlalchemy import create_engine


print("- App started.")

# GET CREDENTIALS FROM CONFIG
with open('config.json', 'r') as config_file:
    config = json.load(config_file)
    imo_keys = config['imo_keys']
    snowflake_conn = config['snowflake_connection']


client_id = imo_keys['CLIENT_ID']
client_secret = imo_keys['SECRET']
config_snowflake_user = snowflake_conn['USER']
config_snowflake_pwd = snowflake_conn['PWD']
config_snowflake_acct = snowflake_conn['ACCOUNT']
config_snowflake_database = snowflake_conn['DATABASE']
config_snowflake_schema = snowflake_conn['SCHEMA']


BASE_URL = 'https://api-dev.imohealth.com/precision/normalizedemo/v2/icd-10-cm'
audience = "https://api-dev.imohealth.com"
grant_type = "client_credentials"
data = {
    "grant_type": grant_type,
    "client_id": client_id,
    "client_secret": client_secret,
    "audience": audience
}
auth0_auth_url = "https://auth-dev.imohealth.com/oauth/token"
auth_response = requests.post(auth0_auth_url, data=data)

# READ TOKEN FROM AUTH0 RESPONSE
auth_response_json = auth_response.json()
auth_token = auth_response_json["access_token"]
auth_token_header_value = "Bearer %s" % auth_token

# CONVERT PANDAS DATATIME FORMAT TO INCLUDE TIMEZONE
def fix_date_cols(df, tz='UTC'):
     cols = df.select_dtypes(include=['datetime64[ns]']).columns
     for col in cols:
         df[col] = df[col].dt.tz_localize(tz)
     return df

# REST API REQUEST TO NORMALIZE API
def normalize(term):
    # Prepare API Request
    auth_token_header = {
        'Authorization': auth_token_header_value,
        'Content-Type': 'text/plain; charset=utf-8'
    }

    # Send Normalize Request
    params = {'term': term, 'includeIMO': 'false', 'threshold': '0.5'}
    response = requests.get(BASE_URL, params=params, headers=auth_token_header)
    json_data = json.loads(response.text)
    if 'data' in json_data and len(json_data['data']) > 0:
        if 'icd10cm' in json_data['data'][0] and len(json_data['data'][0]['icd10cm']) > 0:
            if 'code' in json_data['data'][0]['icd10cm'][0]:
                return json_data['data'][0]['icd10cm'][0]['code'].upper()
            else:
                return ""
        else:
            return ""
    else:
        return ""


# TAKE USER INPUT TO BUILD SNOWFLAKE CONNECTION
print('Building your snowflake connection..snowflake://{user}:{password}@{account}/{database}/{schema}')
snowflake_user = input("Please enter your snowflake username: ")
snowflake_pwd = input("Please enter your snowflake password: ")
snowflake_acct = input("Please enter your snowflake account name: ")
snowflake_database = input("Please enter your snowflake database name: ")
snowflake_schema = input("Please enter your schema name: ")
if not snowflake_user:
    snowflake_user = config_snowflake_user
if not snowflake_pwd:
    snowflake_pwd = config_snowflake_pwd
if not snowflake_acct:
    snowflake_acct = config_snowflake_acct
if not snowflake_database:
    snowflake_database= config_snowflake_database
if not snowflake_schema:
    snowflake_schema = config_snowflake_schema

        
# Connection to snowflake
engine = create_engine(
    'snowflake://{user}:{password}@{account}/{database}/{schema}'.format(
        user=snowflake_user,
        password=snowflake_pwd,
        account=snowflake_acct,
        database= snowflake_database,
        schema=snowflake_schema
    )
)
cnn = engine.connect()


# Read Snowflake data into pandas
sql = "select * from TESTSNOWFLAKE.TEST_SCHEMA.PATIENT_TERMS"
df = pd.read_sql(sql, cnn)


# Column mapping
# For the 
df2 = df.head(300)
df2.columns = df2.columns.str.upper()
term_column_name = input("Type the term description column? Choose from the following\n1. {column1} \n2. {column2} \n3. {column3}\n4. None\n".format(
    column1=df.columns[0],
    column2=df.columns[1],
    column3=df.columns[2]
))
term_column_name = term_column_name.upper()
# Normalize the dataset
df2.insert(3, "NORMALIZED_ICD10CM", "")
df2["NORMALIZED_ICD10CM"] = df2.apply(lambda row: normalize(row[term_column_name]),axis=1)
# Convert pandas datatime format datatime64[ns] 
df2 = fix_date_cols(df2)
print(df2)

# Write pandas dataframe to SnowFlake internal stage
# cursor.execute(
#    "PUT file://this_directory_path/is_ignored/myfile.csv @mystage",
#    file_stream=<io_object>)
# text buffer
#s_buf = io.BytesIO()

# saving a data frame to a buffer (same as with a regular file):
#df2.to_csv("patientfile.csv", encoding='utf-8')
#file://C:\temp\load\contacts*.csv
#engine.raw_connection().cursor().execute("PUT file://C:/users/mshahzad/source/repos/normalize-snowflake-app/patientfile.csv* @%PATIENT_TERMS")
# Write pandas dataframe directly to Snowflake
df2.to_sql('patient_terms', engine , if_exists='replace', index=False, method=pd_writer)


cnn.close()