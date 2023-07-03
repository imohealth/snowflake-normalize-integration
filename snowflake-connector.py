import pandas as pd
import json
import uuid
import requests
from snowflake.connector.pandas_tools import pd_writer
from sqlalchemy import create_engine

NORMALIZED_COLUMN = "NORMALIZED_ICD10CM"
NORMALIZE_REQUEST_ID = "REQUEST_ID"
BASE_URL = "https://api-dev.imohealth.com/precision/normalize"
AUDIENCE = "https://api-dev.imohealth.com"
GRANT_TYPE = "client_credentials"
AUTH0_AUTH_URL = "https://auth-dev.imohealth.com/oauth/token"

print("- App started.")

# GET CREDENTIALS FROM CONFIG
with open('config.json', 'r') as config_file:
    config = json.load(config_file)
    imo_keys = config['imo_keys']
    snowflake_conn = config['snowflake_connection']
    write_method = config['write_method']
    batch_size = config['batch_size']

if write_method != "direct" and write_method != "internal":
    print("Invalid write method specified (must be 'direct' or 'internal'")
    exit(1)

client_id = imo_keys['CLIENT_ID']
client_secret = imo_keys['SECRET']
config_snowflake_user = snowflake_conn['USER']
config_snowflake_pwd = snowflake_conn['PWD']
config_snowflake_acct = snowflake_conn['ACCOUNT']
config_snowflake_database = snowflake_conn['DATABASE']
config_snowflake_schema = snowflake_conn['SCHEMA']
config_snowflake_table = snowflake_conn['TABLE']

# CREATE AUTH0 PAYLOAD
auth_data = {
    "grant_type": GRANT_TYPE,
    "client_id": client_id,
    "client_secret": client_secret,
    "audience": AUDIENCE
}
auth_response = requests.post(AUTH0_AUTH_URL, data=auth_data)

# READ TOKEN FROM AUTH0 RESPONSE
auth_response_json = auth_response.json()
auth_token = auth_response_json["access_token"]
auth_token_header_value = "Bearer %s" % auth_token


def write_dataframe():
    if write_method == 'direct':
        # Write pandas dataframe directly to Snowflake
        df2.to_sql('patient_terms', engine, if_exists='replace', index=False, method=pd_writer)
        print("Dataframe pushed directly to {database}.{schema}.{table}".format(
            database=config_snowflake_database,
            schema=config_snowflake_schema,
            table=config_snowflake_table
        ))
    elif write_method == 'internal':
        # Write pandas dataframe to SnowFlake internal stage
        df2.to_csv("patientfile.csv", encoding='utf-8')
        engine.raw_connection().cursor().execute("PUT file://patientfile.csv* @%{table}".format(table=config_snowflake_table))
        print("Dataframe pushed to internal stage {table}. Use SQL GET to download the data (ex: GET @%{table} file://xx/xx".format(table=config_snowflake_table))


# CONVERT PANDAS DATATIME FORMAT TO INCLUDE TIMEZONE
def fix_date_cols(df, tz='UTC'):
    cols = df.select_dtypes(include=['datetime64[ns]']).columns
    for col in cols:
        df[col] = df[col].dt.tz_localize(tz)
    return df


def normalize_batch(df, size):
    request_uuid = uuid.uuid1()

    conditions = df["condition"]
    normalize_requests = []

    for i, condition in enumerate(conditions):
        normalize_requests.append({
            "domain": "problem",
            "record_id": str(i),
            "input_term": condition,
        })

    # Prepare API Request
    auth_token_header = {
        'Authorization': auth_token_header_value,
        'Content-Type': 'text/plain; charset=utf-8'
    }

    request = {
        "client_request_id": str(request_uuid),
        "preferences": {
            "threshold": 0.5,
            "size": size,
            "debug": False,
            "discrepancy_check": False,
            "match_pref": "input_term"
        },
        "requests": normalize_requests
    }

    resp = requests.post(
        BASE_URL,
        headers=auth_token_header,
        json=request,
        allow_redirects=True,
    )

    resp_json = resp.json()

    if resp.status_code != 200:
        print(resp_json)
        return [], None

    normalized_codes = []
    if "requests" in resp_json:
        for request in resp_json["requests"]:
            if "response" not in request:
                normalized_codes.append("")
                continue

            normalize_response = request["response"]

            if "items" not in normalize_response or len(normalize_response["items"]) == 0:
                normalized_codes.append("")
                continue

            normalize_response_top_item = normalize_response["items"][0]

            if "metadata" not in normalize_response_top_item:
                normalized_codes.append("")
                continue

            metadata = normalize_response_top_item["metadata"]

            if "mappings" not in metadata:
                normalized_codes.append("")
                continue

            mappings = metadata["mappings"]

            if "icd10cm" not in mappings:
                normalized_codes.append("")
                continue

            icd10cm = mappings["icd10cm"]

            if "codes" not in icd10cm or len(icd10cm["codes"]) == 0:
                normalized_codes.append("")
                continue

            normalized_codes.append(icd10cm["codes"][0]["code"])

    return normalized_codes, resp_json["request_id"]


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

# Read Snowflake data into pandas
sql = "select * from {database}.{schema}.{table}".format(
    database=config_snowflake_database,
    schema=config_snowflake_schema,
    table=config_snowflake_table
)

df = pd.read_sql(sql, cnn)
df2 = df.head(300)
df2.columns = df2.columns.str.upper()
term_column_name = "condition".upper()

if NORMALIZED_COLUMN in df2.columns:
    print("Snowflake Data already normalized")
    cnn.close()
    exit(0)

df2.insert(3, NORMALIZED_COLUMN, "")
df2.insert(4, NORMALIZE_REQUEST_ID, "")

# Convert pandas datatime format datatime64[ns]
df2 = fix_date_cols(df2)

# Normalize the dataset
normalized_codes = []
request_ids = []
for i in range(0, df2.shape[0], batch_size):
    df_slice = df[i:i+batch_size]
    normalized_codes_tuple = normalize_batch(df_slice, batch_size)
    normalized_codes_slice = normalized_codes_tuple[0]
    request_ids = request_ids + ([normalized_codes_tuple[1]] * batch_size)
    normalized_codes = normalized_codes + normalized_codes_slice

df2[NORMALIZED_COLUMN] = normalized_codes
df2[NORMALIZE_REQUEST_ID] = request_ids

print(df2)
write_dataframe()
cnn.close()
