import snowflake.connector

account = 'gbiprod.istamdcc.us-west-2.aws'
user = 'jsuneesh@apple.com'
warehouse = 'GBI_OTHERS_DEV_APS_VWH'
database = 'GBI_OPS_SEMANTIC_DB'
schema = 'IBB_APP'
authenticator = 'externalbrowser'


connection = snowflake.connector.connect(
    user=user,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema,
    authenticator=authenticator
)

cursor = connection.cursor()