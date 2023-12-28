from connection import cursor
import pandas as pd
from fastapi import FastAPI
import datetime
from fastapi import Request
from fastapi.responses import JSONResponse
import ssl
ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
ssl_context.load_cert_chain('/Users/jishnu.suneesh/Documents/cob_application/cert.pem',keyfile='/Users/jishnu.suneesh/Documents/cob_application/key.pem')

app = FastAPI()
current_date=datetime.datetime.now()
result=cursor.execute("select * from GBI_OPS_SEMANTIC_DB.IBB_APP.EVENT_PUBLISH_STATUS_HIST WHERE RPTG_DT=CURRENT_DATE-1 and BO_SIGNAL_CD in ('WW','AMR') and BO_FREQUENCY_CD in ('COB') ORDER BY AMR_PROCESS_TS,RPTG_DT;")

rslt=result.fetchall()
desc = result.description
lst = []
for x in desc:
    lst.append(x[0])
df = pd.DataFrame(rslt, columns=lst)
print(df[['APP_ID', 'RPTG_DT', 'BO_SIGNAL_CD', 'BO_FREQUENCY_CD', 'STATUS_CD', 'AMR_PROCESS_TS']])

def get_cob_status():
    if (df.STATUS_CD == 'PUBLISHED').any():
        rptg_dt=df['RPTG_DT'].iloc[0].strftime("%Y-%m-%d")
        result=f'''COB Published for {rptg_dt}'''
    else:
        result="COB Not Published"


    return (result)

@app.post("/")
async def webhook(request:Request):
    payload=await request.json()
    result = get_cob_status()
    return JSONResponse(content={'fulfillmentText': result})


