from app2 import graph_post_sendmail,get_access_token,build_message

# Send and print response for debugging (status and body)
import pandas as pd
payload = pd.read_csv('test.csv')
for index,row in payload.iterrows():
    payload = build_message(row)
    resp = graph_post_sendmail(get_access_token(), payload)
    break
print(resp.status_code)
print(resp.text)