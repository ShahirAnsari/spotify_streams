from datetime import datetime
import pandas as pd
import concurrent.futures
import requests
from datetime import datetime
import csv
import time
import time

# params = {
#         "period": "quarter",
#         "sortBy": "total",
#         "sortOrder": "desc",
#         "minValue": 0,
#         "maxValue": 1500000000,
#         "minChange": -100,
#         "maxChange": 1500000000,
#     }


params = {
    "startDate" : "2024-04-01",
    "endDate" : "2024-05-30"
}

file = pd.read_csv("Left.csv")

correct_list = []
for uuid in file['Song_uuid'][:2]:
    for x in range(10):
        new_uuid = str(x)+str(uuid)
        print(new_uuid)
        url = f"https://customer.api.soundcharts.com/api/v2.24/song/{new_uuid}/spotify/stream"
        response = requests.get(url,params= params,headers=headers,auth = (username,password))
        if response.status_code==200:
            print("  ===Correct")
            correct_list.append(new_uuid)
            break

new_df = pd.DataFrame(columns=['CorrectDF'])
new_df['CorrectDF'] = correct_list
new_df.to_csv("Correctuuid.csv")