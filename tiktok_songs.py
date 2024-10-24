from datetime import datetime
import pandas as pd
import concurrent.futures
import requests
from datetime import datetime
import csv
import time

params = {
        "period": "quarter",
        "sortBy": "total",
        "sortOrder": "desc",
        "minValue": 0,
        "maxValue": 1500000000,
        "minChange": -1500000000,
        "maxChange": 0,
    }


# headers = {
#         "x-app-id": "soundcharts",
#         "x-api-key": "soundcharts"
#     }

base_url = "https://customer.api.soundcharts.com"
new_url = "https://customer.api.soundcharts.com/api/v2/top-song/tiktok/videos%20created"
url = "https://customer.api.soundcharts.com/api/v2/top-song/tiktok/videos%20created?period=quarter&sortBy=total&sortOrder=desc&minValue=0&maxValue=1500000000&minChange=-100&maxChange=1500000000&token=WzAsNzEzMDExODg1XQ%3D%3D&limit=100"


file_number = 1
max_url_hits = 500
column_names = ['Total','Change','Percent','Song_uuid','Song_name','creditName','nexttoken']
file = open(f'Output/Output_{file_number}.csv', 'a', newline='', encoding="UTF-8")
writer = csv.writer(file)
writer.writerow(column_names)
response = requests.get(url, headers=headers,auth=(username, password))
url_hits = 0
while True:
    if response.status_code==200:
        # print("Sttus 200")
        data = response.json()
        nextlink = data['page']['next']
        next_url = f'{base_url}{nextlink}'
        next_token = data['page']['token']
#         print(next_url)
#         print(nextlink)
        for row in data['items']:
            one_row = [row['total'], row['change'], row['percent'], row['song']['uuid'], row['song']['name'], row['song']['creditName'], next_token]
            try:
                writer.writerow(one_row)
            except:
                print(one_row)
                print(f"Error writing {one_row}")
                f = open("Error/tiktok_songs.txt","a")
                f.write(f"{nextlink}-{one_row} \n")
                f.close()
                continue
#             break                                 #-------------------break after one row write

        try:
            response = requests.get(next_url, headers=headers,auth=(username, password))
            # print(f"Fetching...{next_url}")
        except:
            print(f"Error fetching {nextlink}")
            f = open("Error/tiktok_songs.txt","a")
            f.write(f"{nextlink} \n")
            f.close()
        url_hits += 1
        if url_hits%20==0:print(f"url hits = {url_hits}")
        if url_hits >= max_url_hits:
            file_number += 1
            url_hits = 0
            file.close()
            file = open(f'Output/Output_{file_number}.csv','a',newline='', encoding="UTF-8")
            writer = csv.writer(file)
            writer.writerow(column_names)
                    
        
    else:
        print(f"Error Fetching {url}")
        f = open("Error/tiktok_songs.txt","a")
        f.write(f"{nextlink} \n")
        f.close()
        
try:
    file.close()
except:
    pass

#     break                                           #---------------------break after one url fetch
