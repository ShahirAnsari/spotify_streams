from datetime import datetime
import pandas as pd
import concurrent.futures
import requests
from datetime import datetime
import csv
import time
import time


start = 0
end = 2
url_hit = start
input_file = "Left.csv"
inp_file = pd.read_csv(input_file)
output_filename = f"Tiktok_songs__left_0_9__{start}_{end}.csv"
output_column_names = ['uuid','Songname']


def fetch_url(uuid):
    url = f"https://customer.api.soundcharts.com/api/v2.24/song/{uuid}/spotify/stream"
    response = requests.get(url,params= params,headers=headers,auth = (username,password))
    try:
        if response.status_code==200:
            streams = [uuid,response.json()['related']['name']]
            print(streams)
            return uuid,streams
        else:
            return uuid, []
    except:
        print(f'{url} ---generated an exception: {exc}')
        f = open("Issues.txt","a")
        f.write(f"{url} \n")
        f.close()

def main():
    with open(output_filename,"a",newline="\n") as file:
        writer = csv.writer(file)
        writer.writerow(output_column_names)
        
        for uuid in inp_file['Song_uuid'][start:2]:
            print(uuid)
            base_urls = list(map(lambda x:str(x)+uuid,range(10)))
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                future_to_url = {executor.submit(fetch_url, url): url for url in base_urls}
                for future in concurrent.futures.as_completed(future_to_url):
                    url = future_to_url[future]
                    print(url)
                    try:
                        url, data = future.result()
                        if data:
                            writer.writerow(data)
                        # song_streams = song_streams + data
                    except Exception as exc:
                        print(f'{url} generated an exception: {exc}')
                        f = open("Issues.txt","a")
                        f.write(f"{url} \n")
                        f.close()
