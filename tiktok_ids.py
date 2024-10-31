from datetime import datetime
import pandas as pd
import concurrent.futures
import requests
from datetime import datetime
import csv
import time


params = {
    "platform" : "tiktok",
    "offset" : 0,
    "limit" : 100
}



# url = f"https://customer.api.soundcharts.com/api/v2/song/{id}/identifiers"

def fetch_url(uuid):
    uuid = "7d534228-5165-11e9-9375-549f35161576"
    data = []
    url = f"https://customer.api.soundcharts.com/api/v2/song/{uuid}/identifiers"
    response = requests.get(url,headers = headers, params = params)
    if response.status_code==200:
        response_json = response.json()
        next = response_json['page']['next']
        data.extend([[response_json['related']['uuid'],response_json['related']['name'],response_json['related']['creditName'],items['identifier'], items['url'],items['default']] for items in response_json['items']])
        while next is not None:
            next_url = f"{base_url}{next}"
            print(next_url)
            response = requests.get(next_url,headers = headers)
            if response.status_code==200:
                response_json = response.json()
                next = response_json['page']['next']
                data.extend([[response_json['related']['uuid'],response_json['related']['name'],response_json['related']['creditName'],items['identifier'], items['url'],items['default']] for items in response_json['items']])
    return data



def main():
    print(datetime.now())
    start = 0
    end = 10
    url_hit = start
    input_file = "Inputs/uuid_round2.csv"
    output_filename = f"Output/Tiktok_ids_{start}_{end}.csv"
    output_column_names = ['uuid','name','creditName','identifier','url','default']

    df = pd.read_csv(input_file)

    base_urls = [f"{uuid}" for uuid in df['songid_list'][start:end]]
    # print(datetime.now())
    with open(output_filename,"a",newline="\n") as file:
        writer = csv.writer(file)
        writer.writerow(output_column_names)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_to_url = {executor.submit(fetch_url, url): url for url in base_urls}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                print(url)
                try:
                    data = future.result()
                    writer.writerows(data)
                        
                    # song_streams = song_streams + data
                except Exception as exc:
                    print(f'{url} generated an exception: {exc}')
                    f = open("Errors/spotify_stream_errors.txt","a")
                    f.write(f"{url} \n")
                    f.close()