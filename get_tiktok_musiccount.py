from datetime import datetime
import pandas as pd
import concurrent.futures
import requests
from datetime import datetime
import csv
import time

params = {
    "endDate" : "2024-03-08",
    "period" : 90
}



def fetch_url(identifier):
    url = f"https://customer.api.soundcharts.com/api/v2/tiktok/music/{identifier}/video/volume"
    response = requests.get(url,params= params,headers=headers,auth = (username,password))
    
    # print(response.json()['related'])
    try:
        if response.status_code==200:
            response = response.json()
            streams = [[response['related']['identifier'],response['related']['title'],response['related']['authorName'],response['related']['url'],response['related']['imgUrl'],item['date'],item['value']] for item in response['items']]
            if len(response['items'])==0:
                f = open("Error/tiktok_musicvideo_error.txt","a")
                f.write(f"{identifier}-- Empty \n")
                f.close()
            return url, streams
        else:
            f = open("Error/tiktok_musicvideo_error.txt","a")
            f.write(f"{identifier} \n")
            f.close()
            return url,[]
        # time.sleep(30)

    except:
        f = open("Error/tiktok_musicvideo_error.txt","a")
        f.write(f"{identifier} \n")
        f.close()
        return url,[]


def main():
    print(datetime.now())


    start = 0
    end = 20
    url_hit = start
    input_file = "output/Tiktokids/tiktok_ids_op_350.csv"
    output_filename = "TiktokMusicCount/Tiktok_music_viddeo_.csv"


    song_metadata = []
    df = pd.read_csv(input_file)

    base_urls = [f"{identifiers}" for identifiers in df['Identifier'][start:end]]
    # print(datetime.now())
    song_streams = []
    with open(output_filename,"a",newline="\n",encoding="utf-8") as file:
        writer = csv.writer(file)
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_url = {executor.submit(fetch_url, url): url for url in base_urls}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                print(url)
                try:
                    url, data = future.result()
                    writer.writerows(data)

                    # song_streams = song_streams + data
                except Exception as exc:
                    print(f'{url} generated an exception: {exc}')
                    f = open("Error/tiktok_musicvideo_error.txt","a")
                    f.write(f"{url} \n")
                    f.close()
                    
print(datetime.now())
main()
print(datetime.now())