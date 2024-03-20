from datetime import datetime
import pandas as pd
import time
import concurrent.futures
import requests
from datetime import datetime
import pandas as pd
import csv
import time

params = {
    "startDate" : "2023-12-10",
    "endDate" : "2024-03-08"
}



def fetch_url(uuid):
    url = f"https://customer.api.soundcharts.com/api/v2.24/song/{uuid}/spotify/stream"
    response = requests.get(url,params= params,headers=headers,auth = (username,password))
    try:
        if response.status_code==200:
            streams = [[uuid,items['date'],items['plots'][0]['value']] for items in response.json()['items']]
            return url, streams
        else:
            f = open("spotify_stream_errors.txt","a")
            f.write(f"{uuid} \n")
            f.close()
            return url,[]
        # time.sleep(30)
    except:
        f = open("spotify_stream_errors.txt","a")
        f.write(f"{uuid} \n")
        f.close()
        return url,[]


def main():
    print(datetime.now())


    start = 0
    end = 50
    url_hit = start
    input_file = "Relevant Songs/songlist_forstreams.csv"
    output_filename = "Spotify Streams/threadsong_streams.csv"


    song_metadata = []
    df = pd.read_csv(input_file)

    base_urls = [f"{uuid}" for uuid in df['song_uuid'][start:end]]
    # print(datetime.now())
    song_streams = []
    with open(output_filename,"a",newline="\n") as file:
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
                    f = open("spotify_stream_errors.txt","a")
                    f.write(f"{url} \n")
                    f.close()
                    
print(datetime.now())
main()
print(datetime.now())