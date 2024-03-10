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

username = "mbairathi@gmail.com"
password = "D@ta_access1"

headers = {
        "x-app-id": "UCL_19889CC5",
        "x-api-key": "582828be01568226"
    }

def fetch_url(uuid):
    url = f"https://customer.api.soundcharts.com/api/v2/song/{uuid}/youtube/views"
    response = requests.get(url,params= params,headers=headers,auth = (username,password))
    try:
        if response.status_code==200:
            streams = [[uuid,items['date'],videoplots['identifier'],videoplots['value']] for items in response.json()['items'] for videoplots in items['videoPlots']]
            return url, streams
        else:
            f = open("Error/youtubeerror.txt","a")
            f.write(f"{uuid} \n")
            f.close()
            return url,[]
        # time.sleep(30)
    except:
        return url,[]


def write_file(streams,filename):
    df = pd.DataFrame(streams,columns = ["uuid","Date","identifier","Total"])
    try:
        old_df = pd.read_excel(filename)
        df = pd.concat([old_df,df])
        df.drop("Unnamed: 0",axis=1,inplace=True)
    except:
        pass
    df.to_excel(filename)

def main():
    print(datetime.now())


    # base_url = "https://customer.api.soundcharts.com/api/v2.25/song/"
    start = 0
    end = 20
    url_hit = start
    input_file = "Relevant Songs/songlist_forstreams.csv"
    output_filename = "YoutubeViews/youtube_views_40_80_test.csv"

    # url = "https://customer.api.soundcharts.com/api/v2.24/song/"
    # url = "https://customer.api.soundcharts.com/api/v2/top-song/tiktok/views"

    

    # uuid = "7d534228-5165-11e9-9375-549f35161576"
    # url = f"https://customer.api.soundcharts.com/api/v2.24/song/{uuid}/spotify/stream"


    song_metadata = []
    df = pd.read_csv(input_file)

    base_urls = [f"{uuid}" for uuid in df['song_uuid'][start:end]]
    # print(datetime.now())
    song_streams = []
    with open(output_filename,"a",newline="\n") as file:
        writer = csv.writer(file)
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:  # You can adjust the max_workers value as needed
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
                    f = open("Error/youtubeerror.txt","a")
                    f.write(f"{url} \n")
                    f.close()
                    # time.sleep(30)

        #         if len(song_streams)>20000:
        #             write_file(song_streams,output_filename)
        #             song_streams = []
        #             # time.sleep(120)
        # # print(datetime.now())
        # write_file(song_streams,output_filename)
        # song_streams = []       
    # time.sleep(120)
print(datetime.now())
main()
print(datetime.now())