from datetime import datetime
import pandas as pd
import concurrent.futures
import requests
from datetime import datetime
import csv
import time

params = {
    "startDate" : "2024-03-06",
    "endDate" : "2024-06-04"
}

# params = {
#     "startDate" : "2024-03-06",
#     "endDate" : "2024-06-04"
# }



start = 0
end = 10
url_hit = start
input_file = "Inputs/uuid_round2.csv"
output_filename = f"Output/Shazam_counts_{start}_{end}.csv"
start_time = datetime.now()
print(f"Start = {start} , End = {end}")

def fetch_url(uuid):
    url = url = f"https://customer.api.soundcharts.com/api/v2.24/song/{uuid}/shazam/count"
    response = requests.get(url,params= params,headers=headers,auth = (username,password))
    
    # print(response.json()['related'])
    try:
        if response.status_code==200:
            response = response.json()
            streams = [[uuid,response['related']['name'],response['related']['creditName'],items['date'],plots['identifier'],plots['value']] for items in response['items'] for plots in items['plots']]
            if len(response['items'])==0:
                f = open("Errors/shazam_error.txt","a")
                f.write(f"{uuid}-- Empty \n")
                f.close()
            return url, streams
        else:
            print(f"Error in {uuid}")
            f = open("Errors/shazam_error.txt","a")
            f.write(f"{uuid} \n")
            f.close()
            return url,[]
        # time.sleep(30)

    except:
        f = open("Errors/shazam_error.txt","a")
        f.write(f"{uuid} {response.json()['errors'][0]['message']}\n")
        f.close()
        return url,[]


def main():
    # print(datetime.now())
    # start = 0
    # end = 10
    # url_hit = start
    # input_file = "Relevant Songs/songlist_forstreams.csv"
    # output_filename = "output/shazam_counts.csv"

    song_metadata = []
    df = pd.read_csv(input_file)

    base_urls = [f"{identifiers}" for identifiers in df['songid_list'][start:end]]
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
                    f = open("Errors/shazam_error.txt","a")
                    f.write(f"{url} \n")
                    f.close()
                    
print(datetime.now())
main()
print(datetime.now())