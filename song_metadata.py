import pandas as pd
import requests
import csv
import concurrent.futures






start = 100
end = 1000
url_hit = start
input_file = "Metadata/Input/listsong_metadataCorrect.csv"
output_filename = f"Metadata/Output/song_metadata_{start}_{end}.csv"
output_column_names = ['type', 'uuid', 'name', 'isrc_value', 'countryCode', 'countryName', 'creditName', 'artist_uuid', 'artist_slug', 'artist_name','artist_appUrl','artist_imageUrl', 'releaseDate', 'copyright', 'appUrl', 'imageUrl', 'duration', 'genre_root', 'genre_sub','composers', 'producers', 'label_name', 'label_type', 'danceability', 'energy', 'instrumentalness', 'key', 'liveness', 'loudness','mode', 'speechiness', 'tempo', 'timeSignature', 'valence']
base_url = "https://customer.api.soundcharts.com/api/v2.25/song/"


def fetch_url(uuid):
    url = f"{base_url}{uuid}"
    response = requests.get(url,headers=headers,auth = (username,password))
    song_details = {}
    if response.status_code==200:
        item = response.json()
        song_details = {}
        song_details['type'] = item['type']
        song_details['uuid'] = item['object']['uuid']
        song_details['name'] = item['object']['name']
        try:
            song_details['isrc_value'] = item['object']['isrc']['value']
            song_details['countryCode'] = item['object']['isrc']['countryCode']
            song_details['countryName'] = item['object']['isrc']['countryName']
        except:
            song_details['isrc_value'] = ""
            song_details['countryCode'] = ""
            song_details['countryName'] = ""
        song_details['creditName'] = item['object']['creditName']
        try:
            artist_dict = {}
        
            for d in item['object']['artists']:
                for key, value in d.items():
                    if key in artist_dict:
                        artist_dict[key] += ', ' + value
                    else:
                        artist_dict[key] = value
                        
            song_details['artist_uuid'] = artist_dict['uuid']
            song_details['artist_slug'] = artist_dict['slug']
            song_details['artist_name'] = artist_dict['appUrl']
            song_details['artist_appUrl'] = artist_dict['slug']
            song_details['artist_imageUrl'] = artist_dict['imageUrl']
        except:
            song_details['artist_uuid'] = ''
            song_details['artist_slug'] = ''
            song_details['artist_name'] = ''
            song_details['artist_appUrl'] = ''
            song_details['artist_imageUrl'] = ''
        
        song_details['releaseDate'] = item['object']['releaseDate']
        song_details['copyright'] = item['object']['copyright']
        song_details['appUrl'] = item['object']['appUrl']
        song_details['imageUrl'] = item['object']['imageUrl']
        song_details['duration'] = item['object']['duration']
    
        

        try:
            genre_dict = {'root': [], 'sub': []}
            for d in item['object']['genres']:
                genre_dict['root'].append(d['root'])
                genre_dict['sub'].extend(d['sub'])
        
            song_details['genre_root'] = ",".join(genre_dict['root'])
            song_details['genre_sub'] = ",".join(genre_dict['sub'])
        except:
            song_details['genre_root'] = ""
            song_details['genre_sub'] = ""
            
        
        song_details['composers'] = ",".join(item['object']['composers'])
        song_details['producers'] = ",".join(item['object']['producers'])
    
        try:
            label_dict = {}
            for d in item['object']['labels']:
                for key, value in d.items():
                    if key in label_dict:
                        label_dict[key] += ', ' + value
                    else:
                        label_dict[key] = value
        
            song_details['label_name'] = label_dict['name']
            song_details['label_type'] = label_dict['type']
        except:
            song_details['label_name'] = ""
            song_details['label_type'] = ""
        try:
            song_details['danceability'] = item['object']['audio']['danceability']
            song_details['energy'] = item['object']['audio']['energy']
            song_details['instrumentalness'] = item['object']['audio']['instrumentalness']
            song_details['key'] = item['object']['audio']['key']
            song_details['liveness'] = item['object']['audio']['liveness']
            song_details['loudness'] = item['object']['audio']['loudness']
            song_details['mode'] = item['object']['audio']['mode']
            song_details['speechiness'] = item['object']['audio']['speechiness']
            song_details['tempo'] = item['object']['audio']['tempo']
            song_details['timeSignature'] = item['object']['audio']['timeSignature']
            song_details['valence'] = item['object']['audio']['valence']
        except:
            song_details['danceability'] = ''
            song_details['energy'] = ''
            song_details['instrumentalness'] = ''
            song_details['key'] = ''
            song_details['liveness'] = ''
            song_details['loudness'] = ''
            song_details['mode'] = ''
            song_details['speechiness'] = ''
            song_details['tempo'] = ''
            song_details['timeSignature'] = ''
            song_details['valence'] = ''
    return song_details

def main():
    input_file = pd.read_csv("Metadata/Input/listsong_metadataCorrect.csv")
    base_urls = [f"{uuid}" for uuid in input_file['uuid'][start:end]]

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
                    f = open("Song_Metadata_errors.txt","a")
                    f.write(f"{url} \n")
                    f.close()