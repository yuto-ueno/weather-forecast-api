import csv
from datetime import datetime
import requests
from google.cloud import storage
import tempfile
import os

def fetch_weather_data():
    url = 'https://weather.tsukumijima.net/api/forecast'
    params = {'city': '400040'}
    return requests.get(url, params=params).json()

def write_to_csv(weather_data):
    today = datetime.now().strftime("%Y-%m-%d")
    file_name = f"{today}_weather_forecast.csv"

    with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as temp_file:
        writer = csv.writer(temp_file)
        writer.writerow(['日付', '天気', '最高気温(℃)', '最低気温(℃)', '当日フラグ', '発表日'])
        
        for forecast in weather_data['forecasts']:
            date = forecast['date']
            weather = forecast['telop']
            max_temp = forecast['temperature']['max']['celsius'] if forecast['temperature']['max']['celsius'] else '--'
            min_temp = forecast['temperature']['min']['celsius'] if forecast['temperature']['min']['celsius'] else '--'
            is_today = 1 if date == today else 0
            publishing_date = weather_data['publicTimeFormatted']
            writer.writerow([date, weather, max_temp, min_temp, is_today, publishing_date])
    
    return temp_file.name

def upload_to_gcs(file_path, bucket_name, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)
    print(f"CSVファイルが {bucket_name}/{destination_blob_name} にアップロードされました。")

def main(event, context):
    weather_data = fetch_weather_data()
    temp_file_path = write_to_csv(weather_data)
    bucket_name = "weather_forecast_bucket"
    destination_blob_name = f"{datetime.now().strftime('%Y-%m-%d')}_weather_forecast.csv"
    upload_to_gcs(temp_file_path, bucket_name, destination_blob_name)
    os.unlink(temp_file_path)
