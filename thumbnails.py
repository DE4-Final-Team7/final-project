import requests
import json
import os
from dotenv import load_dotenv

def get_video_thumbnails(api_key, region_code="kr", max_results=10):
    """
    Fetch video thumbnails from YouTube.

    Parameters:
        api_key (str): Your YouTube Data API key.
        region_code (str): The region code (default is 'kr').
        max_results (int): Number of videos to fetch (default is 10).

    Returns:
        list: A list of dictionaries containing video IDs and thumbnail URLs.
    """
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "part": "snippet",
        "chart": "mostPopular",
        "maxResults": max_results,
        "regionCode": region_code,
        "key": api_key
    }

    response = requests.get(url, params=params)
    data = response.json()

    thumbnails = []
    for item in data.get("items", []):
        video_id = item.get("id")
        thumbnail = {
            "videoId": video_id,
            "thumbnailUrl": f"https://img.youtube.com/vi/{video_id}/0.jpg"
        }
        thumbnails.append(thumbnail)

    return thumbnails

if __name__ == "__main__":
    # Replace with your YouTube Data API key
    API_KEY = os.getenv("YOUTUBE_API_KEY")

    # Fetch video thumbnails
    video_thumbnails = get_video_thumbnails(API_KEY)

    # Save the results to a JSON file
    with open("video_thumbnails.json", "w", encoding="utf-8") as json_file:
        json.dump(video_thumbnails, json_file, ensure_ascii=False, indent=4)

    print("video_thumbnails.json")
