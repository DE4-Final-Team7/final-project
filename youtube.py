import requests
import json
import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

def get_popular_videos(api_key, region_code="kr", max_results=10):
    """
    Fetch popular video details from YouTube.

    Parameters:
        api_key (str): Your YouTube Data API key.
        region_code (str): The region code (default is 'kr').
        max_results (int): Number of videos to fetch (default is 10).

    Returns:
        list: A list of dictionaries containing video details.
    """
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "part": "snippet,statistics",
        "chart": "mostPopular",
        "maxResults": max_results,
        "regionCode": region_code,
        "key": api_key
    }

    response = requests.get(url, params=params)
    data = response.json()

    videos = []
    for item in data.get("items", []):
        video_id = item.get("id")
        video = {
            "videoId": video_id,
            "title": item["snippet"].get("title"),
            "thumbnails": f"https://img.youtube.com/vi/{video_id}/0.jpg",
            "viewCount": item["statistics"].get("viewCount"),
            "commentCount": item["statistics"].get("commentCount"),
            "likeCount": item["statistics"].get("likeCount"),
            "publishedAt": item["snippet"].get("publishedAt")
        }
        videos.append(video)

    return videos

if __name__ == "__main__":
    # Load API key from .env file
    API_KEY = os.getenv("YOUTUBE_API_KEY")

    if not API_KEY:
        print("Error: YouTube API key is missing. Please set it in the .env file.")
    else:
        # Fetch popular videos
        popular_videos = get_popular_videos(API_KEY)

        # Save the results to a JSON file
        with open("popular_videos.json", "w", encoding="utf-8") as json_file:
            json.dump(popular_videos, json_file, ensure_ascii=False, indent=4)

        print("Popular videos data saved to popular_videos.json")
