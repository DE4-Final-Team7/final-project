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
        list: A list of video IDs.
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

    video_ids = [item.get("id") for item in data.get("items", [])]
    return video_ids

def get_video_comments(api_key, video_id, max_results=10):
    """
    Fetch comments from a YouTube video.

    Parameters:
        api_key (str): Your YouTube Data API key.
        video_id (str): The ID of the YouTube video.
        max_results (int): Number of comments to fetch (default is 10).

    Returns:
        list: A list of dictionaries containing comment details.
    """
    url = "https://www.googleapis.com/youtube/v3/commentThreads"
    params = {
        "part": "snippet",
        "videoId": video_id,
        "maxResults": max_results,
        "key": api_key,
        "order": "relevance"  # Sort by most relevant (highest like count)
    }

    response = requests.get(url, params=params)
    data = response.json()

    comments = []
    for item in data.get("items", []):
        comment_data = item["snippet"]["topLevelComment"]["snippet"]
        comment = {
            "textDisplay": comment_data.get("textDisplay"),
            "authorDisplayName": comment_data.get("authorDisplayName"),
            "publishedAt": comment_data.get("publishedAt"),
            "likeCount": comment_data.get("likeCount")
        }
        comments.append(comment)

    return comments

if __name__ == "__main__":
    # Load API key from .env file
    API_KEY = os.getenv("YOUTUBE_API_KEY")

    if not API_KEY:
        print("Error: YouTube API key is missing. Please set it in the .env file.")
    else:
        # Fetch popular video IDs
        video_ids = get_popular_videos(API_KEY)

        if not video_ids:
            print("No popular videos found.")
        else:
            all_comments = {}

            for video_id in video_ids:
                print(f"Fetching comments for video ID: {video_id}")
                comments = get_video_comments(API_KEY, video_id)
                all_comments[video_id] = comments

            # Save all comments to a JSON file
            with open("comments_by_video.json", "w", encoding="utf-8") as json_file:
                json.dump(all_comments, json_file, ensure_ascii=False, indent=4)

            print("Comments data saved to comments_by_video.json")
