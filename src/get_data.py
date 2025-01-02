
import requests
from box import Box
from typing import Dict, List


def get_data_from_api(url:str, params:Box) -> Box:
    """_summary_

    Args:
        url (str): _description_
        params (Box): _description_

    Returns:
        Box: _description_
    """
    response = requests.get(url, params)
    data = Box(response.json())
    return data


def get_popular_videos(url:str, params:Box) -> List[Dict]:
    """_summary_

    Args:
        url (str): _description_
        params (Box): _description_

    Returns:
        List[Dict]: _description_
    """
    data = get_data_from_api(url, params) 

    videos = list()
    for item in data.get("items", []):
        video_id = item.id
        video = {
            "video_id": video_id,
            "title": item.snippet.title,
            "category_id": item.snippet.categoryId,
            "view_count": item.statistics.viewCount,
            "comment_count": item.statistics.commentCount,
            "like_count": item.statistics.likeCount,
            "published_at": item.snippet.publishedAt
        }
        videos.append(video)
    
    return videos


def get_video_comments(list_video_id:List, url:str, params:Box) -> List[Dict]:
    """_summary_

    Args:
        list_video_id (List): _description_
        url (str): _description_
        params (Box): _description_

    Returns:
        List[Dict]: _description_
    """
    comments = []
    for video_id in list_video_id:
        params.videoId = video_id
        data = get_data_from_api(url, params)

        for item in data.get("items", []):
            comment = {
                "video_id": video_id,
                "text_display": item.snippet.topLevelComment.snippet.textDisplay,
                "author_display_name": item.snippet.topLevelComment.snippet.authorDisplayName,
                "published_at": item.snippet.topLevelComment.snippet.publishedAt,
                "like_count": item.snippet.topLevelComment.snippet.likeCount
            }
            comments.append(comment)
    
    return comments


