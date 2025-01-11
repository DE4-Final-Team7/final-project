
import logging
import requests
from box import Box
from typing import Dict, List
import re
from konlpy.tag import Hannanum


def get_data_from_api(url:str, params:Box) -> Box:
    """get data from api

    Args:
        url (str): url for api
        params (Box): parameters for api including api key and region code, and so on

    Returns:
        Box: data from api
    """
    response = requests.get(url, params)
    data = Box(response.json())
    return data


def get_popular_videos(url:str, params:Box) -> List[Dict]:
    """get information of popular 10 videos at most from https://www.googleapis.com/youtube/v3/videos

    Args:
        url (str): url for api
        params (Box): parameters for api

    Returns:
        List[Dict]: data with video_id, title, thumbnail_url, category_id, view_count, comment_count, like_count, and published_at by video
    """
    data = get_data_from_api(url, params) 

    videos = list()
    for item in data.get("items", []):
        try:
            video_id = item.id
            video = {
                "video_id": video_id,
                "title": item.snippet.title,
                "thumbnail_url": item.snippet.thumbnails.standard.url,
                "category_id": item.snippet.categoryId,
                "view_count": item.statistics.viewCount,
                "comment_count": item.statistics.commentCount,
                "like_count": item.statistics.likeCount,
                "published_at": item.snippet.publishedAt
            }
            videos.append(video)
        except Exception as e:
            logging.warning(f'data is not empty but something wrong in {item}')
    
    return videos


def get_video_comments(list_video_id:List, url:str, params:Box) -> List[Dict]:
    """get information of 100 comments at most for each video from https://www.googleapis.com/youtube/v3/commentThreads

    Args:
        list_video_id (List): list of video id
        url (str): url for api
        params (Box): parameters for api

    Returns:
        List[Dict]: data with video_id, text_display, author_display_name, published_at, and like_count by video
    """
    comments = list()
    for video_id in list_video_id:
        params.videoId = video_id
        data = get_data_from_api(url, params)

        for item in data.get("items", []):
            try:
                comment = {
                    "video_id": video_id,
                    "text_display": item.snippet.topLevelComment.snippet.textDisplay,
                    "author_display_name": item.snippet.topLevelComment.snippet.authorDisplayName,
                    "published_at": item.snippet.topLevelComment.snippet.publishedAt,
                    "like_count": item.snippet.topLevelComment.snippet.likeCount
                }
                comments.append(comment)
            except Exception as e:
                logging.warning(f'data is not empty but something wrong in {item}')
    
    return comments


def get_video_categories(url: str, params: Box) -> List[Dict]:
    """get information of video categories from https://www.googleapis.com/youtube/v3/videoCategories

    Args:
        url (str): url for api
        params (Box): parameters for api

    Returns:
        List[Dict]: data with category_id and category_name
    """
    data = get_data_from_api(url, params)

    categories = list()
    for item in data.get("items", []):
        try:
            category = {
                "category_id": item.id,  # 카테고리 ID
                "category_name": item.snippet.title  # 카테고리 이름
            }
            categories.append(category)
        except Exception as e:
            logging.warning(f'data is not empty but something wrong in {item}')
    
    return categories


def get_noun(list_text:List[str]) -> List[str]:
    """get nouns from the list of text using Hannanum of konlpy

    Args:
        list_text (List[str]): list of text

    Returns:
        List[str]: list of cleaned nouns with length > 1
    """
    nouns = list()
    for text in list_text:
        nouns.extend(Hannanum().nouns(text))
    cleaned_nouns = [re.sub(r'\W+', '', noun) for noun in nouns]
    filtered_nouns = [noun for noun in cleaned_nouns if len(noun) > 1]
    return filtered_nouns