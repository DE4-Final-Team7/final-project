
import pytest
import os
from box import Box
from src.data_util import get_popular_videos


def test_popular_video():
    config = \
    {
        "api":
        {
            "video":
            {
                "name": "popular_videos",
                "url": "https://www.googleapis.com/youtube/v3/videos",
                "params":
                {
                    "part": "snippet, statistics",
                    "chart": "mostPopular",
                    "maxResults": "10",
                    "regionCode": "kr"
                }
            }
        }
    }
    config = Box(config)
    config.api.video.params.key = os.environ.get("api_key")
    data = get_popular_videos(config.api.video.url, config.api.video.params)
    assert len(data) > 0


