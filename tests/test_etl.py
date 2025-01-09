
import pytest
from box import Box
from src.data_util import get_data_from_api


def test_api():
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
    
    data = get_data_from_api(config.api.video.url, config.api.video.params)
    assert len(data.get("items")) == config.api.video.params.maxResults

