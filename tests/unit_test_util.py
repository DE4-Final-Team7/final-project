import pytest
import os
from box import Box
from src.data_util import get_data_from_api, get_noun


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
    config.api.video.params.key = os.environ.get("api_key")
    data = get_data_from_api(config.api.video.url, config.api.video.params)
    assert len(data.get("items")) == int(config.api.video.params.maxResults)    

def test_noun():
    assert get_noun(["명사"]) == ["명사"]

