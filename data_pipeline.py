
import logging
from box import Box
from src.etl_data import extract, transform, load



config_api =\
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
            "regionCode": "kr",
        }
    },
    "comment":
    {
        "name": "video_comments",
        "url": "https://www.googleapis.com/youtube/v3/commentThreads",
        "params":
        {
            "part": "snippet",
            "maxResults": "10",
            "order": "relevance"
        }

    }
}


config_spark =\
{
    "master_url": "local[*]",
    "key_name_for_jar_file": "spark.jars",
    "jar_file_address": "res/postgresql-42.7.4.jar",
    "app_name": "ETL pipeline",
    "video_data_name": "popular_videos",
    "comment_data_name": "video_comments",
    "video_df_name": "df_popular_videos",
    "comment_df_name": "df_video_comments",
}



config_db =\
{
    "video_table_name": "popular_video",
    "comment_table_name": "video_comment",
    "video_df_name": "df_popular_videos",
    "comment_df_name": "df_video_comments",
    "properties":
    {
        "driver":"org.postgresql.Driver"
    }

}


config_api = Box(config_api)
config_api.video.params.key = "AIzaSyAuqFmlvxc_t07b24Zbn3lM13yshFucrpA"
config_api.comment.params.key = "AIzaSyAuqFmlvxc_t07b24Zbn3lM13yshFucrpA"


config_spark = Box(config_spark)


config_db = Box(config_db)
config_db.url = "jdbc:postgresql://13.124.218.209:5432/test"
config_db.properties.user = "admin"
config_db.properties.password = "admin4321"



logging.basicConfig(level = logging.INFO)


extracted_data = extract(config_api)
transformed_data = transform(extracted_data, config_spark)
load(transformed_data, config_db)


