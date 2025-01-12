

import os
import logging
from box import Box
from src.etl_data import extract, transform, load
from src.ml_process import MLprocess


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


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
        },
        "comment":
        {
            "name": "video_comments",
            "url": "https://www.googleapis.com/youtube/v3/commentThreads",
            "params":
            {
                "part": "snippet",
                "maxResults": "100",
                "order": "relevance"
            }
        },
        "category":
        {
            "name": "video_categories",
            "url": "https://www.googleapis.com/youtube/v3/videoCategories",
            "params":
            {
                "part": "snippet",
                "regionCode": "kr"
            }
        }
    },
    "spark":
    {
        "master_url": "local[*]",
        "key_name_for_jar_package": "spark.jars.packages",
        "jar_package_name": "org.postgresql:postgresql:42.7.4",
        "key_name_for_driver_path": "spark.driver.extraClassPath",
        "jar_file_address": "res/postgresql-42.7.4.jar",
        "app_name": "ETL pipeline",
        "video_data_name": "popular_videos",
        "comment_data_name": "video_comments",
        "category_data_name": "video_categories",
        "video_df_name": "df_popular_videos",
        "comment_df_name": "df_video_comments",
        "category_df_name": "df_video_categories"
    },
    "db":
    {
        "video_table_name": "popular_video",
        "comment_table_name": "video_comment",
        "category_table_name": "video_category",
        "video_df_name": "df_popular_videos",
        "comment_df_name": "df_video_comments",
        "category_df_name": "df_video_categories",
        "properties":
        {
            "driver": "org.postgresql.Driver"
        }
    },
    "dbt":
    {
        "path": "/var/lib/airflow/dags/dbt_dev"
    },
    "analysis":
    {
        "table":
        {
            "comment_table_name": "comment_analysis",
            "comment_text_column_name": "text_display",
            "comment_word_column_name": "comment_word",
            "video_table_name": "video_metadata",
            "video_text_column_name": "title",
            "video_word_column_name": "title_word",
            "word_token_table_name": "word_token"
        },
        "sentiment":
        {
            "name": "sentiment",
            "url": "http://13.124.218.209:8000/sentiment"
        },
        "emotion":
        {
            "name": "emotion",
            "url": "http://13.124.218.209:8000/emotion"
        }
    }
}


db_info = \
{
    "url": "jdbc:postgresql://13.124.218.209:5432/dev",
    "user":"admin",
    "password":"admin4321"
}

config = Box(config)
db_info = Box(db_info)

config.api.video.params.key = "AIzaSyCIGUsELvtNd7RjL0LzXzJGiG5j3hJQu0A"
config.api.comment.params.key = "AIzaSyCIGUsELvtNd7RjL0LzXzJGiG5j3hJQu0A"
config.api.category.params.key = "AIzaSyCIGUsELvtNd7RjL0LzXzJGiG5j3hJQu0A"

config.db.url = db_info.url
config.db.properties.user = db_info.user
config.db.properties.password = db_info.password





def run_etl(config):
    extracted_data = extract(config.api)
    transformed_data = transform(extracted_data, config.spark)
    load(transformed_data, config.db)


def run_ml(config):
    ml_model = MLprocess(config.spark,
                         config.db,
                         config.analysis)
    df = ml_model.download_input()
    df = ml_model.predict_model(df)
    ml_model.upload_output(df)





# run_etl(config)
# os.system("cd /var/lib/airflow/dags/dbt_dev && dbt run --full-refresh && dbt docs generate")
run_ml(config)







