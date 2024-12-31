
import logging
from box import Box
from typing import Dict, List
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import date_format

from src.get_data import get_popular_videos, get_video_comments


def extract(config_api:Box) -> Dict[List[Dict], List[Dict]]:
    """_summary_

    Args:
        config_api (Box): _description_

    Returns:
        Dict[List[Dict], List[Dict]]: _description_
    """
    extracted_data = dict()
    
    # video
    extracted_data[config_api.video.name] = get_popular_videos(config_api.video.url,
                                                               config_api.video.params)
    list_video_id = list()
    for video in extracted_data[config_api.video.name]:
        list_video_id.append(video["video_id"])
    
    # comment
    extracted_data[config_api.comment.name] = get_video_comments(list_video_id,
                                                                 config_api.comment.url,
                                                                 config_api.comment.params)

    logging.info("data extracted")
    
    return extracted_data


def transform(extracted_data:Dict[List[Dict], List[Dict]], config_spark:Box) -> Dict[List[Dict], List[Dict]]:
    """_summary_

    Args:
        extracted_data (Dict[List[Dict], List[Dict]]): _description_
        config_spark (Box): _description_

    Returns:
        Dict[List[Dict], List[Dict]]: _description_
    """
    spark = SparkSession.builder\
        .master(config_spark.master_url)\
        .config(config_spark.key_name_for_jar_file, config_spark.jar_file_address)\
        .appName(config_spark.app_name)\
        .getOrCreate()
    
    # create dataframe
    df_popular_videos = spark.createDataFrame(extracted_data[config_spark.video_data_name])
    df_video_comments = spark.createDataFrame(extracted_data[config_spark.comment_data_name])

    # modify dataframe
    transformed_data  = dict()
    transformed_data[config_spark.video_df_name] = df_popular_videos\
        .withColumn("published_at", date_format(df_popular_videos.published_at, "yyyy-MM-dd HH:mm:ss"))\
        .withColumn("created_at", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))\
        .withColumn("updated_at", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    transformed_data[config_spark.comment_df_name] = df_video_comments\
        .withColumn("published_at", date_format(df_video_comments.published_at, "yyyy-MM-dd HH:mm:ss"))\
        .withColumn("created_at", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))\
        .withColumn("updated_at", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    
    logging.info("data transformed")
    for k, v in transformed_data.items():
        logging.info(k)
        logging.info(v.schema)
    
    return transformed_data


def load(transformed_data:Dict[List[Dict], List[Dict]], config_db:Box) -> None:
    """_summary_

    Args:
        transformed_data (Dict[List[Dict], List[Dict]]): _description_
        config_db (Box): _description_
    """
    # properties = {"user":"admin", "password":"admin4321", "driver":"org.postgresql.Driver"}

    transformed_data[config_db.video_df_name].write.jdbc(url=config_db.url,
                                                         table=config_db.video_table_name,
                                                         mode="append",
                                                         properties=config_db.properties)
    transformed_data[config_db.comment_df_name].write.jdbc(url=config_db.url,
                                                           table=config_db.comment_table_name,
                                                           mode="append",
                                                           properties=config_db.properties)
    
    logging.info("data loaded")
