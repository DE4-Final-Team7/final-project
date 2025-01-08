
import logging
from box import Box
from typing import Dict, List
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import to_timestamp

from src.data_util import get_popular_videos, get_video_comments, get_video_categories


def extract(config_api:Box) -> Dict[str, List[Dict]]:
    """_summary_

    Args:
        config_api (Box): _description_

    Returns:
        Dict[str, List[Dict]]: _description_
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

    # catergory
    # 카테고리 데이터를 추출하여 extracted_data에 추가하는 코드
    extracted_data[config_api.category.name] = get_video_categories(config_api.category.url,
                                                                    config_api.category.params)

    logging.info("data extracted")
    
    return extracted_data


def transform(extracted_data: Dict[str, List[Dict]], config_spark:Box) -> Dict[str, List[DataFrame]]:
    """_summary_

    Args:
        extracted_data (Dict[str, List[Dict]]): _description_
        config_spark (Box): _description_

    Returns:
        Dict[str, List[DataFrame]]: _description_
    """
    spark = SparkSession.builder\
        .master(config_spark.master_url)\
        .config(config_spark.key_name_for_jar_package, config_spark.jar_package_name)\
        .config(config_spark.key_name_for_driver_path, config_spark.jar_file_address)\
        .appName(config_spark.app_name)\
        .getOrCreate()
    
    # create dataframe
    df_popular_videos = spark.createDataFrame(extracted_data[config_spark.video_data_name])
    df_video_comments = spark.createDataFrame(extracted_data[config_spark.comment_data_name])
    df_video_categories = spark.createDataFrame(extracted_data[config_spark.category_data_name])

    # modify dataframe
    transformed_data  = dict()
    transformed_data[config_spark.video_df_name] = df_popular_videos\
        .withColumn("published_at", to_timestamp(df_popular_videos.published_at))\
        .withColumn("created_at", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))\
        .withColumn("updated_at", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    transformed_data[config_spark.comment_df_name] = df_video_comments\
        .withColumn("published_at", to_timestamp(df_video_comments.published_at))\
        .withColumn("created_at", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))\
        .withColumn("updated_at", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    transformed_data[config_spark.category_df_name] = df_video_categories\
        .withColumn("created_at", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))\
        .withColumn("updated_at", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    
    logging.info("data transformed")
    for k, v in transformed_data.items():
        logging.info(k)
        logging.info(v.schema)
    
    return transformed_data


def load(transformed_data:Dict[str, List[DataFrame]], config_db:Box) -> None:
    """_summary_

    Args:
        transformed_data (Dict[str, List[DataFrame]]): _description_
        config_db (Box): _description_
    """
    transformed_data[config_db.video_df_name].write.jdbc(url=config_db.url,
                                                         table=config_db.video_table_name,
                                                         mode="append",
                                                         properties=config_db.properties)
    transformed_data[config_db.comment_df_name].write.jdbc(url=config_db.url,
                                                           table=config_db.comment_table_name,
                                                           mode="append",
                                                           properties=config_db.properties)
    transformed_data[config_db.category_df_name].write.jdbc(url=config_db.url,
                                                           table=config_db.category_table_name,
                                                           mode="ignore",
                                                           properties=config_db.properties)
    
    logging.info("data loaded")
