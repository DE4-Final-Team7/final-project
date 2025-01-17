
import logging
from box import Box

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType 
from pyspark.sql.functions import current_timestamp, to_timestamp

from data_util import get_noun


def tokenize_text(config_spark:Box, config_db:Box, config_analysis:Box) -> None:
    """tokenize text for word cloud 

    Args:
        config_spark (Box): configuration for spark
        config_db (Box): configuration for database
        config_analysis (Box): configuration for analysis
    """
    spark = SparkSession.builder\
            .master(config_spark.master_url)\
            .config(config_spark.key_name_for_jar_package, config_spark.jar_package_name)\
            .config(config_spark.key_name_for_driver_path, config_spark.jar_file_address)\
            .appName(config_spark.app_name)\
            .getOrCreate()

    df_title = spark.read.jdbc(url=config_db.url,
                               table=config_analysis.table.video_table_name,
                               properties=config_db.properties)

    list_title_text = df_title.select(config_analysis.table.video_text_column_name)\
        .rdd.flatMap(lambda x: x).collect()
    title_nouns = get_noun(list_title_text)

    logging.info("title")

    df_comment = spark.read.jdbc(url=config_db.url,
                                 table=config_analysis.table.comment_table_name,
                                 properties=config_db.properties)

    list_comment_text = df_comment.select(config_analysis.table.comment_text_column_name)\
        .rdd.flatMap(lambda x: x).collect()
    comment_nouns = get_noun(list_comment_text)

    logging.info("comment")

    df_comment_nouns = spark.createDataFrame(comment_nouns, StringType())\
        .withColumnRenamed("value", config_analysis.table.comment_word_column_name)
    df_title_nouns = spark.createDataFrame(title_nouns, StringType())\
        .withColumnRenamed("value", config_analysis.table.video_word_column_name)
    
    df_word_token = df_comment_nouns.unionByName(df_title_nouns, allowMissingColumns=True)\
        .withColumn("created_at", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))\
        .withColumn("updated_at", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    df_word_token.write.jdbc(url=config_db.url,
                    table=config_analysis.table.word_token_table_name,
                    mode="overwrite",
                    properties=config_db.properties)