
import logging
import requests
from box import Box
from typing import List, Tuple

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, split, explode
from pyspark.sql.types import StringType 
from pyspark.sql.functions import current_timestamp, to_timestamp

from src.data_util import get_noun


class MLprocess:
    """ML process for sentiment analysis
    """

    def __init__(self, config_spark:Box, config_db:Box, config_analysis:Box) -> None:
        """initialize variables

        Args:
            config_spark (Box): configuration for spark
            config_db (Box): configuration for database
            config_analysis (Box): configuration for analysis
        """
        self.config_spark = config_spark
        self.config_db = config_db
        self.config_analysis = config_analysis
        self.spark = SparkSession.builder\
            .master(config_spark.master_url)\
            .config(config_spark.key_name_for_jar_package, config_spark.jar_package_name)\
            .config(config_spark.key_name_for_driver_path, config_spark.jar_file_address)\
            .appName(config_spark.app_name)\
            .getOrCreate()


    def download_input(self) -> DataFrame:
        """get data from database for ML

        Returns:
            DataFrame: text data as input of ML model
        """
        df = self.spark.read.jdbc(url=self.config_db.url,
                                  table=self.config_analysis.table.comment_table_name,
                                  properties=self.config_db.properties)

        logging.info("input downloaded")
        logging.info(df.schema)

        return df


    def predict_model(self, df:DataFrame) -> DataFrame:
        """predict sentiment and emotion

        Args:
            df (DataFrame): dataframe with input data of ML model

        Returns:
            DataFrame: dataframe with input and output data
        """
        # predict sentiment
        udf_execute_rest_api_sentiment = udf(
            lambda x: (requests.post("http://13.124.218.209:8000/sentiment", json=[x])).json()[0],
            StringType())
        df = df.withColumn(self.config_analysis.sentiment.name,
                           udf_execute_rest_api_sentiment(col(self.config_analysis.table.comment_text_column_name)))
        
        # predict emotion
        udf_execute_rest_api_emotion = udf(
            lambda x: (requests.post("http://13.124.218.209:8000/emotion", json=[x])).json()[0],
            StringType())
        df = df.withColumn(self.config_analysis.emotion.name,
                           udf_execute_rest_api_emotion(col(self.config_analysis.table.comment_text_column_name)))

        df = df.withColumn("updated_at", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        logging.info(df.show())
        logging.info(df.count())

        logging.info("model predicted")

        return df


    def upload_output(self, df:DataFrame) -> None:
        """upload data from ML model and word tokenization to database

        Args:
            df (DataFrame): dataframe with data from ML model
        """
        df = self.spark.createDataFrame(df.toPandas())
        df.write.jdbc(url=self.config_db.url,
                      table=self.config_analysis.table.comment_table_name,
                      mode="overwrite",
                      properties=self.config_db.properties)
        
        # word tokenization
        list_comment_text = df.select(self.config_analysis.table.comment_text_column_name)\
            .rdd.flatMap(lambda x: x).collect()
        comment_nouns = get_noun(list_comment_text)

        df = self.spark.read.jdbc(url=self.config_db.url,
                                  table=self.config_analysis.table.video_table_name,
                                  properties=self.config_db.properties)

        list_title_text = df.select(self.config_analysis.table.video_text_column_name)\
            .rdd.flatMap(lambda x: x).collect()
        title_nouns = get_noun(list_title_text)

        df_comment_nouns = self.spark.createDataFrame(comment_nouns, StringType())\
            .withColumnRenamed("value", self.config_analysis.table.comment_word_column_name)
        df_title_nouns = self.spark.createDataFrame(title_nouns, StringType())\
            .withColumnRenamed("value", self.config_analysis.table.video_word_column_name)
        
        df = df_comment_nouns.unionByName(df_title_nouns, allowMissingColumns=True)\
            .withColumn("created_at", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))\
            .withColumn("updated_at", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        df.write.jdbc(url=self.config_db.url,
                      table=self.config_analysis.table.word_token_table_name,
                      mode="overwrite",
                      properties=self.config_db.properties)
        
        logging.info("output uploaded")
    


    










