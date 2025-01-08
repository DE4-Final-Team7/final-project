
import logging
import requests
from box import Box
from typing import List, Tuple

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType 
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import to_timestamp


class MLprocess:
    """_summary_
    """

    def __init__(self, config_spark:Box, config_db:Box, config_analysis:Box):
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
        """_summary_

        Returns:
            Tuple[DataFrame, List[str]]: _description_
        """

        df = self.spark.read.jdbc(url=self.config_db.url,
                                  table=self.config_analysis.table.table_name,
                                  properties=self.config_db.properties)

        logging.info("input downloaded")
        logging.info(df.schema)

        return df


    def predict_model(self, df) -> DataFrame:
        """_summary_

        Args:
            df (_type_): _description_
            text (List[str]): _description_

        Returns:
            DataFrame: _description_
        """
        # predict sentiment
        udf_execute_rest_api_sentiment = udf(
            lambda x: (requests.post("http://13.124.218.209:8000/sentiment", json=[x])).json()[0],
            StringType())
        df = df.withColumn(self.config_analysis.sentiment.name,
                           udf_execute_rest_api_sentiment(col(self.config_analysis.table.column_name)))
        
        # predict emotion
        udf_execute_rest_api_emotion = udf(
            lambda x: (requests.post("http://13.124.218.209:8000/emotion", json=[x])).json()[0],
            StringType())
        df = df.withColumn(self.config_analysis.emotion.name,
                           udf_execute_rest_api_emotion(col(self.config_analysis.table.column_name)))

        df = df.withColumn("updated_at", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        logging.info(df.show())
        logging.info(df.count())

        logging.info("model predicted")

        return df


    def upload_output(self, df:DataFrame) -> None:
        """_summary_

        Args:
            df (DataFrame): _description_
        """
        df = self.spark.createDataFrame(df.toPandas())
        df.write.jdbc(url=self.config_db.url,
                      table=self.config_analysis.table.table_name,
                      mode="overwrite",
                      properties=self.config_db.properties)
        
        logging.info("output uploaded")
    


    










