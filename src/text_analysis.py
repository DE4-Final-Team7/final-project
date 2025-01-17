
import logging
import re
import pandas as pd
from transformers import pipeline
from typing import List

from analysis_util import get_summarization_pipeline, get_sentiment_pipeline, get_emotion_pipeline


class TextAnalysis:
    """analyze text
    """


    def __init__(self) -> None:
        """initialize variables
        """
        self.summarization_pipeline = get_summarization_pipeline()
        self.sentiment_pipeline = get_sentiment_pipeline()
        self.emotion_pipeline = get_emotion_pipeline()
    

    def preprocess_text(self, list_text: List[str]) -> List[str]:
        """
        preprocesses a given text by removing HTML tags and normalizing unicode characters.

        Args:
            list_text (List[str]): input text to preprocess

        Returns:
            List[str]: cleaned version of the input text, if the input is NaN, returns an empty string
        """
        result = list()
        for text in list_text:
            if pd.isna(text):
                return ''
            # removing HTML tags
            text = re.sub(r'<[^>]+>', '', text)
            # normalizing unicode characters
            text = re.sub(r'&#[0-9]+;', '', text)
            result.append(text.strip())

        return result


    def summarize_text(self, list_text: List[str], max_input_text_len:int=512, max_output_text_len:int=256) -> List[str]:
        """summarizes the input text using a pre-trained summarization model

        Args:
            list_text (List[str]): input text to summarize
            max_input_text_len (int, optional): maximum length of input length. Defaults to 512.
            max_output_text_len (int, optional): maximum length of output length. Defaults to 256.

        Returns:
            List[str]: summarized version of the input text, if the input length is less than or equal to the limit, returns the original input text
        """
        list_preprocessed_text = self.preprocess_text(list_text)
        result = list()
        for text in list_preprocessed_text:
            try:
                input_length = len(text.split())
                if input_length <= max_input_text_len:
                    result.append(text)
                    continue
                summary_result = self.summarization_pipeline(text, max_length=max_output_text_len, do_sample=False, truncation=True)
                result.append(summary_result[0]['summary_text'])
            except Exception as e:
                result.append(None)
                logging.warning(f'data is not empty but something wrong in {text}')

        return result


    def analyze_sentiment(self, list_text: List[str]) -> List[str]:
        """analyzes the sentiment of a given text in multiple languages

        Args:
            list_text (List[str]): input text to analyze

        Returns:
            List[str]: sentiment label (Positive/Negative/Neutral)
        """
        result = list()
        for text in list_text:
            try:
                sentiment_result = self.sentiment_pipeline(text, truncation=True)[0]['label']
                result.append(sentiment_result.capitalize())
            except Exception as e:
                result.append(None)
                logging.warning(f'data is not empty but something wrong in {text}')

        return result


    def analyze_emotion(self, list_text: List[str]) -> List[str]:
        """analyze detailed emotions of a given text and returns emotion with the highest score

        Args:
            list_text (List[str]): input text to analyze

        Returns:
            List[str]: emotion label with the highest score
        """
        result = list()
        for text in list_text:
            try:
                emotions = self.emotion_pipeline(text, truncation=True)
                emotion_scores = {res['label'].lower(): res['score'] for res in emotions[0]}
                emotion_result = max(emotion_scores, key=emotion_scores.get)
                result.append(emotion_result.capitalize())
            except Exception as e:
                result.append(None)
                logging.warning(f'data is not empty but something wrong in {text}')

        return result



