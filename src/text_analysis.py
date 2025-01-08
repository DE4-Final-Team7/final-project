
import logging
import re
import pandas as pd
from transformers import pipeline
from typing import List

from src.analysis_util import get_summarization_pipeline, get_sentiment_pipeline, get_emotion_pipeline


class TextAnalysis:
    """_summary_
    """


    def __init__(self):
        """_summary_
        """
        self.summarization_pipeline = get_summarization_pipeline()
        self.sentiment_pipeline = get_sentiment_pipeline()
        self.emotion_pipeline = get_emotion_pipeline()
    

    def preprocess_text(self, list_text: List[str]) -> List[str]:
        """
        Preprocesses a given text by removing HTML tags and normalizing unicode characters.

        Args:
            text (str): The input text to preprocess.

        Returns:
            str: A cleaned version of the input text. If the input is NaN, returns an empty string.
        """
        result = list()
        for text in list_text:
            if pd.isna(text):
                return ''
            # HTML 태그 제거
            text = re.sub(r'<[^>]+>', '', text)
            # 유니코드 정규화 (예: &#39; → ')
            text = re.sub(r'&#[0-9]+;', '', text)
            result.append(text.strip())

        return result


    def summarize_text(self, list_text: List[str], max_input_text_len:int=512, max_output_text_len:int=256) -> List[str]:
        """
        Summarizes the input text using a pre-trained summarization model.

        Args:
            text (str): The input text to summarize.

        Returns:
            str: A summarized version of the input text. If the input length is within the limit, returns the input text.
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
        """
        Analyzes the sentiment of a given text in multiple languages.

        Args:
            text (str): The input text to analyze.

        Returns:
            str: The predicted sentiment label (Positive/Negative/Neutral).
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
        """
        Analyzes detailed emotions of a given text and returns the highest scoring emotion.

        Args:
            text (str): The input text to analyze.
            sentiment (str): The sentiment label (Positive/Negative/Neutral) of the text.

        Returns:
            str: The emotion label with the highest score.
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



