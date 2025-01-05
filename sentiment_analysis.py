import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
from dotenv import load_dotenv
import os
import re
from typing import List

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# .env 파일 로드
load_dotenv()


def fetch_data_from_db(query: str) -> pd.DataFrame:
    """
    Fetches data from a PostgreSQL database and returns it as a DataFrame.

    Args:
        query (str): The SQL query to execute.

    Returns:
        pd.DataFrame: A DataFrame containing the query results. If an error occurs, returns an empty DataFrame.
    """
    conn = None
    cursor = None
    try:
        db_config = {
            "dbname": os.getenv("DB_NAME"),
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD")
        }
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        results = cursor.fetchall()
        logging.info("Query executed successfully. Rows fetched: %d", len(results))
        return pd.DataFrame(results)
    except Exception as e:
        logging.error("Error during query execution: %s", e)
        return pd.DataFrame()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logging.info("Database connection closed.")


def preprocess_text(text: str) -> str:
    """
    Preprocesses a given text by removing HTML tags and normalizing unicode characters.

    Args:
        text (str): The input text to preprocess.

    Returns:
        str: A cleaned version of the input text. If the input is NaN, returns an empty string.
    """
    if pd.isna(text):
        return ''

    # HTML 태그 제거
    text = re.sub(r'<[^>]+>', '', text)
    
    # 유니코드 정규화 (예: &#39; → ')
    text = re.sub(r'&#[0-9]+;', '', text)

    return text.strip()


def get_sentiment_pipeline() -> pipeline:
    """
    Initializes and returns a multilingual sentiment analysis pipeline using XLM-RoBERTa.

    Returns:
        pipeline: The initialized sentiment analysis pipeline.
    """
    tokenizer = AutoTokenizer.from_pretrained(
        "cardiffnlp/twitter-xlm-roberta-base-sentiment", use_fast=True
    )
    model = AutoModelForSequenceClassification.from_pretrained("cardiffnlp/twitter-xlm-roberta-base-sentiment")
    return pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)


def get_emotion_pipeline() -> pipeline:
    """
    Initializes and returns the emotion classification pipeline using GoEmotions.

    Returns:
        pipeline: The initialized emotion classification pipeline.
    """
    return pipeline(
        "text-classification",
        model="bhadresh-savani/bert-base-go-emotion",
        top_k=None,
        truncation=True
    )


def get_summarization_pipeline() -> pipeline:
    """
    Initializes and returns the summarization pipeline.

    Returns:
        pipeline: The initialized summarization pipeline.
    """
    return pipeline("summarization", model="sshleifer/distilbart-cnn-12-6")


def summarize_text(text: str) -> str:
    """
    Summarizes the input text using a pre-trained summarization model.

    Args:
        text (str): The input text to summarize.

    Returns:
        str: A summarized version of the input text. If the input length is within the limit, returns the input text.
    """
    summarization_pipeline = get_summarization_pipeline()
    preprocessed_text = preprocess_text(text)
    max_token_limit = 512
    input_length = len(text.split())
    if input_length <= max_token_limit:
        return preprocessed_text
    max_len = max_token_limit - 10
    summary = summarization_pipeline(preprocessed_text, max_length=max_len, do_sample=False, truncation=True)
    return summary[0]['summary_text']


def analyze_sentiment(text: str) -> str:
    """
    Analyzes the sentiment of a given text in multiple languages.

    Args:
        text (str): The input text to analyze.

    Returns:
        str: The predicted sentiment label (Positive/Negative/Neutral).
    """
    sentiment_pipeline = get_sentiment_pipeline()
    preprocessed_text = preprocess_text(text)
    result = sentiment_pipeline(preprocessed_text, truncation=True, max_length=256)[0]['label']
    return result.capitalize()


def analyze_emotion(text: str, sentiment: str) -> str:
    """
    Analyzes detailed emotions of a given text and returns the highest scoring emotion.

    Args:
        text (str): The input text to analyze.
        sentiment (str): The sentiment label (Positive/Negative/Neutral) of the text.

    Returns:
        str: The emotion label with the highest score.
    """
    emotion_pipeline = get_emotion_pipeline()
    preprocessed_text = preprocess_text(text)
    result = emotion_pipeline(preprocessed_text, truncation=True, max_length=256)
    emotion_scores = {res['label'].lower(): res['score'] for res in result[0]}

    grouped_emotions = {
        'happiness': ['amusement', 'excitement', 'joy', 'love'],
        'approval': ['admiration', 'approval'],
        'sadness': ['disappointment', 'grief', 'remorse', 'sadness'],
        'anger': ['anger', 'annoyance', 'disapproval', 'disgust'],
        'anxiety': ['fear', 'nervousness', 'embarrassment'],
        'surprise': ['surprise', 'curiosity', 'realization'],
        'satisfaction': ['gratitude', 'pride', 'relief', 'desire', 'caring']
    }

    category_scores = {
        category: sum(emotion_scores.get(emotion, 0) for emotion in emotions)
        for category, emotions in grouped_emotions.items()
    }

    dominant_emotion = max(category_scores, key=category_scores.get)
    return dominant_emotion.capitalize()


def main(text_list: List[str]) -> pd.DataFrame:
    """
    Analyzes a list of texts and returns a DataFrame with results.

    Args:
        text_list (List[str]): A list of input texts to analyze.

    Returns:
        pd.DataFrame: A DataFrame containing the original texts, sentiments, and detailed emotions.
    """
    # summarize_text의 결과를 직접 감성 분석에 사용
    sentiments = [analyze_sentiment(summarize_text(text)) for text in text_list]
    emotions = [analyze_emotion(summarize_text(text), sentiment) for text, sentiment in zip(text_list, sentiments)]

    result_df = pd.DataFrame({
        "sentiment": sentiments,   # 감성 레이블
        "detailed_emotion": emotions  # 세부 감정
    })
    return result_df



if __name__ == "__main__":
    query = "SELECT * FROM public.video_comment LIMIT 5;"
    df = fetch_data_from_db(query)

    # 텍스트 컬럼 추출 후 리스트로 변환
    text_list = df['text_display'].tolist()

    # 감정 분석 수행
    result_df = main(text_list)
