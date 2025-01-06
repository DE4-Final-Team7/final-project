import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
from dotenv import load_dotenv
import os
import re

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
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
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

    text = re.sub(r'<[^>]+>', '', text)  # HTML 태그 제거
    text = re.sub(r'&#[0-9]+;', '', text)  # 유니코드 정규화
    return text.strip()


def get_sentiment_pipeline() -> pipeline:
    """
    Initializes and returns a multilingual sentiment analysis pipeline using XLM-RoBERTa.

    Returns:
        pipeline: The initialized sentiment analysis pipeline.
    """
    tokenizer = AutoTokenizer.from_pretrained(
        "cardiffnlp/twitter-xlm-roberta-base-sentiment", use_fast=False
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
        top_k=None
    )


def get_summarization_pipeline() -> pipeline:
    """
    Initializes and returns the summarization pipeline.

    Returns:
        pipeline: The initialized summarization pipeline.
    """
    return pipeline("summarization", model="sshleifer/distilbart-cnn-12-6")


def analyze_sentiment(text: str) -> str:
    """
    Preprocesses, summarizes, and analyzes the sentiment of a given text.

    Args:
        text (str): The input text to analyze.

    Returns:
        str: The predicted sentiment label (Positive/Negative/Neutral).
    """
    summarization_pipeline = get_summarization_pipeline()
    sentiment_pipeline = get_sentiment_pipeline()

    preprocessed_text = preprocess_text(text)
    summary = summarization_pipeline(preprocessed_text)
    summarized_text = summary[0]['summary_text']

    result = sentiment_pipeline(summarized_text, truncation=True)[0]['label']
    return result.capitalize()


def analyze_emotion(text: str) -> str:
    """
    Preprocesses, summarizes, and analyzes the detailed emotions of a given text.

    Args:
        text (str): The input text to analyze.

    Returns:
        str: The emotion label with the highest score.
    """
    summarization_pipeline = get_summarization_pipeline()
    emotion_pipeline = get_emotion_pipeline()

    preprocessed_text = preprocess_text(text)
    summary = summarization_pipeline(preprocessed_text)
    summarized_text = summary[0]['summary_text']

    result = emotion_pipeline(summarized_text, truncation=True)
    emotion_scores = {res['label'].lower(): res['score'] for res in result[0]}
    dominant_emotion = max(emotion_scores, key=emotion_scores.get)

    return dominant_emotion.capitalize()


def main(text_list: list) -> pd.DataFrame:
    """
    Analyzes a list of texts and returns a DataFrame with sentiment and emotion results.

    Args:
        text_list (list): A list of input texts to analyze.

    Returns:
        pd.DataFrame: A DataFrame containing the original texts, sentiment labels, and detailed emotions.
    """
    sentiments = [analyze_sentiment(text) for text in text_list]
    emotions = [analyze_emotion(text) for text in text_list]

    result_df = pd.DataFrame({
        "sentiment": sentiments,
        "detailed_emotion": emotions
    })
    return result_df


if __name__ == "__main__":
    query = "SELECT * FROM public.comment_analysis LIMIT 5;"
    df = fetch_data_from_db(query)
    print(df)
    
    if not df.empty:
        text_list = df['text_display'].tolist()
        result_df = main(text_list)
        print(result_df)
    else:
        logging.info("No data fetched from the database.")
