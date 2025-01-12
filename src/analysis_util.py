
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification


def get_summarization_pipeline() -> pipeline:
    """
    initializes and returns the summarization pipeline

    Returns:
        pipeline: initialized summarization pipeline
    """
    return pipeline("summarization", model="sshleifer/distilbart-cnn-12-6")


def get_sentiment_pipeline() -> pipeline:
    """
    initializes and returns a multilingual sentiment analysis pipeline using XLM-RoBERTa

    Returns:
        pipeline: initialized sentiment analysis pipeline
    """
    tokenizer = AutoTokenizer.from_pretrained(
        "cardiffnlp/twitter-xlm-roberta-base-sentiment",
        use_fast=True
    )
    model = AutoModelForSequenceClassification.from_pretrained("cardiffnlp/twitter-xlm-roberta-base-sentiment")
    return pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)


def get_emotion_pipeline() -> pipeline:
    """
    initializes and returns the emotion classification pipeline using GoEmotions

    Returns:
        pipeline: initialized emotion classification pipeline
    """
    return pipeline(
        "text-classification",
        model="bhadresh-savani/bert-base-go-emotion",
        top_k=None
    )
