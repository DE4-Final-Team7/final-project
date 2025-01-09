
import pytest
from src.text_analysis import TextAnalysis

text_analysis = TextAnalysis()


def test_sentiment():
    summarized_text = text_analysis.summarize_text(["I loved every second of this amazing movie."])
    assert text_analysis.analyze_sentiment(summarized_text) == "Positive"

def test_emotion():
    summarized_text = text_analysis.summarize_text(["I loved every second of this amazing movie."])
    assert text_analysis.analyze_emotion(summarized_text) == "Love"

