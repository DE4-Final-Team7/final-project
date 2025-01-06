import pytest
from sentiment_analysis import preprocess_text, analyze_sentiment, analyze_emotion, main

@pytest.mark.parametrize(
    "text, expected_sentiment, expected_emotion",
    [
        ("I loved every second of this amazing movie.", "Positive", "Joy"),
        ("This was a complete waste of time, absolutely horrible.", "Negative", "Disgust")
    ]
)
def test_analyze_sentiment_and_emotion(text, expected_sentiment, expected_emotion):
    """
    Tests the analyze_sentiment and analyze_emotion functions by comparing the predicted
    sentiment and emotion against the expected values.
    """
    predicted_sentiment = analyze_sentiment(text)
    predicted_emotion = analyze_emotion(text)
    
    assert predicted_sentiment == expected_sentiment, (
        f"Expected sentiment: {expected_sentiment}, but got: {predicted_sentiment}"
    )
    assert predicted_emotion == expected_emotion, (
        f"Expected emotion: {expected_emotion}, but got: {predicted_emotion}"
    )


@pytest.mark.parametrize(
    "input_text, expected_output",
    [
        ("<p>Hello, World!</p>", "Hello, World!"),
        ("&#39;Hello&#39;", "Hello"),
        ("이 영화 정말 최고였어요! <br>", "이 영화 정말 최고였어요!"),
        (None, "")
    ]
)
def test_preprocess_text(input_text, expected_output):
    """
    Tests the preprocess_text function by comparing the output of preprocessing
    against the expected cleaned text.
    """
    result = preprocess_text(input_text)
    assert result == expected_output, f"Expected {expected_output}, but got {result}"


def test_main_function():
    """
    Tests the main function by analyzing a list of sample texts and comparing the
    resulting DataFrame against the expected sentiments and emotions.
    """
    sample_texts = [
        "I loved every second of this amazing movie.",
        "This was a complete waste of time, absolutely horrible."
    ]
    
    expected_sentiments = ["Positive", "Negative"]
    expected_emotions = ["Joy", "Disgust"]
    
    result_df = main(sample_texts)
    
    # Check if the length of the result matches the length of the input
    assert len(result_df) == len(sample_texts)
    
    # Check if the predicted sentiments and emotions match the expected values
    for i, row in result_df.iterrows():
        assert row["sentiment"] == expected_sentiments[i], (
            f"Expected sentiment: {expected_sentiments[i]}, but got: {row['sentiment']}"
        )
        assert row["detailed_emotion"] == expected_emotions[i], (
            f"Expected emotion: {expected_emotions[i]}, but got: {row['detailed_emotion']}"
        )
