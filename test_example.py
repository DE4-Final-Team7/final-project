import pytest
from sentiment_analysis import preprocess_text, summarize_text, analyze_sentiment, analyze_emotion, main

@pytest.mark.parametrize(
    "text, expected_sentiment, expected_emotion",
    [
        ("I loved every second of this amazing movie.", "Positive", "Happiness"),
        ("This was a complete waste of time, absolutely horrible.", "Negative", "Anger"),
        ("이 선수, 런던 올림픽 4강 영웅 이범영 선수가 맞습니다!!!", "Positive", "Approval")
    ]
)
def test_analyze_sentiment_and_emotion(text, expected_sentiment, expected_emotion):
    predicted_sentiment = analyze_sentiment(text)
    predicted_emotion = analyze_emotion(text, predicted_sentiment)
    
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
    result = preprocess_text(input_text)
    assert result == expected_output, f"Expected {expected_output}, but got {result}"


def test_summarize_text():
    short_text = "This is a short text."
    long_text = " ".join(["This is a long text."] * 150)

    assert summarize_text(short_text) == short_text
    assert len(summarize_text(long_text).split()) <= 502


def test_main_function():
    sample_texts = [
        "I loved every second of this amazing movie.",
        "This was a complete waste of time, absolutely horrible.",
        "이 선수, 런던 올림픽 4강 영웅 이범영 선수가 맞습니다!!!"
    ]
    
    expected_sentiments = ["Positive", "Negative", "Positive"]
    expected_emotions = ["Happiness", "Anger", "Approval"]
    
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
