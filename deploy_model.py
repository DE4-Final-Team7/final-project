
from fastapi import FastAPI
import uvicorn
from typing import List

from src.text_analysis import TextAnalysis


app = FastAPI()

text_analysis = TextAnalysis()

@app.post("/sentiment")
def predict_sentiment(input: List[str]):
    summarized_text = text_analysis.summarize_text(input)
    return text_analysis.analyze_sentiment(summarized_text)

@app.post("/emotion")
def predict_emotion(input: List[str]):
    summarized_text = text_analysis.summarize_text(input)
    return text_analysis.analyze_emotion(summarized_text)



if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

