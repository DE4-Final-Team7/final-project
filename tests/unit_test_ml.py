
import pytest
from src.text_analysis import TextAnalysis

text_analysis = TextAnalysis()


def test_preprocess_text():
    assert text_analysis.preprocess_text(["a<b>c"]) == ["ac"]

