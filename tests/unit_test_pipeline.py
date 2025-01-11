
import pytest
from src.etl_data import extract, transform, load
from src.ml_process import MLprocess


def test_extract_without_argument():
    with pytest.raises(TypeError):
        extract()

def test_transform_without_argument():
    with pytest.raises(TypeError):
        transform()

def test_load_without_argument():
    with pytest.raises(TypeError):
        load()

def test_MLprocess_without_argument():
    with pytest.raises(TypeError):
        MLprocess()
