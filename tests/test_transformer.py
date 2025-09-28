import pytest
import json
import pandas as pd
from etl_pipeline.transformer import Transformer

@pytest.fixture
def config():
    return {
        "output": {
            "raw_file": "raw.jsonl",
            "transformed_file": "out",
            "format": "csv",
        }
    }

@pytest.fixture
def marvel_character_data():
    return {
        "id": 1011334,
        "name": "3-D Man",
        "description": "   Test description\n\nwith   multiple\tspaces   ",
        "comics": {"available": 12, "items": []},
        "series": {"available": 3, "items": []},
        "stories": {"available": 21, "items": []},
        "events": {"available": 1, "items": []}
    }

def test_transformer_success_csv(tmp_path, config, marvel_character_data):
    raw_file = tmp_path / "raw.jsonl"
    raw_file.write_text(json.dumps(marvel_character_data) + "\n")

    transformer = Transformer(config, data_dir=tmp_path)
    path, count = transformer.run()
    
    assert count == 1
    assert path.endswith(".csv")
    assert (tmp_path / "out.csv").exists()
    
    df = pd.read_csv(path)
    assert len(df) == 1
    assert df.iloc[0]["id"] == 1011334
    assert df.iloc[0]["name"] == "3-D Man"
    assert df.iloc[0]["description"] == "Test description with multiple spaces"
    assert df.iloc[0]["comics"] == 12
    assert df.iloc[0]["series"] == 3

def test_transformer_success_parquet(tmp_path, config, marvel_character_data):
    """Skip parquet test if pyarrow not available"""
    pytest.importorskip("pyarrow")
    
    config["output"]["format"] = "parquet"
    raw_file = tmp_path / "raw.jsonl"
    raw_file.write_text(json.dumps(marvel_character_data) + "\n")

    transformer = Transformer(config, data_dir=tmp_path)
    path, count = transformer.run()
    
    assert count == 1
    assert path.endswith(".parquet")
    assert (tmp_path / "out.parquet").exists()
    
    df = pd.read_parquet(path)
    assert len(df) == 1
    assert df.iloc[0]["id"] == 1011334

def test_transformer_multiple_records(tmp_path, config, marvel_character_data):
    raw_file = tmp_path / "raw.jsonl"
    data1 = marvel_character_data
    data2 = {**marvel_character_data, "id": 1011335, "name": "A-Bomb"}
    
    raw_file.write_text(
        json.dumps(data1) + "\n" + json.dumps(data2) + "\n"
    )

    transformer = Transformer(config, data_dir=tmp_path)
    path, count = transformer.run()
    
    assert count == 2
    df = pd.read_csv(path)
    assert len(df) == 2
    assert df.iloc[0]["name"] == "3-D Man"
    assert df.iloc[1]["name"] == "A-Bomb"

def test_transformer_empty_file(tmp_path, config):
    raw_file = tmp_path / "raw.jsonl"
    raw_file.write_text("")

    transformer = Transformer(config, data_dir=tmp_path)
    
    # Empty file should return 0 count, not raise ValueError
    path, count = transformer.run()
    assert count == 0
    assert path.endswith(".csv")

def test_transformer_missing_nested_fields(tmp_path, config):
    raw_file = tmp_path / "raw.jsonl"
    incomplete_data = {
        "id": 1011334,
        "name": "3-D Man",
        "description": "Test",
        "comics": None,
        "series": {"available": 3},
        "stories": "invalid",
        "events": {"available": 1}
    }
    raw_file.write_text(json.dumps(incomplete_data) + "\n")

    transformer = Transformer(config, data_dir=tmp_path)
    path, count = transformer.run()
    
    assert count == 1
    df = pd.read_csv(path)
    assert pd.isna(df.iloc[0]["comics"])
    assert df.iloc[0]["series"] == 3
    assert pd.isna(df.iloc[0]["stories"])

def test_transformer_description_cleaning(tmp_path, config):
    raw_file = tmp_path / "raw.jsonl"
    data_with_messy_description = {
        "id": 1,
        "name": "Test",
        "description": "\n\n  Multiple\n\n\tspaces   and\r\n  newlines  \n",
        "comics": {"available": 1},
        "series": {"available": 1},
        "stories": {"available": 1},
        "events": {"available": 1}
    }
    raw_file.write_text(json.dumps(data_with_messy_description) + "\n")

    transformer = Transformer(config, data_dir=tmp_path)
    path, count = transformer.run()
    
    df = pd.read_csv(path)
    assert df.iloc[0]["description"] == "Multiple spaces and newlines"

def test_transformer_invalid_json_file(tmp_path, config):
    raw_file = tmp_path / "raw.jsonl"
    raw_file.write_text("invalid json content")

    transformer = Transformer(config, data_dir=tmp_path)
    
    with pytest.raises(ValueError):
        transformer.run()

def test_transformer_unsupported_format(tmp_path, config):
    config["output"]["format"] = "json"
    raw_file = tmp_path / "raw.jsonl"
    raw_file.write_text(json.dumps({"id": 1, "name": "test", "description": "", "comics": {"available": 1}, "series": {"available": 1}, "stories": {"available": 1}, "events": {"available": 1}}) + "\n")

    transformer = Transformer(config, data_dir=tmp_path)
    
    with pytest.raises(ValueError, match="Unsupported format: json"):
        transformer.run()
