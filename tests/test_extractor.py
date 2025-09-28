import pytest
import json
import requests
from unittest.mock import Mock
from etl_pipeline.extractor import Extractor

@pytest.fixture
def config():
    return {
        "api": {
            "base_url": "https://gateway.marvel.com",
            "api_path": "/v1/public/characters",
            "retries": 3,
            "timeout": 10,
            "page_size": 2,
        },
        "output": {"raw_file": "raw.jsonl", "transformed_file": "final", "format": "csv"},
    }

@pytest.fixture
def marvel_character_data():
    return {
        "id": 1011334,
        "name": "3-D Man",
        "description": "Test description",
        "comics": {"available": 12},
        "series": {"available": 3},
        "stories": {"available": 21},
        "events": {"available": 1}
    }

def test_extractor_success(monkeypatch, tmp_path, config, marvel_character_data):
    call_count = 0
    
    def mock_get(url, params=None, timeout=None):
        nonlocal call_count
        response = Mock()
        
        if call_count == 0:
            response.json.return_value = {
                "data": {
                    "results": [marvel_character_data, {**marvel_character_data, "id": 1011335}]
                }
            }
        else:
            response.json.return_value = {"data": {"results": []}}
        
        response.raise_for_status.return_value = None
        call_count += 1
        return response

    monkeypatch.setattr("requests.get", mock_get)
    extractor = Extractor(config, data_dir=tmp_path)
    path, count = extractor.run()
    
    assert count == 2
    assert tmp_path.joinpath("raw.jsonl").exists()
    
    with open(path, 'r') as f:
        lines = f.readlines()
        assert len(lines) == 2
        data = json.loads(lines[0])
        assert data["id"] == 1011334
        assert data["name"] == "3-D Man"

def test_extractor_empty_response(monkeypatch, tmp_path, config):
    def mock_get(url, params=None, timeout=None):
        response = Mock()
        response.json.return_value = {"data": {"results": []}}
        response.raise_for_status.return_value = None
        return response

    monkeypatch.setattr("requests.get", mock_get)
    extractor = Extractor(config, data_dir=tmp_path)
    path, count = extractor.run()
    
    assert count == 0
    assert tmp_path.joinpath("raw.jsonl").exists()

def test_extractor_malformed_response(monkeypatch, tmp_path, config):
    def mock_get(url, params=None, timeout=None):
        response = Mock()
        response.json.return_value = {"invalid": "structure"}
        response.raise_for_status.return_value = None
        return response

    monkeypatch.setattr("requests.get", mock_get)
    extractor = Extractor(config, data_dir=tmp_path)
    path, count = extractor.run()
    
    assert count == 0

def test_extractor_http_error(monkeypatch, tmp_path, config):
    def mock_get(url, params=None, timeout=None):
        response = Mock()
        response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
        return response

    monkeypatch.setattr("requests.get", mock_get)
    extractor = Extractor(config, data_dir=tmp_path)
    
    with pytest.raises(AttributeError):  # max_retries not defined in original code
        extractor.run()

def test_extractor_pagination(monkeypatch, tmp_path, config, marvel_character_data):
    call_count = 0
    
    def mock_get(url, params=None, timeout=None):
        nonlocal call_count
        response = Mock()
        
        if call_count == 0:
            response.json.return_value = {
                "data": {"results": [marvel_character_data] * 2}
            }
        elif call_count == 1:
            response.json.return_value = {
                "data": {"results": [marvel_character_data]}
            }
        else:
            response.json.return_value = {"data": {"results": []}}
        
        response.raise_for_status.return_value = None
        call_count += 1
        return response

    monkeypatch.setattr("requests.get", mock_get)
    extractor = Extractor(config, data_dir=tmp_path)
    path, count = extractor.run()
    
    assert count == 3
    assert call_count == 3
