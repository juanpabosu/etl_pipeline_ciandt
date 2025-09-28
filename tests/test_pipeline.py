import pytest
import logging
from unittest.mock import Mock
from etl_pipeline.pipeline import Pipeline

class MockExtractor:
    def __init__(self, path, count, should_fail=False):
        self.path = path
        self.count = count
        self.should_fail = should_fail
    
    def run(self):
        if self.should_fail:
            raise Exception("Extraction failed")
        return self.path, self.count

class MockTransformer:
    def __init__(self, path, count, should_fail=False):
        self.path = path
        self.count = count
        self.should_fail = should_fail
    
    def run(self):
        if self.should_fail:
            raise Exception("Transformation failed")
        return self.path, self.count

def test_pipeline_success(caplog, tmp_path):
    caplog.set_level(logging.INFO)
    
    extractor = MockExtractor(str(tmp_path / "raw.jsonl"), 5)
    transformer = MockTransformer(str(tmp_path / "out.csv"), 5)
    pipeline = Pipeline(extractor, transformer)
    
    result = pipeline.run()
    
    assert result is not None
    assert result["raw"][0] == str(tmp_path / "raw.jsonl")
    assert result["raw"][1] == 5
    assert result["transformed"][0] == str(tmp_path / "out.csv")
    assert result["transformed"][1] == 5
    
    assert "Pipeline started" in caplog.text
    assert "Extraction step completed" in caplog.text
    assert "Transformation step completed" in caplog.text
    assert "Pipeline finished successfully" in caplog.text

def test_pipeline_no_data_extracted(caplog, tmp_path):
    caplog.set_level(logging.INFO)
    
    extractor = MockExtractor(str(tmp_path / "raw.jsonl"), 0)
    transformer = MockTransformer(str(tmp_path / "out.csv"), 0)
    pipeline = Pipeline(extractor, transformer)
    
    result = pipeline.run()
    
    assert result is None
    assert "Pipeline aborted: no data extracted" in caplog.text
    assert "Transformation step completed" not in caplog.text

def test_pipeline_extraction_failure(caplog, tmp_path):
    caplog.set_level(logging.INFO)
    
    extractor = MockExtractor(str(tmp_path / "raw.jsonl"), 5, should_fail=True)
    transformer = MockTransformer(str(tmp_path / "out.csv"), 5)
    pipeline = Pipeline(extractor, transformer)
    
    result = pipeline.run()
    
    assert result is None
    assert "Extraction failed" in caplog.text
    assert "Transformation step completed" not in caplog.text

def test_pipeline_transformation_failure(caplog, tmp_path):
    caplog.set_level(logging.INFO)
    
    extractor = MockExtractor(str(tmp_path / "raw.jsonl"), 5)
    transformer = MockTransformer(str(tmp_path / "out.csv"), 5, should_fail=True)
    pipeline = Pipeline(extractor, transformer)
    
    result = pipeline.run()
    
    assert result is None
    assert "Extraction step completed" in caplog.text
    assert "Transformation failed" in caplog.text
    assert "Pipeline finished successfully" not in caplog.text

def test_pipeline_timing_logs(caplog, tmp_path):
    caplog.set_level(logging.INFO)
    
    extractor = MockExtractor(str(tmp_path / "raw.jsonl"), 100)
    transformer = MockTransformer(str(tmp_path / "out.csv"), 100)
    pipeline = Pipeline(extractor, transformer)
    
    result = pipeline.run()
    
    assert result is not None
    
    # Check that timing information is logged
    log_messages = [record.message for record in caplog.records]
    extraction_timing = any("Extraction step completed in" in msg and "100 rows" in msg for msg in log_messages)
    transformation_timing = any("Transformation step completed in" in msg and "100 rows" in msg for msg in log_messages)
    pipeline_timing = any("Pipeline finished successfully in" in msg for msg in log_messages)
    
    assert extraction_timing
    assert transformation_timing
    assert pipeline_timing

def test_pipeline_data_flow_validation(caplog, tmp_path):
    """Test that pipeline properly validates data flow between components"""
    caplog.set_level(logging.INFO)
    
    # Test with mismatched data counts (edge case)
    extractor = MockExtractor(str(tmp_path / "raw.jsonl"), 10)
    transformer = MockTransformer(str(tmp_path / "out.csv"), 8)  # Some data lost in transformation
    pipeline = Pipeline(extractor, transformer)
    
    result = pipeline.run()
    
    assert result is not None
    assert result["raw"][1] == 10
    assert result["transformed"][1] == 8
    assert "10 rows" in caplog.text
    assert "8 rows" in caplog.text
