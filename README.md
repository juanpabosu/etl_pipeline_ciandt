# Python ETL Pipeline and SQL Test for Data Engineers

This project implements a simple Extract-Transform-Load (ETL) pipeline in Python and SQL challenges using GCP BigQuery

## ETL Features
- Extract data from a REST API with pagination
- Save raw data in JSON Lines format
- Transform data using pandas and save in CSV or Parquet format (dynamic filename)
- Logging, exception handling, and execution time tracking per step
- Configurable through a YAML config file
- Includes unit tests for extractor, transformer, and pipeline orchestration

## Prerequisites
- Python 3.8 or higher
- pip (Python package installer)
- BigQuery Studio

## ETL Pipeline Setup

### 1. Clone or Download the Project
```bash
cd etl_pipeline_ciandt
```

### 2. Create Virtual Environment

**Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

**macOS/Linux:**
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Verify Installation
```bash
pip list
```

## Project Structure
```
etl_pipeline_ciandt/
│
├── config/
│   └── config.yaml        # Configuration file
│
├── data/                  # Extracted and transformed data
│
├── etl_pipeline/
│   ├── __init__.py
│   ├── extractor.py       # Extract data from API
│   ├── transformer.py     # Transform data
│   ├── pipeline.py        # Orchestrates Extract → Transform steps
│   └── main.py            # Entry point
│ 
├──sql/                    # SQL queries
│
├── tests/                 # Unit tests
│   ├── test_extractor.py
│   ├── test_transformer.py
│   └── test_pipeline.py
│
├── requirements.txt       # Python dependencies
└── README.md
```

## ETL Pipline Usage

### Configuration
Edit `config/config.yaml` to customize:
- API endpoints
- Output formats
- File paths
- Other pipeline settings

### Run Pipeline
```bash
python -m etl_pipeline.main
```

**Output:**
- Raw data: `data/raw_output.jsonl`
- Transformed data: `data/final_output.csv` or `.parquet` (depending on config)

### Run Tests
```bash
pytest tests/
```

### Run Specific Test
```bash
pytest tests/test_extractor.py -v
```

## Deactivate Virtual Environment
When you're done working:
```bash
deactivate
```

## SQL Test

Review SQL challenges in `sql/README.md`