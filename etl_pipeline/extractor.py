import requests
import logging
import json
import os
import time
from pathlib import Path

class Extractor:
    def __init__(self, config, data_dir=Path(__file__).resolve().parents[1] / "data"):
        # api properties
        self.base_url = config["api"]["base_url"]
        self.api_path = config["api"]["api_path"]
        self.limit = config["api"]["page_size"]
        self.retries = config["api"]["retries"]
        self.timeout = config["api"]["timeout"]

        ## storage properties        
        self.raw_filename = config["output"]["raw_file"]
        self.data_dir = data_dir
        
        ## auth properties - harcoded for testing purposes
        self.ts = '1'
        self.apikey = 'a439401808d851ea84afb3bbb5184299'
        self.hash = 'ac21937da96021d43e052393da516ceb'

    def run(self):
            """Extract data from API with limit+offset pagination, write JSONL file, and return file path + row count."""
            raw_path = os.path.join(self.data_dir, self.raw_filename)
            os.makedirs(self.data_dir, exist_ok=True)

            offset = 0
            total_rows = 0
            retries = 0

            with open(raw_path, "w", encoding="utf-8") as f:
                while True:
                    try:
                        url = self.base_url + self.api_path                        
                        params = {"ts": self.ts, "apikey": self.apikey, "hash": self.hash, "limit": self.limit, "offset": offset}
                        response = requests.get(url, params=params, timeout=30)
                        response.raise_for_status()
                        payload  = response.json()

                        # Navigate to data.results
                        rows = (
                            payload.get("data", {}).get("results", [])
                            if isinstance(payload.get("data"), dict)
                            else []
                        )

                        # Handle no data returned
                        if not rows:
                            break

                        # Write each row as JSON line
                        for row in rows:
                            f.write(json.dumps(row) + "\n")

                        total_rows += len(rows)
                        logging.info(f"Fetched {len(rows)} rows at offset {offset}")

                        offset += self.limit # next offset
                        retries = 0  # reset after success

                    except Exception as e:
                        retries += 1
                        if retries > self.max_retries:
                            logging.error(f"Extractor failed after {self.max_retries} retries: {e}")
                            raise
                        logging.warning(f"Retry {retries}/{self.max_retries} after error: {e}")
                        time.sleep(2 ** retries)

            if total_rows == 0:
                logging.warning("Extraction finished but no data was retrieved.")
            else:
                logging.info(f"Extraction finished: {total_rows} records saved.")
            
            return str(raw_path), total_rows
