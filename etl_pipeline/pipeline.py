import logging
import time

class Pipeline:
    def __init__(self, extractor, transformer):
        self.extractor = extractor
        self.transformer = transformer

    def run(self):
        logging.info("Pipeline started.")
        start_pipeline = time.time()

        # Extraction
        start = time.time()
        try:
            extract_path, extracted_count = self.extractor.run()
        except Exception as e:
            logging.error(f"Extraction failed: {e}")
            return None
        elapsed = time.time() - start
        logging.info(f"Extraction step completed in {elapsed:.2f}s, {extracted_count} rows at {extract_path}")

        if extracted_count == 0:
            logging.warning("Pipeline aborted: no data extracted.")
            return None

        # Transformation
        start = time.time()
        try:
            transform_path, transformed_count = self.transformer.run()
        except Exception as e:
            logging.error(f"Transformation failed: {e}")
            return None
        elapsed = time.time() - start
        logging.info(f"Transformation step completed in {elapsed:.2f}s, {transformed_count} rows at {transform_path}")

        total_elapsed = time.time() - start_pipeline
        logging.info(f"Pipeline finished successfully in {total_elapsed:.2f}s.")
        return {"raw": (extract_path, extracted_count), "transformed": (transform_path, transformed_count)}
