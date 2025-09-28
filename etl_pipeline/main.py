import yaml
import logging
import sys
from pathlib import Path

from etl_pipeline.extractor import Extractor
from etl_pipeline.transformer import Transformer
from etl_pipeline.pipeline import Pipeline

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

def main():
    setup_logging()

    config_path = Path(__file__).resolve().parents[1] / "config" / "config.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    extractor = Extractor(config)
    transformer = Transformer(config)
    pipeline = Pipeline(extractor, transformer)

    pipeline.run()

if __name__ == "__main__":
    main()
