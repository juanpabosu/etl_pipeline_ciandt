import pandas as pd
import logging
import re
from pathlib import Path
from tabulate import tabulate


class Transformer:
    def __init__(self, config, data_dir=Path(__file__).resolve().parents[1] / "data"):
        self.raw_file = data_dir / config["output"]["raw_file"]
        self.format = config["output"]["format"]
        self.base_name = config["output"]["transformed_file"]
        self.data_dir = data_dir

    def run(self):
        logging.info("Starting transformation...")

        def preview_markdown(df, max_rows=10, max_colwidth=20):
            df_preview = df.copy()
            for col in df_preview.select_dtypes(include=["object"]):  # only string columns
                df_preview[col] = df_preview[col].astype(str).apply(
                    lambda x: x[:max_colwidth] + "..." if len(x) > max_colwidth else x
                )
            print(df_preview.head(max_rows).to_markdown(index=False))

        try:
            df_raw = pd.read_json(path_or_buf=self.raw_file, lines=True)
        except ValueError:
            logging.error("No valid JSON data to transform.")
            raise

        if df_raw.empty:
            logging.warning("Transformation aborted: empty dataset.")
            return str(self.data_dir / f"{self.base_name}.{self.format}"), 0
        
        pd.set_option("display.max_colwidth", 20)  # show only 20 chars

        # flatten the nested fields into clean columns      
        df = pd.DataFrame({
            "id": df_raw["id"],
            "name": df_raw["name"],
            "description": df_raw["description"],
            "comics": df_raw["comics"].apply(lambda x: x.get("available") if isinstance(x, dict) else None),
            "series": df_raw["series"].apply(lambda x: x.get("available") if isinstance(x, dict) else None),
            "stories": df_raw["stories"].apply(lambda x: x.get("available") if isinstance(x, dict) else None),
            "events": df_raw["events"].apply(lambda x: x.get("available") if isinstance(x, dict) else None),
        })

        # Clean description: remove/collapse whitespace/newlines
        if "description" in df.columns:
            df["description"] = (
                df["description"]
                .fillna("")
                .apply(lambda x: re.sub(r"\s+", " ", str(x)))       
                .str.strip()
            )

        print(f"Sample output:")
        preview_markdown(df)
        print(f"Dataframe shape: {df.shape}")

        output_path = self.data_dir / f"{self.base_name}.{self.format}"
        if self.format == "csv":
            df.to_csv(output_path, index=False)
        elif self.format == "parquet":
            df.to_parquet(output_path, index=False)
        else:
            raise ValueError(f"Unsupported format: {self.format}")

        logging.info(f"Transformed data saved to {output_path}")
        return str(output_path), len(df)
