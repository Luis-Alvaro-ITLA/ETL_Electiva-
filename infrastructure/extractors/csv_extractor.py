import pandas as pd

from domain.interfaces.extractor_interface import IExtractor


class CsvExtractor(IExtractor):
    def __init__(self, settings, logger):
        self.settings = settings
        self.logger = logger
        self.csv_config = self.settings.get("csv", {})

    def extract(self):
        path = self.csv_config.get("path")
        encoding = self.csv_config.get("encoding", "utf-8")
        delimiter = self.csv_config.get("delimiter", ",")
        chunksize = self.csv_config.get("chunksize", 10000)

        if not path:
            raise ValueError("No se encontró 'csv.path' en config.json")

        self.logger.info(f"Iniciando extracción de CSV: {path}")

        extracted_data = []
        total_rows = 0

        try:
            for chunk_number, chunk in enumerate(
                pd.read_csv(
                    path,
                    encoding=encoding,
                    sep=delimiter,
                    chunksize=chunksize
                ),
                start=1
            ):
                rows = chunk.to_dict(orient="records")
                extracted_data.extend(rows)
                total_rows += len(rows)

                self.logger.info(
                    f"CSV chunk {chunk_number}: {len(rows)} filas extraídas"
                )

            self.logger.info(
                f"Extracción CSV completada. Total filas extraídas: {total_rows}"
            )

            return {
                "source": "csv",
                "path": path,
                "total_records": total_rows,
                "data": extracted_data
            }

        except FileNotFoundError:
            self.logger.error(f"No se encontró el archivo CSV: {path}")
            raise
        except Exception as exc:
            self.logger.error(f"Error extrayendo CSV: {exc}")
            raise