import pandas as pd
from sqlalchemy import text

from domain.interfaces.extractor_interface import IExtractor
from infrastructure.database.connection import create_sql_server_engine


class DbExtractor(IExtractor):
    def __init__(self, settings, logger):
        self.settings = settings
        self.logger = logger
        self.db_config = self.settings.get("database", {})
        self.engine = create_sql_server_engine(settings)

    def extract(self):
        source_table = self.db_config.get("source_table")
        query = self.db_config.get("query")
        chunksize = self.db_config.get("chunksize", 10000)

        if not query and not source_table:
            raise ValueError(
                "Debes definir 'database.query' o 'database.source_table' en config.json"
            )

        if not query:
            query = f"SELECT * FROM {source_table}"

        self.logger.info("Iniciando extracción desde base de datos")

        extracted_data = []
        total_rows = 0

        try:
            with self.engine.connect() as connection:
                for chunk_number, chunk in enumerate(
                    pd.read_sql(text(query), connection, chunksize=chunksize),
                    start=1
                ):
                    rows = chunk.to_dict(orient="records")
                    extracted_data.extend(rows)
                    total_rows += len(rows)

                    self.logger.info(
                        f"DB chunk {chunk_number}: {len(rows)} filas extraídas"
                    )

            self.logger.info(
                f"Extracción BD completada. Total filas extraídas: {total_rows}"
            )

            return {
                "source": "database",
                "table": source_table,
                "query": query,
                "total_records": total_rows,
                "data": extracted_data
            }

        except Exception as exc:
            self.logger.error(f"Error extrayendo desde base de datos: {exc}")
            raise