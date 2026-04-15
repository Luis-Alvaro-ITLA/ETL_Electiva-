import pandas as pd

class BulkLoader:
    def __init__(self, engine, logger):
        self.engine = engine
        self.logger = logger

    def bulk_insert(self, table_name: str, data: list, schema: str = None):
        if not data:
            return

        df = pd.DataFrame(data)

        self.logger.info(f"Cargando {len(df)} registros en {schema}.{table_name}")

        df.to_sql(
            table_name,
            self.engine,
            if_exists="append",
            index=False,
            schema=schema,
            method="multi"
        )