import pandas as pd

class BulkLoader:
    def __init__(self, engine, logger):
        self.engine = engine
        self.logger = logger

    def bulk_insert(self, table_name: str, data: list, schema=None):
        if not data:
            return

        df = pd.DataFrame(data)

        self.logger.info(f"Cargando {len(df)} registros en {table_name}")

        df.to_sql(
            name=table_name,
            con=self.engine,
            if_exists="append",
            index=False,
            schema=schema,
            method="multi",
            chunksize=500
        )