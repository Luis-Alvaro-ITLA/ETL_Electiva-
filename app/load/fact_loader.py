import pandas as pd


class FactLoader:
    def __init__(self, engine, logger):
        self.engine = engine
        self.logger = logger

    def load_fact(self, transformed_data, fuente_map, fecha_map):
        if not transformed_data:
            self.logger.warning("No hay datos para cargar en Fact.Opiniones")
            return

        self.logger.info("Preparando datos para Fact.Opiniones")

        df = pd.DataFrame(transformed_data)

        if df.empty:
            self.logger.warning("DataFrame vacío, no se insertará nada")
            return

        df["Fuente_ID"] = df["fuente"].map(fuente_map)
        df["Fecha_ID"] = df["fecha"].map(fecha_map)

        df["cliente_id"] = pd.to_numeric(df["cliente_id"], errors="coerce")
        df["producto_id"] = pd.to_numeric(df["producto_id"], errors="coerce")
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

        df["Año"] = df["fecha"].dt.year

        df_final = df[[
            "cliente_id",
            "producto_id",
            "Fuente_ID",
            "Fecha_ID",
            "rating",
            "sentimiento",
            "comentario",
            "Año"
        ]].rename(columns={
            "cliente_id": "Cliente_ID",
            "producto_id": "Producto_ID",
            "rating": "Rating",
            "sentimiento": "Sentimiento",
            "comentario": "Comentario"
        })

        for col in ["Cliente_ID", "Producto_ID", "Fuente_ID", "Fecha_ID", "Rating", "Año"]:
            df_final[col] = df_final[col].astype("Int64")

        self.logger.info(f"Insertando {len(df_final)} registros en Fact.Opiniones")

        df_final.to_sql(
            name="Opiniones",
            schema="Fact",
            con=self.engine,
            if_exists="append",
            index=False,
            chunksize=100,
            method="multi"
        )

        self.logger.info("Carga en Fact.Opiniones completada")