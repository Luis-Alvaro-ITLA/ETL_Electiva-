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

        fact_rows = []

        for idx, row in enumerate(transformed_data, start=1):
            try:
                fact_rows.append({
                    "Opinion_ID": idx,
                    "Cliente_ID": int(row["cliente_id"]) if row["cliente_id"] else None,
                    "Producto_ID": int(row["producto_id"]) if row["producto_id"] else None,
                    "Fuente_ID": fuente_map.get(row["fuente"]),
                    "Fecha_ID": fecha_map.get(row["fecha"]),
                    "Rating": row.get("rating"),
                    "Sentimiento": row.get("sentimiento"),
                    "Comentario": row.get("comentario"),
                    "Año": row["fecha"].year if row.get("fecha") else None
                })

            except Exception as exc:
                self.logger.error(f"Error transformando fila para Fact: {exc}")

        df = pd.DataFrame(fact_rows)

        if df.empty:
            self.logger.warning("DataFrame vacío, no se insertará nada en Fact.Opiniones")
            return

        try:
            df["Cliente_ID"] = df["Cliente_ID"].astype("Int64")
            df["Producto_ID"] = df["Producto_ID"].astype("Int64")
            df["Fuente_ID"] = df["Fuente_ID"].astype("Int64")
            df["Fecha_ID"] = df["Fecha_ID"].astype("Int64")
            df["Rating"] = df["Rating"].astype("Int64")
            df["Año"] = df["Año"].astype("Int64")
        except Exception as exc:
            self.logger.error(f"Error convirtiendo tipos en FactLoader: {exc}")
            raise

        self.logger.info(f"Insertando {len(df)} registros en Fact.Opiniones")

        try:
            df.to_sql(
                name="Opiniones",
                schema="Fact",
                con=self.engine,
                if_exists="append",
                index=False,
                chunksize=1000
            )

            self.logger.info("Carga en Fact.Opiniones completada")

        except Exception as exc:
            self.logger.error(f"Error insertando en Fact.Opiniones: {exc}")
            raise