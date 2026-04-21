from infrastructure.database.connection import create_dw_engine
from app.load.bulk_loader import BulkLoader
from app.load.dimension_loader import DimensionLoader
import pandas as pd

from app.load.fact_loader import FactLoader

class LoadDataUseCase:
    def __init__(self, settings, logger):
        self.engine = create_dw_engine(settings)
        self.logger = logger

        self.bulk_loader = BulkLoader(self.engine, logger)
        self.dim_loader = DimensionLoader(self.engine, logger, self.bulk_loader)

    def _get_fuente_map(self):
        query = "SELECT Fuente_ID, Tipo FROM Dim.Fuente"
        df = pd.read_sql(query, self.engine)
        return dict(zip(df["Tipo"], df["Fuente_ID"]))

    def _get_fecha_map(self):
        query = "SELECT Fecha_ID, Fecha FROM Dim.Fecha"
        df = pd.read_sql(query, self.engine)
        return dict(zip(df["Fecha"], df["Fecha_ID"]))

    def execute(self, transformed_data):
        self.logger.info("Iniciando carga de dimensiones...")

        # Limpiar tablas
        self.dim_loader.truncate_tables()

        # Construir dimensiones
        self.dim_loader.load_clientes_from_facts(transformed_data)
        self.dim_loader.load_productos_from_facts(transformed_data)

        # Dimensiones
        fuentes = [row["fuente"] for row in transformed_data]
        fechas = [row["fecha"] for row in transformed_data]

        self.dim_loader.load_fuentes(fuentes)
        self.dim_loader.load_fecha(fechas)

        self.logger.info("Carga de dimensiones completada.")

        # Fact Table
        self.fact_loader = FactLoader(self.engine, self.logger)

        fuente_map = self._get_fuente_map()
        fecha_map = self._get_fecha_map()

        self.fact_loader.load_fact(transformed_data, fuente_map, fecha_map)