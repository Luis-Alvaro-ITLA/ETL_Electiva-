from infrastructure.database.connection import create_dw_engine
from app.load.bulk_loader import BulkLoader
from app.load.dimension_loader import DimensionLoader

class LoadDataUseCase:
    def __init__(self, settings, logger):
        self.engine = create_dw_engine(settings)
        self.logger = logger

        self.bulk_loader = BulkLoader(self.engine, logger)
        self.dim_loader = DimensionLoader(self.engine, logger, self.bulk_loader)

    def execute(self, transformed_data):
        self.logger.info("Iniciando carga de dimensiones...")

        # 1. Limpiar tablas
        self.dim_loader.truncate_tables()

        # 2. Construir dimensiones desde datos reales
        self.dim_loader.load_clientes_from_facts(transformed_data)
        self.dim_loader.load_productos_from_facts(transformed_data)

        # 3. Dimensiones dinámicas
        fuentes = [row["fuente"] for row in transformed_data]
        fechas = [row["fecha"] for row in transformed_data]

        self.dim_loader.load_fuentes(fuentes)
        self.dim_loader.load_fecha(fechas)

        self.logger.info("Carga de dimensiones completada.")