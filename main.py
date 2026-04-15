from app.use_cases.extract_data import ExtractDataUseCase
from app.use_cases.transform_data import TransformDataUseCase
from app.use_cases.load_data import LoadDataUseCase

from infrastructure.config.settings import Settings
from infrastructure.extractors.api_extractor import ApiExtractor
from infrastructure.extractors.csv_extractor import CsvExtractor
from infrastructure.extractors.db_extractor import DbExtractor
from infrastructure.logging.logger import get_logger

def main():
    settings = Settings()
    logger = get_logger()

    # EXTRACT
    extractors = [
        CsvExtractor(settings, logger),
        DbExtractor(settings, logger),
        ApiExtractor(settings, logger)
    ]

    extract_use_case = ExtractDataUseCase(extractors, logger)
    extraction_results = extract_use_case.execute()

    # TRANSFORM
    transform_use_case = TransformDataUseCase(logger)
    transformed_data = transform_use_case.execute(extraction_results)

    # LOAD
    load_use_case = LoadDataUseCase(settings, logger)
    load_use_case.execute(transformed_data)

    logger.info("ETL completado exitosamente")


if __name__ == "__main__":
    main()