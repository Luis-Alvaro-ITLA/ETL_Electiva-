from app.use_cases.extract_data import ExtractDataUseCase
from infrastructure.config.settings import Settings
from infrastructure.extractors.api_extractor import ApiExtractor
from infrastructure.extractors.csv_extractor import CsvExtractor
from infrastructure.extractors.db_extractor import DbExtractor
from infrastructure.logging.logger import get_logger

def main():
    settings = Settings()
    logger = get_logger()

    extractors = [
        CsvExtractor(settings, logger),
        DbExtractor(settings, logger),
        ApiExtractor(settings, logger)
    ]

    use_case = ExtractDataUseCase(extractors, logger)
    results = use_case.execute()

    logger.info("Resumen de extracción:")
    for result in results:
        logger.info(
            f"Fuente: {result['source']} | Registros extraídos: {result['total_records']}"
        )


if __name__ == "__main__":
    main()