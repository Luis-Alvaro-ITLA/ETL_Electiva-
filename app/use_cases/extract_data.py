from infrastructure.storage.raw_writer import RawDataWriter

class ExtractDataUseCase:
    def __init__(self, extractors, logger):
        self.extractors = extractors
        self.logger = logger
        self.writer = RawDataWriter()

    def execute(self):
        extraction_results = []

        for extractor in self.extractors:
            try:
                result = extractor.extract()

                file_path = self.writer.write(result)

                extraction_results.append(result)

                self.logger.info(
                    f"{extractor.__class__.__name__} → "
                    f"{result['total_records']} registros guardados en {file_path}"
                )

            except Exception as exc:
                self.logger.error(
                    f"Error en {extractor.__class__.__name__}: {exc}"
                )

        return extraction_results