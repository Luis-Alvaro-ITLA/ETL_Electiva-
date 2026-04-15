from app.transform.mapper_comment import CommentMapper
from app.transform.mapper_review import ReviewMapper
from app.transform.mapper_survey import SurveyMapper


class TransformDataUseCase:
    def __init__(self, logger):
        self.logger = logger

    def execute(self, extraction_results):
        transformed_data = []

        for result in extraction_results:
            source = result.get("source")
            data = result.get("data", [])

            self.logger.info(f"Transformando fuente: {source}")

            if source == "api":
                transformed_data.extend([
                    CommentMapper.transform(row) for row in data
                ])

            elif source == "database":
                transformed_data.extend([
                    ReviewMapper.transform(row) for row in data
                ])

            elif source == "csv":
                transformed_data.extend([
                    SurveyMapper.transform(row) for row in data
                ])

        self.logger.info(
            f"Total registros transformados: {len(transformed_data)}"
        )

        return transformed_data