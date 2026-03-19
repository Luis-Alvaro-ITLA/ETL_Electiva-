import json
from pathlib import Path
from datetime import date, datetime


class RawDataWriter:
    def __init__(self, base_path="staging/raw"):
        self.base_path = Path(base_path)

    def write(self, extraction_result: dict):
        source = extraction_result.get("source", "unknown")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        source_path = self.base_path / source
        source_path.mkdir(parents=True, exist_ok=True)

        file_path = source_path / f"{source}_{timestamp}.json"

        with file_path.open("w", encoding="utf-8") as file:
            json.dump(
                extraction_result,
                file,
                ensure_ascii=False,
                indent=2,
                default=self._json_serializer
            )

        return str(file_path)

    @staticmethod
    def _json_serializer(obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")