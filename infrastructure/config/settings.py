import json
from pathlib import Path

class Settings:
    def __init__(self, path: str = "config/config.json"):
        config_path = Path(path)
        with config_path.open("r", encoding="utf-8") as file:
            self.config = json.load(file)

    def get(self, key: str, default=None):
        return self.config.get(key, default)