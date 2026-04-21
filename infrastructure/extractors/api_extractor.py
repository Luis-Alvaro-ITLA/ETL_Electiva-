import requests

from domain.interfaces.extractor_interface import IExtractor

class ApiExtractor(IExtractor):
    def __init__(self, settings, logger):
        self.settings = settings
        self.logger = logger
        self.api_config = self.settings.get("api", {})

    def extract(self):
        base_url = self.api_config.get("base_url")
        endpoint = self.api_config.get("endpoint", "")
        timeout = self.api_config.get("timeout", 30)
        headers = self.api_config.get("headers", {}) or {}
        params = self.api_config.get("params", {}) or {}
        data_path = self.api_config.get("data_path", "datos")

        url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"

        self.logger.info(f"Iniciando extracción paginada desde API: {url}")

        all_records = []
        offset = 0
        limit = params.get("limit", 1000)

        try:
            while True:
                params["offset"] = offset

                response = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=timeout
                )
                response.raise_for_status()

                payload = response.json()
                records = self._extract_data_from_payload(payload, data_path)

                if not records:
                    break

                all_records.extend(records)

                self.logger.info(
                    f"API chunk offset {offset}: {len(records)} registros"
                )

                if len(records) < limit:
                    break

                offset += limit

            self.logger.info(
                f"Extracción API completada. Total registros: {len(all_records)}"
            )

            return {
                "source": "api",
                "url": url,
                "total_records": len(all_records),
                "data": all_records
            }

        except Exception as exc:
            self.logger.error(f"Error en ApiExtractor: {exc}")
            raise

    def _extract_data_from_payload(self, payload, data_path):
        current = payload

        for key in data_path.split("."):
            if not isinstance(current, dict) or key not in current:
                raise ValueError(
                    f"No se encontró la ruta '{data_path}' en la respuesta JSON"
                )
            current = current[key]

        return current