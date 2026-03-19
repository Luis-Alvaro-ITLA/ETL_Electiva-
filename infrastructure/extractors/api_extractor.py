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
        method = self.api_config.get("method", "GET").upper()
        timeout = self.api_config.get("timeout", 30)
        headers = self.api_config.get("headers", {}) or {}
        params = self.api_config.get("params", {}) or {}
        data_path = self.api_config.get("data_path", "datos")

        if not base_url:
            raise ValueError("No se encontró 'api.base_url' en config.json")

        url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"

        self.logger.info(f"Iniciando extracción desde API: {url}")

        try:
            if method != "GET":
                raise ValueError(
                    f"Para esta API local se esperaba método GET, no {method}"
                )

            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=timeout
            )
            response.raise_for_status()

            payload = response.json()
            records = self._extract_data_from_payload(payload, data_path)

            if not isinstance(records, list):
                raise ValueError(
                    f"La ruta '{data_path}' no contiene una lista de registros"
                )

            self.logger.info(
                f"Extracción API completada. Total registros extraídos: {len(records)}"
            )

            return {
                "source": "api",
                "url": url,
                "method": method,
                "total_records": len(records),
                "data": records
            }

        except requests.exceptions.Timeout:
            self.logger.error(f"Timeout al consumir la API: {url}")
            raise
        except requests.exceptions.ConnectionError:
            self.logger.error(
                f"No se pudo conectar a la API en {url}. Verifica que esté levantada."
            )
            raise
        except requests.exceptions.RequestException as exc:
            self.logger.error(f"Error HTTP consumiendo la API: {exc}")
            raise
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