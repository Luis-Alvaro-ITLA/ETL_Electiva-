from datetime import datetime
from domain.models.survey import Survey
from app.transform.sentiment_analyzer import SentimentAnalyzer

class SurveyMapper:

    @staticmethod
    def transform(raw_row: dict) -> dict:
        survey = Survey(
            opinion_id=str(raw_row["IdOpinion"]),
            cliente_id=str(raw_row["IdCliente"]),
            producto_id=str(raw_row["IdProducto"]),
            fecha=datetime.fromisoformat(str(raw_row["Fecha"])),
            comentario=raw_row["Comentario"],
            clasificacion=raw_row["Clasificación"],
            puntaje_satisfaccion=raw_row["PuntajeSatisfacción"],
            fuente=raw_row["Fuente"]
        )

        sentimiento = SentimentAnalyzer.classify_by_csv_label(survey.clasificacion)

        return {
            "cliente_id": survey.cliente_id,
            "producto_id": survey.producto_id,
            "fuente": survey.fuente,
            "comentario": survey.comentario,
            "fecha": survey.fecha,
            "rating": survey.puntaje_satisfaccion,
            "sentimiento": sentimiento
        }