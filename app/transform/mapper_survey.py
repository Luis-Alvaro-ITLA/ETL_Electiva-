from datetime import datetime
from domain.models.review import Review
from app.transform.sentiment_analyzer import SentimentAnalyzer


class ReviewMapper:

    @staticmethod
    def transform(raw_row: dict) -> dict:
        review = Review(
            resena_id=str(raw_row["Resena_ID"]),
            cliente_id=str(raw_row["Cliente_ID"]),
            producto_id=str(raw_row["Producto_ID"]),
            fuente_id=str(raw_row["Fuente_ID"]),
            comentario=raw_row["Comentario"],
            rating=raw_row["Rating"],
            fecha=datetime.fromisoformat(str(raw_row["Fecha"]))
        )

        sentimiento = SentimentAnalyzer.classify_by_rating(review.rating)

        return {
            "cliente_id": review.cliente_id,
            "producto_id": review.producto_id,
            "fuente": "Web",  # Ajustable
            "comentario": review.comentario,
            "fecha": review.fecha,
            "rating": review.rating,
            "sentimiento": sentimiento
        }