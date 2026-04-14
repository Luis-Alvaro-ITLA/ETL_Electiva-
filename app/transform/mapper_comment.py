from datetime import datetime
from domain.models.comment import Comment
from app.transform.sentiment_analyzer import SentimentAnalyzer


class CommentMapper:

    @staticmethod
    def transform(raw_row: dict) -> dict:
        comment = Comment(
            comentario_id=str(raw_row["Comentario_ID"]),
            cliente_id=str(raw_row["Cliente_ID"]),
            producto_id=str(raw_row["Producto_ID"]),
            fecha=datetime.fromisoformat(str(raw_row["Fecha"])),
            comentario=raw_row["Comentario"],
            fuente="API"
        )

        sentimiento = SentimentAnalyzer.classify_comment(comment.comentario)

        return {
            "cliente_id": comment.cliente_id,
            "producto_id": comment.producto_id,
            "fuente": comment.fuente,
            "comentario": comment.comentario,
            "fecha": comment.fecha,
            "rating": None,
            "sentimiento": sentimiento
        }