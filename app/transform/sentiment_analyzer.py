import re

class SentimentAnalyzer:

    NEGATIVE_WORDS = [
        "insatisfecho", "decepcionado", "mala calidad",
        "dañado", "dañó", "producto roto", "pésima", "mal",
        "no funciona", "no recomendable", "no volvería",
        "muy malo", "terrible", "defectuoso"
    ]

    POSITIVE_WORDS = [
        "perfecto", "muy contento", "excelente", "maravilloso",
        "me encanta", "muy satisfecho", "recomendable",
        "buena calidad", "funciona perfecto", "gran relación calidad-precio"
    ]

    @staticmethod
    def classify_comment(text: str) -> str:
        text = text.lower()

        for word in SentimentAnalyzer.NEGATIVE_WORDS:
            if word in text:
                return "Negativo"

        for word in SentimentAnalyzer.POSITIVE_WORDS:
            if word in text:
                return "Positivo"

        return "Neutro"

    @staticmethod
    def classify_by_rating(rating: int) -> str:
        if rating in (1, 2):
            return "Negativo"
        if rating == 3:
            return "Neutro"
        if rating in (4, 5):
            return "Positivo"
        return "Neutro"

    @staticmethod
    def classify_by_csv_label(label: str) -> str:
        if not label:
            return "Neutro"
        return label.capitalize()