from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Review:
    resena_id: str
    cliente_id: str
    producto_id: str
    fuente_id: str
    comentario: str
    rating: Optional[int]
    fecha: Optional[datetime]