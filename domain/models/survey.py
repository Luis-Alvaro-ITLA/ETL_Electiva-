from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Survey:
    opinion_id: str
    cliente_id: str
    producto_id: str
    fecha: datetime
    comentario: str
    clasificacion: str
    puntaje_satisfaccion: Optional[int]
    fuente: str