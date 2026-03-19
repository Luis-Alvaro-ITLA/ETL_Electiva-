from dataclasses import dataclass
from datetime import datetime

@dataclass
class Comment:
    comentario_id: str
    cliente_id: str
    producto_id: str
    fecha: datetime
    comentario: str
    fuente: str