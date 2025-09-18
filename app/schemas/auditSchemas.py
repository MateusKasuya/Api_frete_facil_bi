from pydantic import BaseModel
from typing import Optional, ClassVar
from datetime import date


#Modelo de Retorno de Empresas Cadstradas (Get/Post/Put)
class AuditInsert(BaseModel):
    codempresa: str = None
    usuario: str = None
    #datareq = Column(DateTime)
    #horareq = Column(DateTime)
    ip_address: str = None
    bodyreq: str = None
    bodyresponse: str = None
    user_agent: str = None