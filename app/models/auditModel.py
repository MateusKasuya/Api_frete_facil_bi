from sqlalchemy import Column, Integer, String, Boolean, DateTime, func, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class LoginAudit(Base):
    __tablename__ = 'tbauditoria'

    id = Column(Integer, primary_key=True)
    usuario = Column(String)
    codempresa = Column(String)
    ip_address = Column(String) #IP da origem da Requisição
    metodo = Column(String, nullable=False)  # GET, POST, etc.
    endpoint = Column(String, nullable=False)  # URL acessada
    params = Column(JSON, nullable=True)  # Query params
    body_request = Column(JSON, nullable=True)  # Corpo da requisição
    body_response = Column(JSON, nullable=True)  # Resposta da API
    status_code = Column(Integer, nullable=False)  # Status HTTP
    user_agent =Column(String, nullable=False) #Aplicação de onde esta sendo enviado a Requisição