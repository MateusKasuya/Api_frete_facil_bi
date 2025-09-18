from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class tbusuario(Base):
    __tablename__ = 'tbusuarios'

    codusuario = Column(Integer, primary_key=True, index=True)
    nomeusuario = Column(String)
    senhausuario = Column(String)
    codempresa = Column(Integer)
    usuarioativo = Column(String)
    cpfusuario = Column(String)
    emailusuario = Column(String)
