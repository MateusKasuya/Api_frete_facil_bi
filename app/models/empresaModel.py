from sqlalchemy import Column, Integer, String, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func


Base = declarative_base()

class tbempresa(Base):
    __tablename__ = 'tbempresas'
    codempresa = Column(Integer, primary_key=True, index=True)
    nomeempresa = Column(String)
    cnpjcpf = Column(String) 
    tipobdempresa = Column(String)
    portabd = Column(String)
    ipbd = Column(String)
    caminhobd = Column(String)
    ativa = Column(String)
    #datadesativacao = Column(Date)
    #datacadastro = Column(Date)
    


    