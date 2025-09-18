from pydantic import BaseModel
from typing import Optional, ClassVar
from datetime import date

#Modelo de Retorno de Empresas Cadstradas (Get/Post/Put)
class EmpresaRetorno(BaseModel):
    codempresa: Optional[int]
    nomeempresa: str = None
    cnpjcpf: str = None
    tipobdempresa: str = None
    portabd: str = None
    ipbd: str = None
    caminhobd: str = None
    ativa: str = None
    datadesativacao: date = None
    datacadastro: date = None

    class Config:
        orm_mode = True
    

#Modelo de Body para Cadastro de Empresas (Post/Put)
class EmpresaCadastro(BaseModel):
    nomeempresa: str
    cnpjcpf: str
    tipobdempresa: str 
    portabd: str
    ipbd: str 
    caminhobd: str = None
    ativa: str 
   
    class Config:
        orm_mode = True
    
    