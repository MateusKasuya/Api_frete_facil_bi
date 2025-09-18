from pydantic import BaseModel, RootModel, Field
from typing import Optional, Dict, List, Union
from datetime import date


# Schema para parâmetros de consulta (entrada)
class FiltrosBI(BaseModel):
    data_inicio: Optional[date] = None
    data_fim: Optional[date] = None
    
    # Filtros por código (aceita valor único ou lista)
    codfilial: Optional[Union[List[int], int]] = Field(None, description="Código(s) da filial")
    codcliente: Optional[Union[List[str], str]] = Field(None, description="Código(s) do cliente")
    codcid: Optional[Union[List[int], int]] = Field(None, description="Código(s) da cidade (int)")
    codpro: Optional[Union[List[int], int]] = Field(None, description="Código(s) do produto")
    
    regiao: Optional[Union[List[str], str]] = Field(None, description="Nome(s) da região")

# Schema para resposta (saída) 
class BigNumbers(BaseModel):
    faturamento: float
    faturamento_ano_anterior: float
    volumes: float
    volumes_ano_anterior: float
    embarques: int
    embarques_ano_anterior: float
    ticket_medio: float
    ticket_medio_ano_anterior: float
    custos: float
    custos_ano_anterior: float
    pedagios: float
    pedagios_ano_anterior: float
    margem: float
    margem_ano_anterior: float  

# Schema individual para os dados de cada mês
class DadosMesAno(BaseModel):
    volume: float
    embarques: int
    faturamento: float

# Schema para a estrutura aninhada por ano
class KPIMesAno(RootModel[Dict[str, Dict[str, DadosMesAno]]]):
    pass

class DadosDiaMesAtual(BaseModel):
    volume: float
    embarques: int
    faturamento: float

# Schema para a estrutura aninhada por dia
class KPIDiaMesAtual(RootModel[Dict[str, DadosDiaMesAtual]]):
    pass

class DadosFilial(BaseModel):
    volume: float
    embarques: int
    faturamento: float

# Schema para a estrutura aninhada por dia
class KPIFilial(RootModel[Dict[str, DadosFilial]]):
    pass

class DadosRegiao(BaseModel):
    volume: float
    embarques: int
    faturamento: float

# Schema para a estrutura aninhada por dia
class KPIRegiao(RootModel[Dict[str, DadosRegiao]]):
    pass

class DadosCidade(BaseModel):
    volume: float
    embarques: int
    faturamento: float

# Schema para a estrutura aninhada por dia
class KPICidade(RootModel[Dict[str, DadosCidade]]):
    pass

class DadosCliente(BaseModel):
    faturamento: float
# Schema para a estrutura aninhada por dia
class KPICliente(RootModel[Dict[str, DadosCliente]]):
    pass

class DadosProduto(BaseModel):
    faturamento: float
# Schema para a estrutura aninhada por dia
class KPIProduto(RootModel[Dict[str, DadosProduto]]):
    pass

class TabelaFaturamento(BaseModel):
    nrofatura: int
    anofatura: int
    datarecbto: date
    faturamento: float
    filial: str
    cliente: str
    cidade: str
    coduf: str
    produto: str


    
