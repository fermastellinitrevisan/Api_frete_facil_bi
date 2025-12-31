from pydantic import BaseModel, RootModel, Field
from typing import Optional, Dict, List, Union
from datetime import date


# Schema para parâmetros de consulta (entrada)
class FiltrosBI(BaseModel):
    data_inicio: Optional[date] = None
    data_fim: Optional[date] = None
    ano: Optional[Union[List[int], int]] = Field(None, description="Ano")
    mes: Optional[Union[List[int], int]] = Field(None, description="Mês")
    dia: Optional[Union[List[int], int]] = Field(None, description="Dia")
    ano: Optional[Union[int, int]] = Field(None, description="Ano")
    mes: Optional[Union[int, int]] = Field(None, description="Mês")
    dia: Optional[Union[int, int]] = Field(None, description="Dia")
    
    # Filtros por código (aceita valor único ou lista)
    codfilial: Optional[Union[List[int], int]] = Field(None, description="Código(s) da filial")
    codcliente: Optional[Union[List[str], str]] = Field(None, description="Código(s) do cliente")
    codcid: Optional[Union[List[int], int]] = Field(None, description="Código(s) da cidade (int)")
    codpro: Optional[Union[List[int], int]] = Field(None, description="Código(s) do produto")
    
    regiao: Optional[Union[List[str], str]] = Field(None, description="Nome(s) da região")

<<<<<<< Updated upstream
    # Filtros cptit
    codfornecedor: Optional[Union[List[str], str]] = Field(None, description="Código(s) do fornecedor")
    codtransacao: Optional[Union[List[int], int]] = Field(None, description="Código(s) da transação")
=======
    class Config:
        orm_mode = True  
>>>>>>> Stashed changes

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
    mes: str
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
    filial: str
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
    cidade: str
    volume: float
    embarques: int
    faturamento: float

# Schema para a estrutura aninhada por dia
class KPICidade(RootModel[Dict[str, DadosCidade]]):
    pass

class DadosCliente(BaseModel):
    cliente: str
    faturamento: float
# Schema para a estrutura aninhada por dia
class KPICliente(RootModel[Dict[str, DadosCliente]]):
    pass

class DadosProduto(BaseModel):
    produto: str
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

class FiltroFilial(BaseModel):
    codfilial: str
    filial: str

class FiltroCliente(BaseModel):
    codcliente: str
    cliente: str

class BigNumbersContasReceber(BaseModel):
    faturamento: float
    a_receber: float
    em_atraso: float
    a_receber_total: float
    prazo_medio: float
        
class DadosRecebimentosDiaMesAtual(BaseModel):
    faturamento: float
    a_receber: float

# Schema para a estrutura aninhada por dia
class RecebimentosDiaMesAtual(RootModel[Dict[str, DadosRecebimentosDiaMesAtual]]):
    pass

class DadosAReceberCliente(BaseModel):
    cliente: str
    a_receber: float
# Schema para a estrutura aninhada por dia
class AReceberCliente(RootModel[Dict[str, DadosAReceberCliente]]):
    pass

class TabelaAReceber(BaseModel):
    datavencto: date
    cliente: str
    cidade: str
    coduf: str
    produto: str
    a_receber: float
    conta: str

class FiltroFornecedor(BaseModel):
    codfornecedor: str
    fornecedor: str

class FiltroTransacao(BaseModel):
    codtransacao: str
    transacao: str

class BigNumbersContasPagar(BaseModel):
    pago: float
    a_pagar: float
    em_atraso: float
    a_pagar_total: float

class DadosContasPagarDiaMesAtual(BaseModel):
    pago: float
    a_pagar: float

# Schema para a estrutura aninhada por dia
class ContasPagarDiaMesAtual(RootModel[Dict[str, DadosContasPagarDiaMesAtual]]):
    pass

class DadosAPagarFornecedor(BaseModel):
    fornecedor: str
    a_pagar: float
# Schema para a estrutura aninhada por dia
class APagarFornecedor(RootModel[Dict[str, DadosAPagarFornecedor]]):
    pass

class TabelaAPagar(BaseModel):
    datavencto: date
    fornecedor: str
    transacao: str
    a_pagar: float
    conta: str
