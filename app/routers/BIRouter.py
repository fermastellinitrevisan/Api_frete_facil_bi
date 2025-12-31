from urllib.request import Request
from fastapi import APIRouter, Depends, HTTPException, status, Request
from datetime import date, timedelta
from typing import List
from fastapi.security import OAuth2PasswordBearer 
from app.db.conexaopg import pg_connection_manager
from app.db.conexaofb import firebird_connection_manager
from app.auth.auth import decode_access_token
from app.schemas.BIschemas import *

router = APIRouter()

# Recurso para autenticação com token (OAuth2)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Função helper para normalizar filtros (converter valor único em lista)
def normalize_filter(value):
    if value is None:
        return None
    if isinstance(value, list):
        return value
    return [value]

# Função para obter os dados de conexão do Firebird
async def get_firebird_connection_data(idempresa: int):
    async with pg_connection_manager() as conn:
        # Recupera as informações de conexão do Firebird
        row = await conn.fetchrow(
            "select t.ipbd, t.portabd, t.caminhobd from tbempresas t where t.codempresa = $1", int(idempresa)
        )
        if not row:
            raise HTTPException(status_code=404, detail="Configuração de conexão não encontrada")
        return dict(row)

@router.post("/bi/big_numbers", tags=["BI"], response_model=List[BigNumbers], status_code=status.HTTP_200_OK)
async def get_big_numbers(
    request: Request,
    consulta: FiltrosBI,
    token: str = Depends(oauth2_scheme)
):

    """
    Consulta big numbers usando POST com schema de entrada.
    Permite consultas mais complexas no futuro.
    """
    # Verifica o token
    payload = decode_access_token(token)
    idempresa = payload.get("empresa")

    if not idempresa:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ID da empresa não encontrado no token")
    
    # Obtém os dados de conexão do Firebird
    conn_data = await get_firebird_connection_data(idempresa)

    # Usa o context manager para gerenciar a conexão automaticamente
    with firebird_connection_manager(conn_data['ipbd'], conn_data['portabd'], conn_data['caminhobd']) as (con, cur):

        # ← AQUI usa os campos do schema
        data_fim = consulta.data_fim or date.today()
        data_inicio = consulta.data_inicio or (data_fim - timedelta(days=30))

        params = [data_inicio, data_fim]

        # Consulta do ano anterior
        data_fim_ano_anterior = (consulta.data_fim - timedelta(days=365)) if consulta.data_fim else (date.today() - timedelta(days=365))
        data_inicio_ano_anterior = (consulta.data_inicio - timedelta(days=365)) if consulta.data_inicio else (data_fim_ano_anterior - timedelta(days=30))

        params_ano_anterior = [data_inicio_ano_anterior, data_fim_ano_anterior]


        query_factrc = """
            SELECT
	            SUM(vlrrecbto)
            FROM
	            vwfactrc_bi
            WHERE
                datarecbto >= ?
                AND datarecbto <= ?
        """


        query_frctrc = """
            SELECT
                SUM(vlrcusto),
                SUM(vlrpedagio),
                SUM(pesofrete_ton),
                SUM(embarque),
                SUM(faturado)
            FROM
                vwfrctrc_bi
            WHERE
                dataemissao >= ?
                AND dataemissao <= ?
        """

        # Normalizar filtros e aplicar
        codfilial = normalize_filter(consulta.codfilial)
        codcliente = normalize_filter(consulta.codcliente)

        # Filtros por código (prioridade)
        if codfilial:
            placeholders_filial = ', '.join(['?'] * len(codfilial))
            query_factrc += f" AND codfilial IN ({placeholders_filial})"
            query_frctrc += f" AND codfilial IN ({placeholders_filial})"
            params.extend(codfilial)
            params_ano_anterior.extend(codfilial)

        if codcliente:
            placeholders_cliente = ', '.join(['?'] * len(codcliente))
            query_factrc += f" AND codcliente IN ({placeholders_cliente})"
            query_frctrc += f" AND codcliente IN ({placeholders_cliente})"
            params.extend(codcliente)
            params_ano_anterior.extend(codcliente)


        # Filtro por cidade
        codcid = normalize_filter(consulta.codcid)
        if codcid:
            placeholders_codcid = ', '.join(['?'] * len(codcid))
            query_factrc += f" AND codcid IN ({placeholders_codcid})"
            query_frctrc += f" AND codcid IN ({placeholders_codcid})"
            params.extend(codcid)
            params_ano_anterior.extend(codcid)

        regiao = normalize_filter(consulta.regiao)
        if regiao:
            placeholders_regiao = ', '.join(['?'] * len(regiao))
            query_factrc += f" AND regiao IN ({placeholders_regiao})"
            query_frctrc += f" AND regiao IN ({placeholders_regiao})"
            params.extend(regiao)
            params_ano_anterior.extend(regiao)

        codpro = normalize_filter(consulta.codpro)
        if codpro:
            placeholders_codpro = ', '.join(['?'] * len(codpro))
            query_factrc += f" AND codpro IN ({placeholders_codpro})"
            query_frctrc += f" AND codpro IN ({placeholders_codpro})"
            params.extend(codpro)
            params_ano_anterior.extend(codpro)

        ano = normalize_filter(consulta.ano)
        if ano:
            placeholders_ano = ', '.join(['?'] * len(ano))
            query_factrc += f" AND ano_recbto IN ({placeholders_ano})"
            query_frctrc += f" AND ano_emissao IN ({placeholders_ano})"
            params.extend(ano)
            params_ano_anterior.extend(ano)

        mes = normalize_filter(consulta.mes)
        if mes:
            placeholders_mes = ', '.join(['?'] * len(mes))
            query_factrc += f" AND mes_numero IN ({placeholders_mes})"
            query_frctrc += f" AND mes_numero IN ({placeholders_mes})"
            params.extend(mes)
            params_ano_anterior.extend(mes)

        dia = normalize_filter(consulta.dia)
        if dia:
            placeholders_dia = ', '.join(['?'] * len(dia))
            query_factrc += f" AND dia_recbto IN ({placeholders_dia})"
            query_frctrc += f" AND dia_emissao IN ({placeholders_dia})"
            params.extend(dia)
            params_ano_anterior.extend(dia)

        cur.execute(query_factrc, tuple(params))
        resultado_factrc = cur.fetchall()
        faturamento = float(resultado_factrc[0][0]) if resultado_factrc and resultado_factrc[0][0] is not None else 0.0

        cur.execute(query_factrc, tuple(params_ano_anterior))
        resultado_factrc_ano_anterior = cur.fetchall()
        faturamento_ano_anterior = float(resultado_factrc_ano_anterior[0][0]) if resultado_factrc_ano_anterior and resultado_factrc_ano_anterior[0][0] is not None else 0.0
        
        cur.execute(query_frctrc, tuple(params))
        resultado = cur.fetchall()
        custos = float(resultado[0][0]) if resultado and resultado[0][0] is not None else 0.0
        pedagios = float(resultado[0][1]) if resultado and resultado[0][1] is not None else 0.0
        volumes = float(resultado[0][2]) if resultado and resultado[0][2] is not None else 0.0
        embarques = int(resultado[0][3]) if resultado and resultado[0][3] is not None else 0
        faturados = int(resultado[0][4]) if resultado and resultado[0][4] is not None else 0

        cur.execute(query_frctrc, tuple(params_ano_anterior))
        resultado_frctrc_ano_anterior = cur.fetchall()
        custos_ano_anterior = float(resultado_frctrc_ano_anterior[0][0]) if resultado_frctrc_ano_anterior and resultado_frctrc_ano_anterior[0][0] is not None else 0.0
        pedagios_ano_anterior = float(resultado_frctrc_ano_anterior[0][1]) if resultado_frctrc_ano_anterior and resultado_frctrc_ano_anterior[0][1] is not None else 0.0
        volumes_ano_anterior = float(resultado_frctrc_ano_anterior[0][2]) if resultado_frctrc_ano_anterior and resultado_frctrc_ano_anterior[0][2] is not None else 0.0
        embarques_ano_anterior = int(resultado_frctrc_ano_anterior[0][3]) if resultado_frctrc_ano_anterior and resultado_frctrc_ano_anterior[0][3] is not None else 0
        faturados_ano_anterior = int(resultado_frctrc_ano_anterior[0][4]) if resultado_frctrc_ano_anterior and resultado_frctrc_ano_anterior[0][4] is not None else 0
               
        # Combina os resultados
        dados = [
            {
                "faturamento": faturamento,
                "faturamento_ano_anterior": ((faturamento / faturamento_ano_anterior) -1) * 100 if faturamento_ano_anterior != 0 else 0.0,
                "volumes": volumes,
                "volumes_ano_anterior": ((volumes / volumes_ano_anterior) -1) * 100 if volumes_ano_anterior != 0 else 0.0,
                "embarques": embarques,
                "embarques_ano_anterior": ((embarques / embarques_ano_anterior) -1) * 100 if embarques_ano_anterior != 0 else 0.0,
                "ticket_medio": (faturamento / faturados) if faturados != 0 else 0.0,
                "ticket_medio_ano_anterior": (((faturamento / faturados) / (faturamento_ano_anterior / faturados_ano_anterior)) -1) * 100 if faturados != 0 and faturados_ano_anterior != 0 and faturamento_ano_anterior != 0 else 0.0,
                "custos": custos,
                "custos_ano_anterior": ((custos / custos_ano_anterior) -1) * 100 if custos_ano_anterior != 0 else 0.0,
                "pedagios": pedagios,
                "pedagios_ano_anterior": ((pedagios / pedagios_ano_anterior) -1) * 100 if pedagios_ano_anterior != 0 else 0.0,
                "margem": ((faturamento - custos) / faturamento) * 100 if faturamento != 0 else 0.0,
                "margem_ano_anterior": ((((faturamento - custos) / faturamento) / ((faturamento_ano_anterior - custos_ano_anterior) / faturamento_ano_anterior)) -1 ) * 100 if faturamento != 0 and faturamento_ano_anterior != 0 else 0.0,
            }
        ]

        # Para BI: sempre retorna dados, mesmo que zerados
        if not dados:
            # Retorna estrutura com zeros para BI
            dados = [
                {
                    "faturamento": 0.0,
                    "faturamento_ano_anterior": 0.0,
                    "volumes": 0.0,
                    "volumes_ano_anterior": 0.0,
                    "embarques": 0,
                    "embarques_ano_anterior": 0.0,
                    "ticket_medio": 0.0,
                    "ticket_medio_ano_anterior": 0.0,
                    "custos": 0.0,
                    "custos_ano_anterior": 0.0,
                    "pedagios": 0.0,
                    "pedagios_ano_anterior": 0.0,
                    "margem": 0.0,
                    "margem_ano_anterior": 0.0,
                }
            ]
        
        return dados

@router.post('/bi/kpi_mes_ano', tags=["BI"], response_model=KPIMesAno, status_code=status.HTTP_200_OK)
async def get_kpi_mes_ano(
    consulta: FiltrosBI,
    token: str = Depends(oauth2_scheme)
):
    """
    Consulta Grafico mês e ano de kpi usando POST com schema de entrada.
    Permite consultas mais complexas no futuro.
    """
    # Verifica o token
    payload = decode_access_token(token)
    idempresa = payload.get("empresa")

    if not idempresa:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ID da empresa não encontrado no token")
    
    # Obtém os dados de conexão do Firebird
    conn_data = await get_firebird_connection_data(idempresa)

    # Usa o context manager para gerenciar a conexão automaticamente
    with firebird_connection_manager(conn_data['ipbd'], conn_data['portabd'], conn_data['caminhobd']) as (con, cur):
        query = """
                SELECT
                    ano,
                    mes_numero,
                    mes,
                    SUM(volume),
                    SUM(embarque),
                    SUM(faturamento)
                FROM
                    (
                    SELECT
                        ano_emissao AS ano,
                        mes_emissao AS mes,
                        mes_numero,
                        dia_emissao AS dia,
                        pesofrete_ton AS volume,
                        embarque AS embarque,
                        0 AS faturamento,
                        codfilial,
                        codcid,
                        regiao
                    FROM
                        VWFRCTRC_BI
                    WHERE
                        ano_emissao >= EXTRACT(YEAR FROM CURRENT_TIMESTAMP) - 2
                UNION ALL
                    SELECT
                        ano_recbto AS ano,
                        mes_recbto AS mes,
                        mes_numero,
                        dia_recbto AS dia,
                        0 AS volume,
                        0 AS embarque,
                        vlrrecbto AS faturamento,
                        codfilial,
                        codcid,
                        regiao
                    FROM
                        VWFACTRC_BI
                    WHERE
                        ano_recbto >= EXTRACT(YEAR FROM CURRENT_TIMESTAMP) - 2
                ) dados
                WHERE 1=1
                GROUP BY
                    ano,
                    mes,
                    mes_numero
                ORDER BY
                    ano,
                    mes_numero
        """

        params = []

        # Aplicar filtros no WHERE externo (após o UNION)
        filtros_externos = ""
        
        # Normalizar filtros
        codfilial = normalize_filter(consulta.codfilial)
        codcid = normalize_filter(consulta.codcid)
        regiao = normalize_filter(consulta.regiao)

        # Aplicar filtros por filial
        if codfilial:
            placeholders_filial = ', '.join(['?'] * len(codfilial))
            filtros_externos += f" AND codfilial IN ({placeholders_filial})"
            params.extend(codfilial)

        # Aplicar filtros por cidade
        if codcid:
            placeholders_codcid = ', '.join(['?'] * len(codcid))
            filtros_externos += f" AND codcid IN ({placeholders_codcid})"
            params.extend(codcid)

        # Aplicar filtros por região
        if regiao:
            placeholders_regiao = ', '.join(['?'] * len(regiao))
            filtros_externos += f" AND CAST(regiao AS VARCHAR(50)) IN ({placeholders_regiao})"
            params.extend(regiao)

        ano = normalize_filter(consulta.ano)
        if ano:
            placeholders_ano = ', '.join(['?'] * len(ano))
            filtros_externos += f" AND ano IN ({placeholders_ano})"
            params.extend(ano)

        mes = normalize_filter(consulta.mes)
        if mes:
            placeholders_mes = ', '.join(['?'] * len(mes))
            filtros_externos += f" AND mes_numero IN ({placeholders_mes})"
            params.extend(mes)
        
        dia = normalize_filter(consulta.dia)
        if dia:
            placeholders_dia = ', '.join(['?'] * len(dia))
            filtros_externos += f" AND dia IN ({placeholders_dia})"
            params.extend(dia)

        # Inserir filtros no WHERE externo
        query = query.replace(
            "WHERE 1=1",
            f"WHERE 1=1{filtros_externos}"
        )

        cur.execute(query, tuple(params))

        # Dicionário para armazenar os dados organizados por ano e mês
        dados = {}
        
        for row in cur.fetchall():
            ano = str(int(row[0])) if row[0] is not None else "0"
            mes_numero = str(int(row[1])) if row[1] is not None else "0"
            mes = str(row[2]) if row[2] is not None else "Indefinido"
            volume = float(row[3]) if row[3] is not None else 0.0
            embarques = int(row[4]) if row[4] is not None else 0
            faturamento = float(row[5]) if row[5] is not None else 0.0
            
            # Inicializa o ano se não existir
            if ano not in dados:
                dados[ano] = {}
            
            # Adiciona os dados do mês
            dados[ano][mes_numero] = DadosMesAno(
                mes=mes,
                volume=volume,
                embarques=embarques,
                faturamento=faturamento
            )

        if not dados:
            # Para BI: retorna estrutura vazia em vez de erro 404
            return {}
        
        return dados

@router.post('/bi/kpi_dia_mes_atual', tags=["BI"], response_model=KPIDiaMesAtual, status_code=status.HTTP_200_OK)
async def get_kpi_dia_mes_atual(
    consulta: FiltrosBI = FiltrosBI(),
    token: str = Depends(oauth2_scheme)
):
    """
    Consulta Grafico dia e mes atual de kpi usando POST com schema de entrada.
    Permite consultas mais complexas no futuro.
    """
    # Verifica o token
    payload = decode_access_token(token)
    idempresa = payload.get("empresa")

    if not idempresa:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ID da empresa não encontrado no token")
    
    # Obtém os dados de conexão do Firebird
    conn_data = await get_firebird_connection_data(idempresa)

    # Usa o context manager para gerenciar a conexão automaticamente
    with firebird_connection_manager(conn_data['ipbd'], conn_data['portabd'], conn_data['caminhobd']) as (con, cur):
        query = """
                SELECT
                    dia,
                    SUM(volume),
                    SUM(embarques),
                    SUM(faturamento)
                FROM
                    (
                    SELECT
                        dia_emissao AS dia,
                        pesofrete_ton AS volume,
                        embarque AS embarques,
                        0 AS faturamento,
                        codfilial,
                        codcid,
                        regiao,
                        mes_numero,
                        ano_emissao AS ano
                FROM
                    VWFRCTRC_BI
                WHERE
                    ano_emissao = EXTRACT(YEAR FROM CURRENT_TIMESTAMP)
                    AND mes_numero = EXTRACT(MONTH FROM CURRENT_TIMESTAMP)
                UNION ALL
                    SELECT
                        dia_recbto AS dia,
                        0 AS volume,
                        0 AS embarques,
                        vlrrecbto AS faturamento,
                        codfilial,
                        codcid,
                        regiao,
                        mes_numero,
                        ano_recbto AS ano
                    FROM
                        VWFACTRC_BI
                    WHERE
                        ano_recbto = EXTRACT(YEAR FROM CURRENT_TIMESTAMP)
                        AND mes_numero = EXTRACT(MONTH FROM CURRENT_TIMESTAMP)
                ) dados
                WHERE 1=1
                GROUP BY
                    dia
                ORDER BY
                    dia
        """

        params = []

        # Aplicar filtros no WHERE externo (após o UNION) - igual kpi_mes_ano
        filtros_externos = ""
        
        # Normalizar filtros
        codfilial = normalize_filter(consulta.codfilial)
        codcid = normalize_filter(consulta.codcid)
        regiao = normalize_filter(consulta.regiao)

        # Aplicar filtros por filial
        if codfilial:
            placeholders_filial = ', '.join(['?'] * len(codfilial))
            filtros_externos += f" AND codfilial IN ({placeholders_filial})"
            params.extend(codfilial)

        # Aplicar filtros por cidade
        if codcid:
            placeholders_codcid = ', '.join(['?'] * len(codcid))
            filtros_externos += f" AND codcid IN ({placeholders_codcid})"
            params.extend(codcid)

        # Aplicar filtros por região
        if regiao:
            placeholders_regiao = ', '.join(['?'] * len(regiao))
            filtros_externos += f" AND CAST(regiao AS VARCHAR(50)) IN ({placeholders_regiao})"
            params.extend(regiao)

        ano = normalize_filter(consulta.ano)
        if ano:
            placeholders_ano = ', '.join(['?'] * len(ano))
            filtros_externos += f" AND ano IN ({placeholders_ano})"
            params.extend(ano)

        mes = normalize_filter(consulta.mes)
        if mes:
            placeholders_mes = ', '.join(['?'] * len(mes))
            filtros_externos += f" AND mes_numero IN ({placeholders_mes})"
            params.extend(mes)

        dia = normalize_filter(consulta.dia)
        if dia:
            placeholders_dia = ', '.join(['?'] * len(dia))
            filtros_externos += f" AND dia IN ({placeholders_dia})"
            params.extend(dia)

        # Inserir filtros no WHERE externo
        query = query.replace(
            "WHERE 1=1",
            f"WHERE 1=1{filtros_externos}"
        )

        cur.execute(query, tuple(params))

        # Dicionário para armazenar os dados organizados por dia
        dados = {}

        for row in cur.fetchall():
            dia = str(int(row[0])) if row[0] is not None else "0"
            volume = float(row[1]) if row[1] is not None else 0.0
            embarques = int(row[2]) if row[2] is not None else 0
            faturamento = float(row[3]) if row[3] is not None else 0.0
            
            # Adiciona os dados do dia
            dados[dia] = DadosDiaMesAtual(
                volume=volume,
                embarques=embarques,
                faturamento=faturamento
            )

        if not dados:
            return {}
        
        return dados

@router.post("/bi/kpi_filial", tags=["BI"], response_model=KPIFilial, status_code=status.HTTP_200_OK)
async def get_kpi_filial(
    consulta: FiltrosBI = FiltrosBI(),
    token: str = Depends(oauth2_scheme)
):

    """
    Consulta kpi filial usando POST com schema de entrada.
    Permite consultas mais complexas no futuro.
    """
    # Verifica o token
    payload = decode_access_token(token)
    idempresa = payload.get("empresa")

    if not idempresa:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ID da empresa não encontrado no token")
    
    # Obtém os dados de conexão do Firebird
    conn_data = await get_firebird_connection_data(idempresa)

    # Usa o context manager para gerenciar a conexão automaticamente
    with firebird_connection_manager(conn_data['ipbd'], conn_data['portabd'], conn_data['caminhobd']) as (con, cur):

        # ← AQUI usa os campos do schema
        data_fim = consulta.data_fim or date.today()
        data_inicio = consulta.data_inicio or (data_fim - timedelta(days=30))

        query= """
        SELECT
            codfilial,
            filial,
            SUM(volume),
            SUM(embarques),
            SUM(faturamento)
        FROM
            (
            SELECT
                filial,
                0 AS volume,
                0 AS embarques,
                vlrrecbto AS faturamento,
                codfilial,
                codcliente,
                codcid,
                regiao,
                codpro,
                datarecbto AS data_operacao,
                dia_recbto AS dia,
                mes_numero,
                ano_recbto AS ano
            FROM
                VWFACTRC_BI
        UNION ALL
            SELECT
                filial,
                pesofrete_ton AS volume,
                embarque AS embarques,
                0 AS faturamento,
                codfilial,
                codcliente,
                codcid,
                regiao,
                codpro,
                dataemissao AS data_operacao,
                dia_emissao AS dia,
                mes_numero,
                ano_emissao AS ano
            FROM
                VWFRCTRC_BI
        ) dados
        WHERE data_operacao >= ? AND data_operacao <= ?
        GROUP BY
            codfilial,
            filial
        ORDER BY SUM(faturamento) DESC
        """

        params = []

        # Aplicar filtros no WHERE externo (mesma lógica dos outros endpoints)
        filtros_externos = ""
        
        params.extend([data_inicio, data_fim])

        # Normalizar filtros
        codfilial = normalize_filter(consulta.codfilial)
        codcliente = normalize_filter(consulta.codcliente)
        codcid = normalize_filter(consulta.codcid)
        regiao = normalize_filter(consulta.regiao)
        codpro = normalize_filter(consulta.codpro)
        ano = normalize_filter(consulta.ano)
        mes = normalize_filter(consulta.mes)
        dia = normalize_filter(consulta.dia)

        # Aplicar filtros por filial
        if codfilial:
            placeholders_filial = ', '.join(['?'] * len(codfilial))
            filtros_externos += f" AND codfilial IN ({placeholders_filial})"
            params.extend(codfilial)

        # Aplicar filtros por cliente
        if codcliente:
            placeholders_cliente = ', '.join(['?'] * len(codcliente))
            filtros_externos += f" AND codcliente IN ({placeholders_cliente})"
            params.extend(codcliente)

        # Aplicar filtros por cidade
        if codcid:
            placeholders_codcid = ', '.join(['?'] * len(codcid))
            filtros_externos += f" AND codcid IN ({placeholders_codcid})"
            params.extend(codcid)

        # Aplicar filtros por região
        if regiao:
            placeholders_regiao = ', '.join(['?'] * len(regiao))
            filtros_externos += f" AND CAST(regiao AS VARCHAR(50)) IN ({placeholders_regiao})"
            params.extend(regiao)

        # Aplicar filtros por produto
        if codpro:
            placeholders_codpro = ', '.join(['?'] * len(codpro))
            filtros_externos += f" AND codpro IN ({placeholders_codpro})"
            params.extend(codpro)

        # Aplicar filtros por ano

        if ano:
            placeholders_ano = ', '.join(['?'] * len(ano))
            filtros_externos += f" AND ano IN ({placeholders_ano})"
            params.extend(ano)

        # Aplicar filtros por mes

        if mes:
            placeholders_mes = ', '.join(['?'] * len(mes))
            filtros_externos += f" AND mes_numero IN ({placeholders_mes})"
            params.extend(mes)

        # Aplicar filtros por dia

        if dia:
            placeholders_dia = ', '.join(['?'] * len(dia))
            filtros_externos += f" AND dia IN ({placeholders_dia})"
            params.extend(dia)

        # Inserir filtros no WHERE externo
        query = query.replace(
            "WHERE data_operacao >= ? AND data_operacao <= ?",
            f"WHERE data_operacao >= ? AND data_operacao <= ?{filtros_externos}"
        )

        cur.execute(query, tuple(params))

        # Dicionário para armazenar os dados organizados por filial
        dados = {}

        for row in cur.fetchall():
            codfilial = str(row[0]) if row[0] is not None else None
            filial = str(row[1]) if row[1] is not None else None
            volume = float(row[2]) if row[2] is not None else 0.0
            embarques = int(row[3]) if row[3] is not None else 0
            faturamento = float(row[4]) if row[4] is not None else 0.0
            
            # Adiciona os dados do filial
            dados[codfilial] = DadosFilial(
                filial=filial,
                volume=volume,
                embarques=embarques,
                faturamento=faturamento
            )

        if not dados:
            return {}
        
        return dados

@router.post("/bi/kpi_regiao", tags=["BI"], response_model=KPIRegiao, status_code=status.HTTP_200_OK)
async def get_kpi_regiao(
    consulta: FiltrosBI = FiltrosBI(),
    token: str = Depends(oauth2_scheme)
):

    """
    Consulta kpi regiao usando POST com schema de entrada.
    Permite consultas mais complexas no futuro.
    """
    # Verifica o token
    payload = decode_access_token(token)
    idempresa = payload.get("empresa")

    if not idempresa:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ID da empresa não encontrado no token")
    
    # Obtém os dados de conexão do Firebird
    conn_data = await get_firebird_connection_data(idempresa)

    # Usa o context manager para gerenciar a conexão automaticamente
    with firebird_connection_manager(conn_data['ipbd'], conn_data['portabd'], conn_data['caminhobd']) as (con, cur):

        # ← AQUI usa os campos do schema
        data_fim = consulta.data_fim or date.today()
        data_inicio = consulta.data_inicio or (data_fim - timedelta(days=30))

        query= """
        SELECT
            regiao,
            SUM(volume),
            SUM(embarques),
            SUM(faturamento)
        FROM
            (
            SELECT
                regiao,
                0 AS volume,
                0 AS embarques,
                vlrrecbto AS faturamento,
                codfilial,
                codcliente,
                codcid,
                codpro,
                datarecbto AS data_operacao,
                dia_recbto AS dia,
                mes_numero,
                ano_recbto AS ano
        FROM
            VWFACTRC_BI
        UNION ALL
            SELECT
                regiao,
                pesofrete_ton AS volume,
                embarque AS embarques,
                0 AS faturamento,
                codfilial,
                codcliente,
                codcid,
                codpro,
                dataemissao AS data_operacao,
                dia_emissao AS dia,
                mes_numero,
                ano_emissao AS ano
            FROM
                VWFRCTRC_BI
        ) dados
        WHERE data_operacao >= ? AND data_operacao <= ?
        GROUP BY
            regiao
        ORDER BY SUM(faturamento) DESC
        """

        params = []

        # Aplicar filtros no WHERE externo (mesma lógica dos outros endpoints)
        filtros_externos = ""
        
        params.extend([data_inicio, data_fim])
        
        # Normalizar filtros
        codfilial = normalize_filter(consulta.codfilial)
        codcliente = normalize_filter(consulta.codcliente)
        codcid = normalize_filter(consulta.codcid)
        regiao = normalize_filter(consulta.regiao)
        codpro = normalize_filter(consulta.codpro)
        ano = normalize_filter(consulta.ano)
        mes = normalize_filter(consulta.mes)
        dia = normalize_filter(consulta.dia)

        # Aplicar filtros por filial
        if codfilial:
            placeholders_filial = ', '.join(['?'] * len(codfilial))
            filtros_externos += f" AND codfilial IN ({placeholders_filial})"
            params.extend(codfilial)

        # Aplicar filtros por cliente
        if codcliente:
            placeholders_cliente = ', '.join(['?'] * len(codcliente))
            filtros_externos += f" AND codcliente IN ({placeholders_cliente})"
            params.extend(codcliente)

        # Aplicar filtros por cidade
        if codcid:
            placeholders_codcid = ', '.join(['?'] * len(codcid))
            filtros_externos += f" AND codcid IN ({placeholders_codcid})"
            params.extend(codcid)

        # Aplicar filtros por região
        if regiao:
            placeholders_regiao = ', '.join(['?'] * len(regiao))
            filtros_externos += f" AND CAST(regiao AS VARCHAR(50)) IN ({placeholders_regiao})"
            params.extend(regiao)

        # Aplicar filtros por produto
        if codpro:
            placeholders_codpro = ', '.join(['?'] * len(codpro))
            filtros_externos += f" AND codpro IN ({placeholders_codpro})"
            params.extend(codpro)

        # Aplicar filtros por ano

        if ano:
            placeholders_ano = ', '.join(['?'] * len(ano))
            filtros_externos += f" AND ano IN ({placeholders_ano})"
            params.extend(ano)

        # Aplicar filtros por mes

        if mes:
            placeholders_mes = ', '.join(['?'] * len(mes))
            filtros_externos += f" AND mes_numero IN ({placeholders_mes})"
            params.extend(mes)

        # Aplicar filtros por dia

        if dia:
            placeholders_dia = ', '.join(['?'] * len(dia))
            filtros_externos += f" AND dia IN ({placeholders_dia})"
            params.extend(dia)

        # Inserir filtros no WHERE externo
        query = query.replace(
            "WHERE data_operacao >= ? AND data_operacao <= ?",
            f"WHERE data_operacao >= ? AND data_operacao <= ?{filtros_externos}"
        )

        cur.execute(query, tuple(params))

                # Dicionário para armazenar os dados organizados por regiao
        dados = {}

        for row in cur.fetchall():
            regiao = str(row[0]) if row[0] is not None else None
            volume = float(row[1]) if row[1] is not None else 0.0
            embarques = int(row[2]) if row[2] is not None else 0
            faturamento = float(row[3]) if row[3] is not None else 0.0
            
            # Adiciona os dados do regiao
            dados[regiao] = DadosRegiao(
                volume=volume,
                embarques=embarques,
                faturamento=faturamento
            )

        if not dados:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Nenhum dado encontrado")
        
        return dados

@router.post("/bi/kpi_cidade", tags=["BI"], response_model=KPICidade, status_code=status.HTTP_200_OK)
async def get_kpi_cidade(
    consulta: FiltrosBI = FiltrosBI(),
    token: str = Depends(oauth2_scheme)
):

    """
    Consulta kpi cidade usando POST com schema de entrada.
    Permite consultas mais complexas no futuro.
    """
    # Verifica o token
    payload = decode_access_token(token)
    idempresa = payload.get("empresa")

    if not idempresa:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ID da empresa não encontrado no token")
    
    # Obtém os dados de conexão do Firebird
    conn_data = await get_firebird_connection_data(idempresa)

    # Usa o context manager para gerenciar a conexão automaticamente
    with firebird_connection_manager(conn_data['ipbd'], conn_data['portabd'], conn_data['caminhobd']) as (con, cur):

        # ← AQUI usa os campos do schema
        data_fim = consulta.data_fim or date.today()
        data_inicio = consulta.data_inicio or (data_fim - timedelta(days=30))

        query= """
            SELECT
                codcid,
                cidade || '-' || coduf,
                SUM(volume),
                SUM(embarques),
                SUM(faturamento)
            FROM
                (
                SELECT
                    cidade,
                    coduf,
                    0 AS volume,
                    0 AS embarques,
                    vlrrecbto AS faturamento,
                    codfilial,
                    codcid,
                    regiao,
                    datarecbto AS data_operacao,
                    dia_recbto AS dia,
                    mes_numero,
                    ano_recbto AS ano
                FROM
                    VWFACTRC_BI
            UNION ALL
                SELECT
                    cidade,
                    coduf,
                    pesofrete_ton AS volume,
                    embarque AS embarques,
                    0 AS faturamento,
                    codfilial,
                    codcid,
                    regiao,
                    dataemissao AS data_operacao,
                    dia_emissao AS dia,
                    mes_numero,
                    ano_emissao AS ano
                FROM
                    VWFRCTRC_BI
            ) dados
            WHERE data_operacao >= ? AND data_operacao <= ?
            GROUP BY
                codcid,
                cidade || '-' || coduf
            ORDER BY SUM(faturamento) DESC
        """

        params = []

        # Aplicar filtros no WHERE externo (mesma lógica dos outros endpoints)
        filtros_externos = ""
        
        params.extend([data_inicio, data_fim])
        
        # Normalizar filtros
        codfilial = normalize_filter(consulta.codfilial)
        codcid = normalize_filter(consulta.codcid)
        regiao = normalize_filter(consulta.regiao)
        ano = normalize_filter(consulta.ano)
        mes = normalize_filter(consulta.mes)
        dia = normalize_filter(consulta.dia)

        # Aplicar filtros por filial
        if codfilial:
            placeholders_filial = ', '.join(['?'] * len(codfilial))
            filtros_externos += f" AND codfilial IN ({placeholders_filial})"
            params.extend(codfilial)

        # Aplicar filtros por cidade
        if codcid:
            placeholders_codcid = ', '.join(['?'] * len(codcid))
            filtros_externos += f" AND codcid IN ({placeholders_codcid})"
            params.extend(codcid)

        # Aplicar filtros por região
        if regiao:
            placeholders_regiao = ', '.join(['?'] * len(regiao))
            filtros_externos += f" AND CAST(regiao AS VARCHAR(50)) IN ({placeholders_regiao})"
            params.extend(regiao)

        # Aplicar filtros por ano

        if ano:
            placeholders_ano = ', '.join(['?'] * len(ano))
            filtros_externos += f" AND ano IN ({placeholders_ano})"
            params.extend(ano)

        # Aplicar filtros por mes

        if mes:
            placeholders_mes = ', '.join(['?'] * len(mes))
            filtros_externos += f" AND mes_numero IN ({placeholders_mes})"
            params.extend(mes)

        # Aplicar filtros por dia

        if dia:
            placeholders_dia = ', '.join(['?'] * len(dia))
            filtros_externos += f" AND dia IN ({placeholders_dia})"
            params.extend(dia)

        # Inserir filtros no WHERE externo
        query = query.replace(
            "WHERE data_operacao >= ? AND data_operacao <= ?",
            f"WHERE data_operacao >= ? AND data_operacao <= ?{filtros_externos}"
        )

        cur.execute(query, tuple(params))

                # Dicionário para armazenar os dados organizados por cidade
        dados = {}

        for row in cur.fetchall():
            codcid = str(row[0]) if row[0] is not None else None
            cidade = str(row[1]) if row[1] is not None else None
            volume = float(row[2]) if row[2] is not None else 0.0
            embarques = int(row[3]) if row[3] is not None else 0
            faturamento = float(row[4]) if row[4] is not None else 0.0
            
            # Adiciona os dados do cidade
            dados[codcid] = DadosCidade(
                cidade=cidade,
                volume=volume,
                embarques=embarques,
                faturamento=faturamento
            )

        if not dados:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Nenhum dado encontrado")
        
        return dados

@router.post("/bi/kpi_cliente", tags=["BI"], response_model=KPICliente, status_code=status.HTTP_200_OK)
async def get_kpi_cliente(
    consulta: FiltrosBI = FiltrosBI(),
    token: str = Depends(oauth2_scheme)
):

    """
    Consulta kpi cliente usando POST com schema de entrada.
    Permite consultas mais complexas no futuro.
    """
    # Verifica o token
    payload = decode_access_token(token)
    idempresa = payload.get("empresa")

    if not idempresa:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ID da empresa não encontrado no token")
    
    # Obtém os dados de conexão do Firebird
    conn_data = await get_firebird_connection_data(idempresa)

    # Usa o context manager para gerenciar a conexão automaticamente
    with firebird_connection_manager(conn_data['ipbd'], conn_data['portabd'], conn_data['caminhobd']) as (con, cur):

        # ← AQUI usa os campos do schema
        data_fim = consulta.data_fim or date.today()
        data_inicio = consulta.data_inicio or (data_fim - timedelta(days=30))

        query= """
            SELECT
                codcliente,
                cliente,
                SUM(vlrrecbto)
            FROM
                (
                SELECT
                    cliente,
                    vlrrecbto,
                    codfilial,
                    codcliente,
                    regiao,
                    codpro,
                    datarecbto
                FROM
                    VWFACTRC_BI
            ) dados
            WHERE datarecbto >= ? AND datarecbto <= ?
            GROUP BY
                codcliente,
                cliente
            ORDER BY
                SUM(vlrrecbto) DESC
        """

        params = []

        # Aplicar filtros no WHERE externo (mesma lógica dos outros endpoints)
        filtros_externos = ""
        
        params.extend([data_inicio, data_fim])
        
        # Normalizar filtros
        codfilial = normalize_filter(consulta.codfilial)
        codcliente = normalize_filter(consulta.codcliente)
        regiao = normalize_filter(consulta.regiao)
        codpro = normalize_filter(consulta.codpro)

        # Aplicar filtros por filial
        if codfilial:
            placeholders_filial = ', '.join(['?'] * len(codfilial))
            filtros_externos += f" AND codfilial IN ({placeholders_filial})"
            params.extend(codfilial)

        # Aplicar filtros por cliente
        if codcliente:
            placeholders_cliente = ', '.join(['?'] * len(codcliente))
            filtros_externos += f" AND codcliente IN ({placeholders_cliente})"
            params.extend(codcliente)

        # Aplicar filtros por região
        if regiao:
            placeholders_regiao = ', '.join(['?'] * len(regiao))
            filtros_externos += f" AND CAST(regiao AS VARCHAR(50)) IN ({placeholders_regiao})"
            params.extend(regiao)

        # Aplicar filtros por produto
        if codpro:
            placeholders_codpro = ', '.join(['?'] * len(codpro))
            filtros_externos += f" AND codpro IN ({placeholders_codpro})"
            params.extend(codpro)

        # Inserir filtros no WHERE externo
        query = query.replace(
            "WHERE datarecbto >= ? AND datarecbto <= ?",
            f"WHERE datarecbto >= ? AND datarecbto <= ?{filtros_externos}"
        )


        cur.execute(query, tuple(params))

                # Dicionário para armazenar os dados organizados por cliente
        dados = {}

        for row in cur.fetchall():
            codcliente = str(row[0]) if row[0] is not None else None
            cliente = str(row[1]) if row[1] is not None else None
            faturamento = float(row[2]) if row[2] is not None else 0.0
            
            # Adiciona os dados do cliente
            dados[codcliente] = DadosCliente(
                cliente=cliente,
                faturamento=faturamento
            )

        if not dados:
            return {}
        
        return dados

@router.post("/bi/kpi_produto", tags=["BI"], response_model=KPIProduto, status_code=status.HTTP_200_OK)
async def get_kpi_produto(
    consulta: FiltrosBI = FiltrosBI(),
    token: str = Depends(oauth2_scheme)
):

    """
    Consulta kpi produto usando POST com schema de entrada.
    Permite consultas mais complexas no futuro.
    """
    # Verifica o token
    payload = decode_access_token(token)
    idempresa = payload.get("empresa")

    if not idempresa:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ID da empresa não encontrado no token")
    
    # Obtém os dados de conexão do Firebird
    conn_data = await get_firebird_connection_data(idempresa)

    # Usa o context manager para gerenciar a conexão automaticamente
    with firebird_connection_manager(conn_data['ipbd'], conn_data['portabd'], conn_data['caminhobd']) as (con, cur):

        # ← AQUI usa os campos do schema
        data_fim = consulta.data_fim or date.today()
        data_inicio = consulta.data_inicio or (data_fim - timedelta(days=30))

        query= """
            SELECT
                codpro,
                produto,
                SUM(vlrrecbto)
            FROM
                (
                SELECT
                    produto,
                    vlrrecbto,
                    codfilial,
                    codcliente,
                    regiao,
                    codpro,
                    datarecbto
                FROM
                    VWFACTRC_BI
            )
            WHERE datarecbto >= ? AND datarecbto <= ?
            GROUP BY
                codpro,
                produto
            ORDER BY
                SUM(vlrrecbto) DESC
        """

        params = []

        # Aplicar filtros no WHERE externo (mesma lógica dos outros endpoints)
        filtros_externos = ""
        
        params.extend([data_inicio, data_fim])
        
        # Normalizar filtros
        codfilial = normalize_filter(consulta.codfilial)
        codcliente = normalize_filter(consulta.codcliente)
        regiao = normalize_filter(consulta.regiao)
        codpro = normalize_filter(consulta.codpro)

        # Aplicar filtros por filial
        if codfilial:
            placeholders_filial = ', '.join(['?'] * len(codfilial))
            filtros_externos += f" AND codfilial IN ({placeholders_filial})"
            params.extend(codfilial)

        # Aplicar filtros por cliente
        if codcliente:
            placeholders_cliente = ', '.join(['?'] * len(codcliente))
            filtros_externos += f" AND codcliente IN ({placeholders_cliente})"
            params.extend(codcliente)

        # Aplicar filtros por região
        if regiao:
            placeholders_regiao = ', '.join(['?'] * len(regiao))
            filtros_externos += f" AND CAST(regiao AS VARCHAR(50)) IN ({placeholders_regiao})"
            params.extend(regiao)

        # Aplicar filtros por produto
        if codpro:
            placeholders_codpro = ', '.join(['?'] * len(codpro))
            filtros_externos += f" AND codpro IN ({placeholders_codpro})"
            params.extend(codpro)

        # Inserir filtros no WHERE externo
        query = query.replace(
            "WHERE datarecbto >= ? AND datarecbto <= ?",
            f"WHERE datarecbto >= ? AND datarecbto <= ?{filtros_externos}"
        )

        cur.execute(query, tuple(params))

                # Dicionário para armazenar os dados organizados por produto
        dados = {}

        for row in cur.fetchall():
            codpro = str(row[0]) if row[0] is not None else None
            produto = str(row[1]) if row[1] is not None else None
            faturamento = float(row[2]) if row[2] is not None else 0.0
            
            # Adiciona os dados do produto
            dados[codpro] = DadosProduto(
                produto=produto,
                faturamento=faturamento
            )

        if not dados:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Nenhum dado encontrado")
        
        return dados

@router.post("/bi/tabela_faturamento", tags=["BI"], response_model=List[TabelaFaturamento], status_code=status.HTTP_200_OK)
async def get_tabela_faturamento(
    consulta: FiltrosBI = FiltrosBI(),
    token: str = Depends(oauth2_scheme)
):

    """
    Consulta tabela de faturamento usando POST com schema de entrada.
    Permite consultas mais complexas no futuro.
    """
    # Verifica o token
    payload = decode_access_token(token)
    idempresa = payload.get("empresa")

    if not idempresa:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ID da empresa não encontrado no token")
    
    # Obtém os dados de conexão do Firebird
    conn_data = await get_firebird_connection_data(idempresa)

    # Usa o context manager para gerenciar a conexão automaticamente
    with firebird_connection_manager(conn_data['ipbd'], conn_data['portabd'], conn_data['caminhobd']) as (con, cur):

        # ← AQUI usa os campos do schema
        data_fim = consulta.data_fim or date.today()
        data_inicio = consulta.data_inicio or (data_fim - timedelta(days=30))

        query= """
            SELECT
                nrofatura,
                anofatura,
                datarecbto,
                vlrrecbto,
                filial,
                cliente,
                cidade,
                coduf,
                produto
            FROM
                vwfactrc_bi
            WHERE datarecbto >= ? AND datarecbto <= ?
        """

        params = []

        # Aplicar filtros no WHERE externo (mesma lógica dos outros endpoints)
        filtros_externos = ""
        
        params.extend([data_inicio, data_fim])
        
        # Normalizar filtros
        codfilial = normalize_filter(consulta.codfilial)
        codcliente = normalize_filter(consulta.codcliente)
        regiao = normalize_filter(consulta.regiao)
        codpro = normalize_filter(consulta.codpro)

        # Aplicar filtros por filial
        if codfilial:
            placeholders_filial = ', '.join(['?'] * len(codfilial))
            filtros_externos += f" AND codfilial IN ({placeholders_filial})"
            params.extend(codfilial)

        # Aplicar filtros por cliente
        if codcliente:
            placeholders_cliente = ', '.join(['?'] * len(codcliente))
            filtros_externos += f" AND codcliente IN ({placeholders_cliente})"
            params.extend(codcliente)

        # Aplicar filtros por região
        if regiao:
            placeholders_regiao = ', '.join(['?'] * len(regiao))
            filtros_externos += f" AND CAST(regiao AS VARCHAR(50)) IN ({placeholders_regiao})"
            params.extend(regiao)

        # Aplicar filtros por produto
        if codpro:
            placeholders_codpro = ', '.join(['?'] * len(codpro))
            filtros_externos += f" AND codpro IN ({placeholders_codpro})"
            params.extend(codpro)

        # Inserir filtros no WHERE externo
        query = query.replace(
            "WHERE datarecbto >= ? AND datarecbto <= ?",
            f"WHERE datarecbto >= ? AND datarecbto <= ?{filtros_externos}"
        )

        cur.execute(query, tuple(params))
                     # Combina os resultados
        dados = [
            {
                "nrofatura": str(row[0]) if row[0] is not None else None,
                "anofatura": str(row[1]) if row[1] is not None else None,
                "datarecbto": str(row[2]) if row[2] is not None else None,
                "faturamento": float(row[3]) if row[3] is not None else 0.0,
                "filial": str(row[4]) if row[4] is not None else None,
                "cliente": str(row[5]) if row[5] is not None else None,
                "cidade": str(row[6]) if row[6] is not None else None,
                "coduf": str(row[7]) if row[7] is not None else None,
                "produto": str(row[8]) if row[8] is not None else None
            }
            for row in cur.fetchall()
        ]
              

        if not dados:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Nenhum dado encontrado")
        
        return dados

@router.get("/bi/filtro_filial", tags=["BI"], response_model=List[FiltroFilial], status_code=status.HTTP_200_OK)
async def get_filtro_filial(
    token: str = Depends(oauth2_scheme)
):

    """
        Consulta filtro filial usando GET com schema de entrada.
    """

  # Verifica o token
    payload = decode_access_token(token)
    idempresa = payload.get("empresa")

    if not idempresa:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ID da empresa não encontrado no token")
    
    # Obtém os dados de conexão do Firebird
    conn_data = await get_firebird_connection_data(idempresa)

    # Usa o context manager para gerenciar a conexão automaticamente
    with firebird_connection_manager(conn_data['ipbd'], conn_data['portabd'], conn_data['caminhobd']) as (con, cur):

        query= """
            SELECT
                codfil,
                nome
            FROM
                TBFIL
        """

        cur.execute(query)

        dados = [
            {
                "codfilial": str(row[0]) if row[0] is not None else None,
                "filial": str(row[1]) if row[1] is not None else None
            }
            for row in cur.fetchall()
        ]
              

        if not dados:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Nenhum dado encontrado")
        
        return dados

@router.get("/bi/filtro_cliente", tags=["BI"], response_model=List[FiltroCliente], status_code=status.HTTP_200_OK)
async def get_filtro_cliente(
    token: str = Depends(oauth2_scheme)
):

    """
        Consulta filtro cliente usando GET com schema de entrada.
    """

  # Verifica o token
    payload = decode_access_token(token)
    idempresa = payload.get("empresa")

    if not idempresa:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ID da empresa não encontrado no token")
    
    # Obtém os dados de conexão do Firebird
    conn_data = await get_firebird_connection_data(idempresa)

    # Usa o context manager para gerenciar a conexão automaticamente
    with firebird_connection_manager(conn_data['ipbd'], conn_data['portabd'], conn_data['caminhobd']) as (con, cur):

        query= """
            SELECT
                cgccpfcli,
                nomefantasia
            FROM
                TBCLI
        """

        cur.execute(query)

        dados = [
            {
                "codcliente": str(row[0]) if row[0] is not None else None,
                "cliente": str(row[1]) if row[1] is not None else None
            }
            for row in cur.fetchall()
        ]
              

        if not dados:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Nenhum dado encontrado")
        
        return dados

@router.post("/bi/big_numbers_contas_receber", tags=["BI"], response_model=List[BigNumbersContasReceber], status_code=status.HTTP_200_OK)
async def get_big_numbers_contas_receber(
    consulta: FiltrosBI = FiltrosBI(),
    token: str = Depends(oauth2_scheme)
):

    """
    Consulta big numbers contas receber usando POST com schema de entrada.
    Permite consultas mais complexas no futuro.
    """
    # Verifica o token
    payload = decode_access_token(token)
    idempresa = payload.get("empresa")

    if not idempresa:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ID da empresa não encontrado no token")
    
    # Obtém os dados de conexão do Firebird
    conn_data = await get_firebird_connection_data(idempresa)

    # Usa o context manager para gerenciar a conexão automaticamente
    with firebird_connection_manager(conn_data['ipbd'], conn_data['portabd'], conn_data['caminhobd']) as (con, cur):

        # ← AQUI usa os campos do schema
        data_fim = consulta.data_fim or date.today()
        data_inicio = consulta.data_inicio or (data_fim - timedelta(days=30))

        # Normalizar filtros para aplicar em todas as queries
        codfilial = normalize_filter(consulta.codfilial)
        codcliente = normalize_filter(consulta.codcliente)

        # Construir filtros adicionais para aplicar nas queries
        filtros_adicionais = ""
        params_filtros = []

        if codfilial:
            placeholders_filial = ', '.join(['?'] * len(codfilial))
            filtros_adicionais += f" AND codfilial IN ({placeholders_filial})"
            params_filtros.extend(codfilial)

        if codcliente:
            placeholders_cliente = ', '.join(['?'] * len(codcliente))
            filtros_adicionais += f" AND codcliente IN ({placeholders_cliente})"
            params_filtros.extend(codcliente)

        # 1. FATURAMENTO: Filtrado por período de recebimento
        query_faturamento = f"""
            SELECT COALESCE(SUM(vlrrecbto), 0) AS faturamento
            FROM VWFACTRC_BI
            WHERE datarecbto >= ? AND datarecbto <= ?{filtros_adicionais}
        """
        params_faturamento = [data_inicio, data_fim] + params_filtros

        # 2. A RECEBER: Filtrado por período de vencimento
        query_a_receber = f"""
            SELECT COALESCE(SUM(vlrsaldo), 0) AS a_receber
            FROM VWFACTRC_BI
            WHERE condicao_fatura = 'A Receber'
              AND datavencto >= ? AND datavencto <= ?{filtros_adicionais}
        """
        params_a_receber = [data_inicio, data_fim] + params_filtros

        # 3. EM ATRASO: SEM filtro de data (sempre atual)
        query_em_atraso = f"""
            SELECT COALESCE(SUM(vlrsaldo), 0) AS em_atraso
            FROM VWFACTRC_BI
            WHERE condicao_fatura = 'Em Atraso'{filtros_adicionais}
        """
        params_em_atraso = params_filtros

        # 4. PRAZO MÉDIO: Filtrado por período de recebimento
        query_prazo_medio = f"""
            SELECT COALESCE(AVG(dias_recebimento), 0) AS prazo_medio
            FROM VWFACTRC_BI
            WHERE datarecbto >= ? AND datarecbto <= ?
              AND dias_recebimento IS NOT NULL{filtros_adicionais}
        """
        params_prazo_medio = [data_inicio, data_fim] + params_filtros

        # Executar queries separadamente
        cur.execute(query_faturamento, tuple(params_faturamento))
        faturamento = cur.fetchone()[0] or 0.0

        cur.execute(query_a_receber, tuple(params_a_receber))
        a_receber = cur.fetchone()[0] or 0.0

        cur.execute(query_em_atraso, tuple(params_em_atraso))
        em_atraso = cur.fetchone()[0] or 0.0

        cur.execute(query_prazo_medio, tuple(params_prazo_medio))
        prazo_medio = cur.fetchone()[0] or 0.0

        # Retornar dados combinados
        dados = [
            {
                "faturamento": float(faturamento),
                "a_receber": float(a_receber),
                "em_atraso": float(em_atraso),
                "a_receber_total": float(a_receber) + float(em_atraso),
                "prazo_medio": float(prazo_medio)
            }
        ]
        
        return dados

@router.post('/bi/recebimentos_dia_mes_atual', tags=["BI"], response_model=RecebimentosDiaMesAtual, status_code=status.HTTP_200_OK)
async def get_recebimentos_dia_mes_atual(
    consulta: FiltrosBI = FiltrosBI(),
    token: str = Depends(oauth2_scheme)
):
    """
    Consulta Grafico dia e mes atual de recebimentos usando POST com schema de entrada.
    Permite consultas mais complexas no futuro.
    """
    # Verifica o token
    payload = decode_access_token(token)
    idempresa = payload.get("empresa")

    if not idempresa:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ID da empresa não encontrado no token")
    
    # Obtém os dados de conexão do Firebird
    conn_data = await get_firebird_connection_data(idempresa)

    # Usa o context manager para gerenciar a conexão automaticamente
    with firebird_connection_manager(conn_data['ipbd'], conn_data['portabd'], conn_data['caminhobd']) as (con, cur):
        query = """
                    SELECT
                        dia,
                        SUM(faturamento),
                        SUM(a_receber)
                    FROM
                        (
                        SELECT
                            dia_recbto AS dia,
                            vlrrecbto AS faturamento,
                            0 AS a_receber,
                            codfilial,
                            codcliente
                        FROM
                            VWFACTRC_BI
                        WHERE
                            ano_recbto = EXTRACT(YEAR FROM CURRENT_TIMESTAMP)
                            AND mes_numero = EXTRACT(MONTH FROM CURRENT_TIMESTAMP)
                    UNION ALL
                        SELECT
                            dia_vencto AS dia,
                            0 AS faturamento,
                            vlrsaldo AS a_receber,
                            codfilial,
                            codcliente
                        FROM
                            VWFACTRC_BI
                        WHERE
                            ano_vencto = EXTRACT(YEAR FROM CURRENT_TIMESTAMP)
                            AND mes_numero_vencto = EXTRACT(MONTH FROM CURRENT_TIMESTAMP)
                            AND condicao_fatura = 'A Receber'
                    ) dados
                    WHERE 1=1
                    GROUP BY
                        dia
                    ORDER BY
                        dia
        """

        params = []

        # Aplicar filtros no WHERE externo (após o UNION) - igual kpi_mes_ano
        filtros_externos = ""
        
        # Normalizar filtros
        codfilial = normalize_filter(consulta.codfilial)
        codcliente = normalize_filter(consulta.codcliente)

        # Aplicar filtros por filial
        if codfilial:
            placeholders_filial = ', '.join(['?'] * len(codfilial))
            filtros_externos += f" AND codfilial IN ({placeholders_filial})"
            params.extend(codfilial)

        # Aplicar filtros por cliente
        if codcliente:
            placeholders_codcliente = ', '.join(['?'] * len(codcliente))
            filtros_externos += f" AND codcliente IN ({placeholders_codcliente})"
            params.extend(codcliente)

        # Inserir filtros no WHERE externo
        query = query.replace(
            "WHERE 1=1",
            f"WHERE 1=1{filtros_externos}"
        )

        cur.execute(query, tuple(params))

        # Dicionário para armazenar os dados organizados por dia
        dados = {}

        for row in cur.fetchall():
            dia = str(int(row[0])) if row[0] is not None else "0"
            faturamento = float(row[1]) if row[1] is not None else 0.0
            a_receber = float(row[2]) if row[2] is not None else 0.0
            
            # Adiciona os dados do dia
            dados[dia] = DadosRecebimentosDiaMesAtual(
                faturamento=faturamento,
                a_receber=a_receber
            )

        if not dados:
            return {}
        
        return dados

@router.post("/bi/a_receber_cliente", tags=["BI"], response_model=AReceberCliente, status_code=status.HTTP_200_OK)
async def get_a_receber_cliente(
    consulta: FiltrosBI = FiltrosBI(),
    token: str = Depends(oauth2_scheme)
):

    """
    Consulta a receber cliente usando POST com schema de entrada.
    Permite consultas mais complexas no futuro.
    """
    # Verifica o token
    payload = decode_access_token(token)
    idempresa = payload.get("empresa")

    if not idempresa:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ID da empresa não encontrado no token")
    
    # Obtém os dados de conexão do Firebird
    conn_data = await get_firebird_connection_data(idempresa)

    # Usa o context manager para gerenciar a conexão automaticamente
    with firebird_connection_manager(conn_data['ipbd'], conn_data['portabd'], conn_data['caminhobd']) as (con, cur):

        # ← AQUI usa os campos do schema
        data_fim = consulta.data_fim or date.today()
        data_inicio = consulta.data_inicio or (data_fim - timedelta(days=30))

        # Construir filtros adicionais para aplicar nas queries
        filtros_adicionais_a_receber = ""
        filtros_adicionais_em_atraso = ""
        params_a_receber = [data_inicio, data_fim]
        params_em_atraso = []
        
        # Normalizar filtros
        codfilial = normalize_filter(consulta.codfilial)
        codcliente = normalize_filter(consulta.codcliente)

        # Aplicar filtros por filial
        if codfilial:
            placeholders_filial = ', '.join(['?'] * len(codfilial))
            filtros_adicionais_a_receber += f" AND codfilial IN ({placeholders_filial})"
            filtros_adicionais_em_atraso += f" AND codfilial IN ({placeholders_filial})"
            params_a_receber.extend(codfilial)
            params_em_atraso.extend(codfilial)

        # Aplicar filtros por cliente
        if codcliente:
            placeholders_cliente = ', '.join(['?'] * len(codcliente))
            filtros_adicionais_a_receber += f" AND codcliente IN ({placeholders_cliente})"
            filtros_adicionais_em_atraso += f" AND codcliente IN ({placeholders_cliente})"
            params_a_receber.extend(codcliente)
            params_em_atraso.extend(codcliente)

        query= f"""
            SELECT
                codcliente,
                cliente,
                SUM(vlrsaldo)
            FROM
                (
                SELECT
                    cliente,
                    vlrsaldo,
                    codfilial,
                    codcliente,
                    datavencto
                FROM
                    VWFACTRC_BI
                WHERE condicao_fatura = 'A Receber' AND datavencto >= ? AND datavencto <= ?{filtros_adicionais_a_receber}
            UNION ALL
                SELECT
                    cliente,
                    vlrsaldo,
                    codfilial,
                    codcliente,
                    datavencto
                FROM
                    VWFACTRC_BI
                WHERE condicao_fatura = 'Em Atraso'{filtros_adicionais_em_atraso}
            ) dados
            GROUP BY
                codcliente,
                cliente
            ORDER BY
                SUM(vlrsaldo) DESC
        """

        params = params_a_receber + params_em_atraso


        cur.execute(query, tuple(params))

                # Dicionário para armazenar os dados organizados por cliente
        dados = {}

        for row in cur.fetchall():
            codcliente = str(row[0]) if row[0] is not None else None
            cliente = str(row[1]) if row[1] is not None else None
            a_receber = float(row[2]) if row[2] is not None else 0.0
            
            # Adiciona os dados do cliente
            dados[codcliente] = DadosAReceberCliente(
                cliente=cliente,
                a_receber=a_receber
            )

        if not dados:
            return {}
        
        return dados

@router.post("/bi/tabela_a_receber", tags=["BI"], response_model=List[TabelaAReceber], status_code=status.HTTP_200_OK)
async def get_tabela_a_receber(
    consulta: FiltrosBI = FiltrosBI(),
    token: str = Depends(oauth2_scheme)
):

    """
    Consulta tabela de a receber usando POST com schema de entrada.
    Permite consultas mais complexas no futuro.
    """
    # Verifica o token
    payload = decode_access_token(token)
    idempresa = payload.get("empresa")

    if not idempresa:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ID da empresa não encontrado no token")
    
    # Obtém os dados de conexão do Firebird
    conn_data = await get_firebird_connection_data(idempresa)

    # Usa o context manager para gerenciar a conexão automaticamente
    with firebird_connection_manager(conn_data['ipbd'], conn_data['portabd'], conn_data['caminhobd']) as (con, cur):

        # ← AQUI usa os campos do schema
        data_fim = consulta.data_fim or date.today()
        data_inicio = consulta.data_inicio or (data_fim - timedelta(days=30))

        query= """
            SELECT
                datavencto,
                cliente,
                cidade,
                coduf,
                produto,
                SUM(vlrsaldo),
                conta
            FROM
                vwfactrc_bi
            WHERE datavencto >= ? AND datavencto <= ?
            GROUP BY 
                datavencto,
                cliente,
                cidade,
                coduf,
                produto,
                conta
            ORDER BY 
                datavencto DESC
        """

        params = []

        # Aplicar filtros no WHERE externo (mesma lógica dos outros endpoints)
        filtros_externos = ""
        
        params.extend([data_inicio, data_fim])
        
        # Normalizar filtros
        codfilial = normalize_filter(consulta.codfilial)
        codcliente = normalize_filter(consulta.codcliente)

        # Aplicar filtros por filial
        if codfilial:
            placeholders_filial = ', '.join(['?'] * len(codfilial))
            filtros_externos += f" AND codfilial IN ({placeholders_filial})"
            params.extend(codfilial)

        # Aplicar filtros por cliente
        if codcliente:
            placeholders_cliente = ', '.join(['?'] * len(codcliente))
            filtros_externos += f" AND codcliente IN ({placeholders_cliente})"
            params.extend(codcliente)



        # Inserir filtros no WHERE externo
        query = query.replace(
            "WHERE datavencto >= ? AND datavencto <= ?",
            f"WHERE datavencto >= ? AND datavencto <= ?{filtros_externos}"
        )

        cur.execute(query, tuple(params))
                     # Combina os resultados
        dados = [
            {
                "datavencto": str(row[0]) if row[0] is not None else None,
                "cliente": str(row[1]) if row[1] is not None else None,
                "cidade": str(row[2]) if row[2] is not None else None,
                "coduf": str(row[3]) if row[3] is not None else None,
                "produto": str(row[4]) if row[4] is not None else None,
                "a_receber": float(row[5]) if row[5] is not None else 0.0,
                "conta": str(row[6]) if row[6] is not None else None
            }
            for row in cur.fetchall()
        ]
              

        if not dados:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Nenhum dado encontrado")
        
        return dados

@router.get("/bi/filtro_fornecedor", tags=["BI"], response_model=List[FiltroFornecedor], status_code=status.HTTP_200_OK)
async def get_filtro_fornecedor(
    token: str = Depends(oauth2_scheme)
):

    """
        Consulta filtro fornecedor usando GET com schema de entrada.
    """

  # Verifica o token
    payload = decode_access_token(token)
    idempresa = payload.get("empresa")

    if not idempresa:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ID da empresa não encontrado no token")
    
    # Obtém os dados de conexão do Firebird
    conn_data = await get_firebird_connection_data(idempresa)

    # Usa o context manager para gerenciar a conexão automaticamente
    with firebird_connection_manager(conn_data['ipbd'], conn_data['portabd'], conn_data['caminhobd']) as (con, cur):

        query= """
            SELECT
                cgccpfforne,
                nomefantasia
            FROM
                tbfor
        """

        cur.execute(query)

        dados = [
            {
                "codfornecedor": str(row[0]) if row[0] is not None else None,
                "fornecedor": str(row[1]) if row[1] is not None else None
            }
            for row in cur.fetchall()
        ]
              

        if not dados:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Nenhum dado encontrado")
        
        return dados

@router.get("/bi/filtro_transacao", tags=["BI"], response_model=List[FiltroTransacao], status_code=status.HTTP_200_OK)
async def get_filtro_transacao(
    token: str = Depends(oauth2_scheme)
):

    """
        Consulta filtro transacao usando GET com schema de entrada.
    """

  # Verifica o token
    payload = decode_access_token(token)
    idempresa = payload.get("empresa")

    if not idempresa:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ID da empresa não encontrado no token")
    
    # Obtém os dados de conexão do Firebird
    conn_data = await get_firebird_connection_data(idempresa)

    # Usa o context manager para gerenciar a conexão automaticamente
    with firebird_connection_manager(conn_data['ipbd'], conn_data['portabd'], conn_data['caminhobd']) as (con, cur):

        query= """
            SELECT
	            codtransacao,
	            descricao
            FROM
                tbhis
        """

        cur.execute(query)

        dados = [
            {
                "codtransacao": str(row[0]) if row[0] is not None else None,
                "transacao": str(row[1]) if row[1] is not None else None
            }
            for row in cur.fetchall()
        ]
              

        if not dados:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Nenhum dado encontrado")
        
        return dados

@router.post("/bi/big_numbers_contas_pagar", tags=["BI"], response_model=List[BigNumbersContasPagar], status_code=status.HTTP_200_OK)
async def get_big_numbers_contas_pagar(
    consulta: FiltrosBI = FiltrosBI(),
    token: str = Depends(oauth2_scheme)
):

    """
    Consulta big numbers contas pagar usando POST com schema de entrada.
    Permite consultas mais complexas no futuro.
    """
    # Verifica o token
    payload = decode_access_token(token)
    idempresa = payload.get("empresa")

    if not idempresa:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ID da empresa não encontrado no token")
    
    # Obtém os dados de conexão do Firebird
    conn_data = await get_firebird_connection_data(idempresa)

    # Usa o context manager para gerenciar a conexão automaticamente
    with firebird_connection_manager(conn_data['ipbd'], conn_data['portabd'], conn_data['caminhobd']) as (con, cur):

        # ← AQUI usa os campos do schema
        data_fim = consulta.data_fim or date.today()
        data_inicio = consulta.data_inicio or (data_fim - timedelta(days=30))

        # Normalizar filtros para aplicar em todas as queries
        codfornecedor = normalize_filter(consulta.codfornecedor)
        codtransacao = normalize_filter(consulta.codtransacao)

        # Construir filtros adicionais para aplicar nas queries
        filtros_adicionais = ""
        params_filtros = []

        if codfornecedor:
            placeholders_fornecedor = ', '.join(['?'] * len(codfornecedor))
            filtros_adicionais += f" AND codfornecedor IN ({placeholders_fornecedor})"
            params_filtros.extend(codfornecedor)

        if codtransacao:
            placeholders_transacao = ', '.join(['?'] * len(codtransacao))
            filtros_adicionais += f" AND codtransacao IN ({placeholders_transacao})"
            params_filtros.extend(codtransacao)

        # 1. FATURAMENTO: Filtrado por período de recebimento
        query_pago = f"""
            SELECT
	            COALESCE(SUM(vlrpago), 0) AS pago
            FROM vwcptit_bi
            WHERE datamovto >= ? AND datamovto <= ?{filtros_adicionais}
        """
        params_pago = [data_inicio, data_fim] + params_filtros

        # 2. A PAGAR: Filtrado por período de vencimento
        query_a_pagar = f"""
            SELECT
	            COALESCE(SUM(vlrsaldo), 0) AS a_pagar
            FROM vwcptit_bi
            WHERE condicao_fatura = 'A Pagar'
              AND datavencto >= ? AND datavencto <= ?{filtros_adicionais}
        """
        params_a_pagar = [data_inicio, data_fim] + params_filtros

        # 3. EM ATRASO: SEM filtro de data (sempre atual)
        query_em_atraso = f"""
            SELECT COALESCE(SUM(vlrsaldo), 0) AS em_atraso
            FROM vwcptit_bi
            WHERE condicao_fatura = 'Em Atraso'{filtros_adicionais}
        """
        params_em_atraso = params_filtros

        # Executar queries separadamente
        cur.execute(query_pago, tuple(params_pago))
        pago = cur.fetchone()[0] or 0.0

        cur.execute(query_a_pagar, tuple(params_a_pagar))
        a_pagar = cur.fetchone()[0] or 0.0

        cur.execute(query_em_atraso, tuple(params_em_atraso))
        em_atraso = cur.fetchone()[0] or 0.0

        # Retornar dados combinados
        dados = [
            {
                "pago": float(pago),
                "a_pagar": float(a_pagar),
                "em_atraso": float(em_atraso),
                "a_pagar_total": float(a_pagar) + float(em_atraso)
            }
        ]
        
        return dados

@router.post('/bi/contas_pagar_dia_mes_atual', tags=["BI"], response_model=ContasPagarDiaMesAtual, status_code=status.HTTP_200_OK)
async def get_contas_pagar_dia_mes_atual(
    consulta: FiltrosBI = FiltrosBI(),
    token: str = Depends(oauth2_scheme)
):
    """
    Consulta Grafico dia e mes atual de contas pagar usando POST com schema de entrada.
    Permite consultas mais complexas no futuro.
    """
    # Verifica o token
    payload = decode_access_token(token)
    idempresa = payload.get("empresa")

    if not idempresa:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ID da empresa não encontrado no token")
    
    # Obtém os dados de conexão do Firebird
    conn_data = await get_firebird_connection_data(idempresa)

    # Usa o context manager para gerenciar a conexão automaticamente
    with firebird_connection_manager(conn_data['ipbd'], conn_data['portabd'], conn_data['caminhobd']) as (con, cur):
        query = """
                    SELECT
                        dia,
                        SUM(vlrpago),
                        SUM(a_pagar)
                    FROM
                        (
                        SELECT
                            dia_movto AS dia,
                            vlrpago,
                            0 AS a_pagar,
                            codfornecedor,
                            codtransacao
                        FROM
                            VWCPTIT_BI
                        WHERE
                            ano_movto = EXTRACT(YEAR FROM CURRENT_TIMESTAMP)
                            AND mes_numero_movto = EXTRACT(MONTH FROM CURRENT_TIMESTAMP)
                    UNION ALL
                        SELECT
                            dia_vencto AS dia,
                            0 AS vlrpago,
                            vlrsaldo AS a_pagar,
                            codfornecedor,
                            codtransacao
                        FROM
                            VWCPTIT_BI
                        WHERE
                            ano_vencto = EXTRACT(YEAR FROM CURRENT_TIMESTAMP)
                            AND mes_numero_vencto = EXTRACT(MONTH FROM CURRENT_TIMESTAMP)
                            AND condicao_fatura = 'A Pagar'
                    ) dados
                    WHERE 1=1
                    GROUP BY
                        dia
                    ORDER BY
                        dia
        """

        params = []

        # Aplicar filtros no WHERE externo (após o UNION) - igual kpi_mes_ano
        filtros_externos = ""
        
        # Normalizar filtros
        codfornecedor = normalize_filter(consulta.codfornecedor)
        codtransacao = normalize_filter(consulta.codtransacao)

        # Aplicar filtros por filial
        if codfornecedor:
            placeholders_fornecedor = ', '.join(['?'] * len(codfornecedor))
            filtros_externos += f" AND codfornecedor IN ({placeholders_fornecedor})"
            params.extend(codfornecedor)

        # Aplicar filtros por cliente
        if codtransacao:
            placeholders_codtransacao = ', '.join(['?'] * len(codtransacao))
            filtros_externos += f" AND codtransacao IN ({placeholders_codtransacao})"
            params.extend(codtransacao)

        # Inserir filtros no WHERE externo
        query = query.replace(
            "WHERE 1=1",
            f"WHERE 1=1{filtros_externos}"
        )

        cur.execute(query, tuple(params))

        # Dicionário para armazenar os dados organizados por dia
        dados = {}

        for row in cur.fetchall():
            dia = str(int(row[0])) if row[0] is not None else "0"
            pago = float(row[1]) if row[1] is not None else 0.0
            a_pagar = float(row[2]) if row[2] is not None else 0.0
            
            # Adiciona os dados do dia
            dados[dia] = DadosContasPagarDiaMesAtual(
                pago=pago,
                a_pagar=a_pagar
            )

        if not dados:
            return {}
        
        return dados

@router.post("/bi/a_pagar_fornecedor", tags=["BI"], response_model=APagarFornecedor, status_code=status.HTTP_200_OK)
async def get_a_pagar_fornecedor(
    consulta: FiltrosBI = FiltrosBI(),
    token: str = Depends(oauth2_scheme)
):

    """
    Consulta a pagar fornecedor usando POST com schema de entrada.
    Permite consultas mais complexas no futuro.
    """
    # Verifica o token
    payload = decode_access_token(token)
    idempresa = payload.get("empresa")

    if not idempresa:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ID da empresa não encontrado no token")
    
    # Obtém os dados de conexão do Firebird
    conn_data = await get_firebird_connection_data(idempresa)

    # Usa o context manager para gerenciar a conexão automaticamente
    with firebird_connection_manager(conn_data['ipbd'], conn_data['portabd'], conn_data['caminhobd']) as (con, cur):

        # ← AQUI usa os campos do schema
        data_fim = consulta.data_fim or date.today()
        data_inicio = consulta.data_inicio or (data_fim - timedelta(days=30))

        # Construir filtros adicionais para aplicar nas queries
        filtros_adicionais_a_pagar = ""
        filtros_adicionais_em_atraso = ""
        params_a_pagar = [data_inicio, data_fim]
        params_em_atraso = []
        
        # Normalizar filtros
        codfornecedor = normalize_filter(consulta.codfornecedor)
        codtransacao = normalize_filter(consulta.codtransacao)

        # Aplicar filtros por fornecedor
        if codfornecedor:
            placeholders_fornecedor = ', '.join(['?'] * len(codfornecedor))
            filtros_adicionais_a_pagar += f" AND codfornecedor IN ({placeholders_fornecedor})"
            filtros_adicionais_em_atraso += f" AND codfornecedor IN ({placeholders_fornecedor})"
            params_a_pagar.extend(codfornecedor)
            params_em_atraso.extend(codfornecedor)

        # Aplicar filtros por transacao
        if codtransacao:
            placeholders_transacao = ', '.join(['?'] * len(codtransacao))
            filtros_adicionais_a_pagar += f" AND codtransacao IN ({placeholders_transacao})"
            filtros_adicionais_em_atraso += f" AND codtransacao IN ({placeholders_transacao})"
            params_a_pagar.extend(codtransacao)
            params_em_atraso.extend(codtransacao)

        query= f"""
               SELECT
                    codfornecedor,
                    fornecedor,
                    SUM(vlrsaldo)
                FROM
                    (
                    SELECT
                        fornecedor,
                        vlrsaldo,
                        codfornecedor,
                        codtransacao,
                        datavencto
                    FROM
                        VWCPTIT_BI
                    WHERE condicao_fatura = 'A Pagar' AND datavencto >= ? AND datavencto <= ?{filtros_adicionais_a_pagar}
                UNION ALL
                    SELECT
                        fornecedor,
                        vlrsaldo,
                        codfornecedor,
                        codtransacao,
                        datavencto
                    FROM
                        VWCPTIT_BI
                    WHERE condicao_fatura = 'Em Atraso'{filtros_adicionais_em_atraso}
                            ) dados
                GROUP BY
                    codfornecedor,
                    fornecedor
                ORDER BY
                    SUM(vlrsaldo) DESC
        """

        params = params_a_pagar + params_em_atraso


        cur.execute(query, tuple(params))

                # Dicionário para armazenar os dados organizados por cliente
        dados = {}

        for row in cur.fetchall():
            codfornecedor = str(row[0]) if row[0] is not None else None
            fornecedor = str(row[1]) if row[1] is not None else None
            a_pagar = float(row[2]) if row[2] is not None else 0.0
            
            # Adiciona os dados do cliente
            dados[codfornecedor] = DadosAPagarFornecedor(
                fornecedor=fornecedor,
                a_pagar=a_pagar
            )

        if not dados:
            return {}
        
        return dados

@router.post("/bi/tabela_a_pagar", tags=["BI"], response_model=List[TabelaAPagar], status_code=status.HTTP_200_OK)
async def get_tabela_a_pagar(
    consulta: FiltrosBI = FiltrosBI(),
    token: str = Depends(oauth2_scheme)
):

    """
    Consulta tabela de a pagar usando POST com schema de entrada.
    Permite consultas mais complexas no futuro.
    """
    # Verifica o token
    payload = decode_access_token(token)
    idempresa = payload.get("empresa")

    if not idempresa:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ID da empresa não encontrado no token")
    
    # Obtém os dados de conexão do Firebird
    conn_data = await get_firebird_connection_data(idempresa)

    # Usa o context manager para gerenciar a conexão automaticamente
    with firebird_connection_manager(conn_data['ipbd'], conn_data['portabd'], conn_data['caminhobd']) as (con, cur):

        # ← AQUI usa os campos do schema
        data_fim = consulta.data_fim or date.today()
        data_inicio = consulta.data_inicio or (data_fim - timedelta(days=30))

        query= """
            SELECT
                datavencto,
                fornecedor,
                transacao,
                SUM(vlrsaldo),
                conta
            FROM
                vwcptit_bi
                WHERE datavencto >= ? AND datavencto <= ?
            GROUP BY
                datavencto,
                fornecedor,
                transacao,
                conta
            ORDER BY
                datavencto DESC
        """

        params = []

        # Aplicar filtros no WHERE externo (mesma lógica dos outros endpoints)
        filtros_externos = ""
        
        params.extend([data_inicio, data_fim])
        
        # Normalizar filtros
        codfornecedor = normalize_filter(consulta.codfornecedor)
        codtransacao = normalize_filter(consulta.codtransacao)

        # Aplicar filtros por filial
        if codfornecedor:
            placeholders_fornecedor = ', '.join(['?'] * len(codfornecedor))
            filtros_externos += f" AND codfornecedor IN ({placeholders_fornecedor})"
            params.extend(codfornecedor)

        # Aplicar filtros por cliente
        if codtransacao:
            placeholders_transacao = ', '.join(['?'] * len(codtransacao))
            filtros_externos += f" AND codtransacao IN ({placeholders_transacao})"
            params.extend(codtransacao)



        # Inserir filtros no WHERE externo
        query = query.replace(
            "WHERE datavencto >= ? AND datavencto <= ?",
            f"WHERE datavencto >= ? AND datavencto <= ?{filtros_externos}"
        )

        cur.execute(query, tuple(params))
                     # Combina os resultados
        dados = [
            {
                "datavencto": str(row[0]) if row[0] is not None else None,
                "fornecedor": str(row[1]) if row[1] is not None else None,
                "transacao": str(row[2]) if row[2] is not None else None,
                "a_pagar": float(row[3]) if row[3] is not None else 0.0,
                "conta": str(row[4]) if row[4] is not None else None
            }
            for row in cur.fetchall()
        ]
              

        if not dados:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Nenhum dado encontrado")
        
        return dados