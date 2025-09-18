from fastapi import APIRouter, Depends, HTTPException, status
from datetime import date, timedelta
from typing import List
from fastapi.security import OAuth2PasswordBearer 
from app.db.conexaopg import pg_connection_manager
from app.db.conexaofb import firebird_connection_manager
from app.auth.auth import decode_access_token
from app.schemas.BIschemas import FiltrosBI, BigNumbers, KPIMesAno, KPIDiaMesAtual, DadosMesAno, DadosDiaMesAtual, KPIFilial, DadosFilial, KPIRegiao, DadosRegiao, KPICidade, DadosCidade, KPICliente, DadosCliente, KPIProduto, DadosProduto, TabelaFaturamento

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
    consulta: FiltrosBI = FiltrosBI(),
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
	            factrc_bi
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
                frctrc_bi
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
        
        
        # Obtém os dados da consulta do ano anterior
       
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
    consulta: FiltrosBI = FiltrosBI(),
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
                    mes,
                    mes_numero,
                    SUM(volume),
                    SUM(embarque),
                    SUM(faturamento)
                FROM
                    (
                    SELECT
                        ano_emissao AS ano,
                        mes_emissao AS mes,
                        mes_numero,
                        pesofrete_ton AS volume,
                        embarque AS embarque,
                        0 AS faturamento,
                        codfilial,
                        codcid,
                        regiao
                    FROM
                        FRCTRC_BI
                    WHERE
                        ano_emissao >= EXTRACT(YEAR FROM CURRENT_TIMESTAMP) - 2
                UNION ALL
                    SELECT
                        ano_recbto AS ano,
                        mes_recbto AS mes,
                        mes_numero,
                        0 AS volume,
                        0 AS embarque,
                        vlrrecbto AS faturamento,
                        codfilial,
                        codcid,
                        regiao
                    FROM
                        FACTRC_BI
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
            mes = str(row[1]) if row[1] is not None else "Indefinido"
            volume = float(row[3]) if row[3] is not None else 0.0
            embarques = int(row[4]) if row[4] is not None else 0
            faturamento = float(row[5]) if row[5] is not None else 0.0
            
            # Inicializa o ano se não existir
            if ano not in dados:
                dados[ano] = {}
            
            # Adiciona os dados do mês
            dados[ano][mes] = DadosMesAno(
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
                        regiao
                FROM
                    FRCTRC_BI
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
                        regiao
                    FROM
                        FACTRC_BI
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
                datarecbto AS data_operacao
            FROM
                FACTRC_BI
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
                dataemissao AS data_operacao
            FROM
                FRCTRC_BI
        ) dados
        WHERE data_operacao >= ? AND data_operacao <= ?
        GROUP BY
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

        # Inserir filtros no WHERE externo
        query = query.replace(
            "WHERE data_operacao >= ? AND data_operacao <= ?",
            f"WHERE data_operacao >= ? AND data_operacao <= ?{filtros_externos}"
        )

        cur.execute(query, params)

                # Dicionário para armazenar os dados organizados por filial
        dados = {}

        for row in cur.fetchall():
            filial = str(row[0]) if row[0] is not None else None
            volume = float(row[1]) if row[1] is not None else 0.0
            embarques = int(row[2]) if row[2] is not None else 0
            faturamento = float(row[3]) if row[3] is not None else 0.0
            
            # Adiciona os dados do filial
            dados[filial] = DadosFilial(
                volume=volume,
                embarques=embarques,
                faturamento=faturamento
            )

        if not dados:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Nenhum dado encontrado")
        
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
                datarecbto AS data_operacao
        FROM
            FACTRC_BI
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
                dataemissao AS data_operacao
            FROM
                FRCTRC_BI
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

        # Inserir filtros no WHERE externo
        query = query.replace(
            "WHERE data_operacao >= ? AND data_operacao <= ?",
            f"WHERE data_operacao >= ? AND data_operacao <= ?{filtros_externos}"
        )

        cur.execute(query, params)

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
                    datarecbto AS data_operacao
                FROM
                    FACTRC_BI
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
                    dataemissao AS data_operacao
                FROM
                    FRCTRC_BI
            ) dados
            WHERE data_operacao >= ? AND data_operacao <= ?
            GROUP BY
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

        # Inserir filtros no WHERE externo
        query = query.replace(
            "WHERE data_operacao >= ? AND data_operacao <= ?",
            f"WHERE data_operacao >= ? AND data_operacao <= ?{filtros_externos}"
        )

        cur.execute(query, params)

                # Dicionário para armazenar os dados organizados por cidade
        dados = {}

        for row in cur.fetchall():
            cidade = str(row[0]) if row[0] is not None else None
            volume = float(row[1]) if row[1] is not None else 0.0
            embarques = int(row[2]) if row[2] is not None else 0
            faturamento = float(row[3]) if row[3] is not None else 0.0
            
            # Adiciona os dados do cidade
            dados[cidade] = DadosCidade(
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
                    FACTRC_BI
            )
            WHERE
                datarecbto >= ? AND datarecbto <= ?
            GROUP BY
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

        cur.execute(query, params)

                # Dicionário para armazenar os dados organizados por cliente
        dados = {}

        for row in cur.fetchall():
            cliente = str(row[0]) if row[0] is not None else None
            faturamento = float(row[1]) if row[1] is not None else 0.0
            
            # Adiciona os dados do cliente
            dados[cliente] = DadosCliente(
                faturamento=faturamento
            )

        if not dados:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Nenhum dado encontrado")
        
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
                    FACTRC_BI
            )
            WHERE
                datarecbto >= ? AND datarecbto <= ?
            GROUP BY
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

        cur.execute(query, params)

                # Dicionário para armazenar os dados organizados por produto
        dados = {}

        for row in cur.fetchall():
            produto = str(row[0]) if row[0] is not None else None
            faturamento = float(row[1]) if row[1] is not None else 0.0
            
            # Adiciona os dados do produto
            dados[produto] = DadosProduto(
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
                factrc_bi
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

        cur.execute(query, params)
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