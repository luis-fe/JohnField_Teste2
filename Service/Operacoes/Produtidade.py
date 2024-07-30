import pandas as pd
import ConexaoPostgreMPL
from datetime import datetime

def CalcularTempo(dataInicio, dataFim, tempoInicio, tempoFim):
    # Converte as horas de início e fim em objetos datetime
    tempoInicio = datetime.strptime(tempoInicio, "%H:%M:%S")
    tempoFim = datetime.strptime(tempoFim, "%H:%M:%S")

    if dataInicio == dataFim:
        # Calcular a diferença entre os horários
        delta = tempoFim - tempoInicio
        return delta.total_seconds() / 60
    else:
        # Se as datas forem diferentes, considera-se uma diferença de 24h para simplificar
        tempoInicio = datetime.combine(datetime.strptime(dataInicio, "%Y-%m-%d"), tempoInicio)
        tempoFim = datetime.combine(datetime.strptime(dataFim, "%Y-%m-%d"), tempoFim)
        delta = tempoFim - tempoInicio
        return delta.total_seconds() / 60

def RankingOperacoesEfic(dataInicio, dataFinal):
    sql = """
    SELECT
        "Codigo Registro",
        "Data",
        "Hr Inicio",
        "Hr Final",
        "codOperador",
        "nomeOperador",
        "nomeOperacao",
        "paradas min",
        "tempoTotal(min)",
        "qtdPcs",
        "Meta(pcs/hr)"
    FROM
        "Easy"."ColetasProducao" cp
    WHERE
        "Data" >= %s AND "Data" <= %s
    ORDER BY
        "Data", "codOperador", "Codigo Registro"
    """

    conn = ConexaoPostgreMPL.conexaoJohn()
    produtividade = pd.read_sql(sql, conn, params=(dataInicio, dataFinal))

    if produtividade.empty:
        return pd.DataFrame([])

    # Adicionar uma coluna calculada usando a função CalcularTempo
    produtividade['TempoRealizado(Min)'] = produtividade.apply(
        lambda row: CalcularTempo(
            row['Data'], row['Data'], row['Hr Inicio'], row['Hr Final']
        ), axis=1
    )

    return produtividade
