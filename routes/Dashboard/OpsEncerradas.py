'''
Relatorio criado para obter as ops baixadas no periodo
'''
import pandas as pd
import ConexaoPostgreMPL

def RelatorioEncerramento(dataInicio, dataFinal):
    consulta  ="""
    select * from railway."Easy"."OpsEncerradas" oe
    where dataencerramento >= %s and dataencerramento <=%s
    """

    consulta2 = """
    select * from railway."Easy"."DetalhaOP" do2 
    """

    with ConexaoPostgreMPL as conn:
        consulta = pd.read_sql(consulta,conn,params=(dataInicio,dataFinal,))
        consulta2 = pd.read_sql(consulta2,conn)

        consulta = pd.merge(consulta,consulta2,on='idOP',how='left')
    return consulta