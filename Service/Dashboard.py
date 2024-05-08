import pandas as pd
import ConexaoPostgreMPL


def OPsAbertoPorCliente():

    consulta = """
    select * from "Easy"."DetalhaOP_Abertas" doa 
    """

    conn = ConexaoPostgreMPL.conexaoJohn()
    consulta = pd.read_sql(consulta,conn)
    conn.close()

    dados = {
        '0-Total DE pçs': f'{1} Pçs',
        '3 -Detalhamento': consulta.to_dict(orient='records')}

    return pd.DataFrame([dados])
