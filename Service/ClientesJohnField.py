import pandas as pd
import ConexaoPostgreMPL

def ConsultaClientes():
    conn = ConexaoPostgreMPL.conexaoJohn()
    consulta = pd.read_sql("""
    select "codcliente" ,"nomeCliente" from "Easy"."Cliente" c  
    """,conn)
    conn.close()

    return consulta
