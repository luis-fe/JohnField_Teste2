import pandas as pd
import ConexaoPostgreMPL


def BuscarGrade():
    consulta = """
    select DISTINCT "codGrade", "nomeGrade" from "Easy"."Grade"
    """
    conn = ConexaoPostgreMPL.conexaoJohn()
    consulta = pd.read_sql(consulta, conn)
    conn.close()

    return consulta