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

def BuscarGradeEspecifica(codGrade):
    buscar = BuscarGrade()
    buscar = buscar[buscar['codGrade'] == codGrade]

    return buscar

def NovaGrade(codGrade, nomeGrade, arrayTamanhos):
    VerificarGrade = BuscarGradeEspecifica(codGrade)
    if not VerificarGrade.empty:
        return pd.DataFrame([{'Mensagem':f'Grade {codGrade} ja existe !', 'status':False}])
    else:
        conn = ConexaoPostgreMPL.conexaoJohn()
        conn.close()
