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
        return pd.DataFrame([{'Mensagem': f'Grade {codGrade} já existe!', 'status': False}])
    else:
        conn = ConexaoPostgreMPL.conexaoJohn()

        for tamanho in arrayTamanhos:  # Correção do loop

            inserir = """
            INSERT INTO "Easy"."Grade" ("codGrade", "nomeGrade", "Tamanhos")
            VALUES (%s, %s, %s)  -- Correção na sintaxe do SQL
            """
            cursor = conn.cursor()
            cursor.execute(inserir, (codGrade, nomeGrade, tamanho,))
            conn.commit()

            cursor.close()

        conn.close()
        return pd.DataFrame([{'Mensagem': f'Grade {codGrade} cadastrada com sucesso!', 'status': True}])

