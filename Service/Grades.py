import pandas as pd
import ConexaoPostgreMPL


def BuscarGrade():
    consulta = """
    select  "codGrade", "nomeGrade", "Tamanhos" from "Easy"."Grade"
    """
    conn = ConexaoPostgreMPL.conexaoJohn()
    consulta = pd.read_sql(consulta, conn)
    conn.close()

    # Distrinchar tamanhos
    df_summary = consulta.groupby(['codGrade', 'nomeGrade']).apply(
        lambda x: ';'.join(f"{rest}({nec})" for rest, nec in zip(x['Tamanhos']))).reset_index()
    return df_summary

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

def UpdateGrade(codGrade, nomeGrade, arrayTamanhos):
    VerificarGrade = BuscarGradeEspecifica(codGrade)
    if VerificarGrade.empty:
        return pd.DataFrame([{'Mensagem': f'Grade {codGrade} NAO já existe!', 'status': False}])
    else:
        conn = ConexaoPostgreMPL.conexaoJohn()

        for tamanho in arrayTamanhos:  # Correção do loop

            delete = """
            delete from "Easy"."Grade"
            where "codGrade" = %s
            
            """

            inserir = """
            INSERT INTO "Easy"."Grade" ("codGrade", "nomeGrade", "Tamanhos")
            VALUES (%s, %s, %s)  -- Correção na sintaxe do SQL
            """
            cursor = conn.cursor()

            cursor.execute(delete,(codGrade,))
            conn.commit()

            cursor.execute(inserir, (codGrade, nomeGrade, tamanho,))
            conn.commit()

            cursor.close()

        conn.close()
        return pd.DataFrame([{'Mensagem': f'Grade {codGrade} Atualizada com sucesso!', 'status': True}])

