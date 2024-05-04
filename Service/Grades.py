import pandas as pd
import ConexaoPostgreMPL


def BuscarGrade():
    consulta = """
    SELECT "codGrade", "nomeGrade", "Tamanhos" FROM "Easy"."Grade"
    """
    conn = ConexaoPostgreMPL.conexaoJohn()
    consulta = pd.read_sql(consulta, conn)
    conn.close()

    # Convertendo a coluna 'Tamanhos' para lista de strings
    consulta['Tamanhos'] = consulta['Tamanhos'].apply(lambda x: [x])

    # Agrupar tamanhos em uma lista
    df_summary = consulta.groupby(['codGrade', 'nomeGrade'])['Tamanhos'].sum().reset_index()

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

def ObterTamanhos():
    conn = ConexaoPostgreMPL.conexaoJohn()

    consulta = """
    select codsequencia , "DescricaoTamanho"  from "Easy"."Tamanhos" t 
order by t.codsequencia asc
    """
    consulta = pd.DataFrame(consulta,conn)
    conn.close()

    return consulta