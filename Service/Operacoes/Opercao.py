import pandas as pd
import ConexaoPostgreMPL


def BuscarOperacoes():
    conn = ConexaoPostgreMPL.conexaoJohn()

    consulta = """
    select  c.*, f."nomeFase" from "Easy"."Operacao" c
    inner join "Easy"."Fase" f on f."codFase" = c."codFase"
    """

    consulta = pd.read_sql(consulta,conn)
    conn.close()

    return consulta

def BuscarOperacaoEspecifica(codOperacao):
    conn = ConexaoPostgreMPL.conexaoJohn()

    consulta = """
    select c."codOperacao"  from "Easy"."Operacao" c  
    where c."codOperacao" = %s
    """

    consulta = pd.read_sql(consulta,conn,params=(codOperacao,))
    conn.close()

    return consulta

def InserirOperacao(codOperacao, nomeOperacao, nomeFase, Maq_Equipamento):
    consulta = BuscarOperacaoEspecifica(codOperacao)

    if consulta.empty:
        conn = ConexaoPostgreMPL.conexaoJohn()
        inserir = """
        insert into "Easy"."Operacao" ("codOperacao" , "codFase", "Maq/Equipamento","nomeOperacao") values ( %s, %s,  %s, %s )
        """
        cursor = conn.cursor()
        cursor.execute(inserir,(codcategoria, nomeCategoria,))
        conn.commit()
        cursor.close()

        conn.close()

        return pd.DataFrame([{'Mensagem': "Operacão cadastrada com Sucesso!", "status": True}])

    else:
        return pd.DataFrame([{'Mensagem': "Operacão já´existe!", "status": False}])

def UpdateCategoria(codcategoria, nomeCategoria):

    consulta = BuscarCategoriaEspecifica(codcategoria)

    if consulta.empty:
        return pd.DataFrame([{'Mensagem':"Categoria Nao encontrado!","status":False}])
    else:
        CategoriaAtual = consulta['nomeCategoria'][0]
        if CategoriaAtual == nomeCategoria :
            nomeCategoria = CategoriaAtual


        conn = ConexaoPostgreMPL.conexaoJohn()
        update = """
        update "Easy"."Categoria"
        set  "nomeCategoria" = %s 
        where "codcategoria" = %s 
        """

        cursor = conn.cursor()
        cursor.execute(update,(nomeCategoria,codcategoria,))
        conn.commit()
        cursor.close()

        conn.close()
        return pd.DataFrame([{'Mensagem': "Categoria Alterado com Sucesso!", "status": True}])