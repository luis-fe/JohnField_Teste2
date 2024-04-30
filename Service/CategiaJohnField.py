import pandas as pd
import ConexaoPostgreMPL


def BuscarCategorias():
    conn = ConexaoPostgreMPL.conexao()

    consulta = """
    select c.codcategoria , c."nomeCategoria"  from "Easy"."Categoria" c  
    """

    consulta = pd.read_sql(consulta,conn)
    conn.close()

    return consulta

def BuscarCategoriaEspecifica(codcategoria):
    conn = ConexaoPostgreMPL.conexao()

    consulta = """
    select c.codcategoria , c."nomeCategoria"  from "Easy"."Categoria" c  
    where c.codcategoria = %s
    """

    consulta = pd.read_sql(consulta,conn,params=(codcategoria,))
    conn.close()

    return consulta

def InserirCategoria(codcategoria, nomeCategoria):
    consulta = BuscarCategoriaEspecifica(codcategoria)

    if consulta.empety:
        conn = ConexaoPostgreMPL.conexao()
        inserir = """
        insert into "Easy"."Categoria" (codcategoria , "nomeCategoria") values ( %s, %s )
        """
        cursor = conn.cursor()
        cursor.execute(insert,(codcategoria, nomeCategoria,))
        conn.commit()
        cursor.close()

        conn.close()

        return pd.DataFrame([{'Mensagem': "Categoria cadastrado com Sucesso!", "status": True}])

    else:
        return pd.DataFrame([{'Mensagem': "Categoria já´existe!", "status": False}])

def UpdateCategoria(codcategoria, nomeCategoria):

    consulta = BuscarCategoriaEspecifica(codcategoria)

    if consulta.empty:
        return pd.DataFrame([{'Mensagem':"Categoria Nao encontrado!","status":False}])
    else:
        CategoriaAtual = consulta['codcategoria'][0]
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