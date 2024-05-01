import pandas as pd
import ConexaoPostgreMPL


def BuscarFases():
    conn = ConexaoPostgreMPL.conexaoJohn()

    consulta = """
    select f."codFase" , f."nomeFase"  from "Easy"."Fase" f
    """

    consulta = pd.read_sql(consulta,conn)
    conn.close()

    return consulta

def BuscarFaseEspecifica(codFase):
    conn = ConexaoPostgreMPL.conexaoJohn()

    consulta = """
    select f."codFase" , f."nomeFase"  from "Easy"."Fase" f
    where f.codFase = %s
    """

    consulta = pd.read_sql(consulta,conn,params=(codFase,))
    conn.close()

    return consulta

def InserirFase(codFase, nomeFase):
    consulta = BuscarFaseEspecifica(codFase)

    if consulta.empty:
        conn = ConexaoPostgreMPL.conexaoJohn()
        inserir = """
        insert into "Easy"."Fase" ("codFase" , "nomeFase") values ( %s, %s )
        """
        cursor = conn.cursor()
        cursor.execute(inserir,(codFase, nomeFase,))
        conn.commit()
        cursor.close()

        conn.close()

        return pd.DataFrame([{'Mensagem': "Fase cadastrada com Sucesso!", "status": True}])

    else:
        return pd.DataFrame([{'Mensagem': "Fase já´existe!", "status": False}])

def UpdateFase(codFase, nomeFase):

    consulta = BuscarCategoriaEspecifica(codcategoria)

    if consulta.empty:
        return pd.DataFrame([{'Mensagem':"FASE Nao encontrado!","status":False}])
    else:
        FaseAtual = consulta['nomeFase'][0]
        if FaseAtual == nomeFase :
            nomeFase = FaseAtual


        conn = ConexaoPostgreMPL.conexaoJohn()
        update = """
        update "Easy"."Fase"
        set  "nomeFase" = %s 
        where "codFase" = %s 
        """

        cursor = conn.cursor()
        cursor.execute(update,(nomeFase,codFase,))
        conn.commit()
        cursor.close()

        conn.close()
        return pd.DataFrame([{'Mensagem': "FASE Alterada com Sucesso!", "status": True}])