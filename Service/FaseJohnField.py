import pandas as pd
import ConexaoPostgreMPL


def BuscarFases():
    conn = ConexaoPostgreMPL.conexaoJohn()

    consulta = """
select f."codFase" , 
f."nomeFase", 
f."FaseInicial" as "FaseInical?",
"FaseFinal" as "FaseFinal?" , "ObrigaInformaTamCor" as "ObrigaInformaTamCor?"
from "Easy"."Fase" f
    """

    consulta = pd.read_sql(consulta,conn)
    conn.close()

    return consulta

def BuscarFasesTipoInicial():
    conn = ConexaoPostgreMPL.conexaoJohn()

    consulta = """
select f."codFase" , 
f."nomeFase" 
from "Easy"."Fase" f
where "FaseInicial" = 'SIM'
    """

    consulta = pd.read_sql(consulta,conn)
    conn.close()

    return consulta

def BuscarFaseEspecifica(codFase):
    conn = ConexaoPostgreMPL.conexaoJohn()

    consulta = """
    select f."codFase" , f."nomeFase", f."FaseInicial" , f."FaseFinal"  from "Easy"."Fase" f
    where f."codFase" = %s
    """

    consulta = pd.read_sql(consulta,conn,params=(codFase,))
    conn.close()

    return consulta

def InserirFase(codFase, nomeFase, FaseInicial, FaseFinal, ObrigaInformaTamCor):
    consulta = BuscarFaseEspecifica(codFase)

    if consulta.empty:
        conn = ConexaoPostgreMPL.conexaoJohn()
        inserir = """
        insert into "Easy"."Fase" ("codFase" , "nomeFase", "FaseInicial","FaseFinal", "ObrigaInformaTamCor") values ( %s, %s, %s, %s , %s )
        """
        cursor = conn.cursor()
        cursor.execute(inserir,(codFase, nomeFase,FaseInicial, FaseFinal,ObrigaInformaTamCor))
        conn.commit()
        cursor.close()

        conn.close()

        return pd.DataFrame([{'Mensagem': "Fase cadastrada com Sucesso!", "status": True}])

    else:
        return pd.DataFrame([{'Mensagem': "Fase já´existe!", "status": False}])

def UpdateFase(codFase, nomeFase, FaseInicial, FaseFinal,ObrigaInformaTamCor):

    consulta = BuscarFaseEspecifica(codFase)

    if consulta.empty:
        return pd.DataFrame([{'Mensagem':"FASE Nao encontrado!","status":False}])
    else:
        FaseAtual = consulta['nomeFase'][0]
        if FaseAtual == nomeFase :
            nomeFase = FaseAtual

        FaseInicialAtual = consulta['FaseInicial'][0]
        if FaseInicialAtual == FaseInicial :
            FaseInicial = FaseInicialAtual

        FaseFinallAtual = consulta['FaseInicial'][0]
        if FaseFinallAtual == FaseFinal :
            FaseFinal = FaseFinallAtual

        ObrigaInformaTamCorAtual = consulta['ObrigaInformaTamCor'][0]
        if ObrigaInformaTamCorAtual == ObrigaInformaTamCor :
            ObrigaInformaTamCor = ObrigaInformaTamCorAtual


        conn = ConexaoPostgreMPL.conexaoJohn()
        update = """
        update "Easy"."Fase"
        set  "nomeFase" = %s , "FaseInicial" = %s , "FaseFinal" = %s, "ObrigaInformaTamCor" = %s
        where "codFase" = %s 
        """

        cursor = conn.cursor()
        cursor.execute(update,(nomeFase,FaseInicial, FaseFinal, ObrigaInformaTamCor, codFase,))
        conn.commit()
        cursor.close()

        conn.close()
        return pd.DataFrame([{'Mensagem': "FASE Alterada com Sucesso!", "status": True}])