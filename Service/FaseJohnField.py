import pandas as pd
import ConexaoPostgreMPL


def BuscarFases():
    conn = ConexaoPostgreMPL.conexaoJohn()

    consulta = """
select f."codFase" , 
f."nomeFase", 
f."FaseInicial" as "FaseInical?",
"FaseFinal" as "FaseFinal?" , "ObrigaInformaTamCor" as "ObrigaInformaTamCor?", "LeadTime"
from "Easy"."Fase" f
    """

    consulta = pd.read_sql(consulta,conn)
    conn.close()

    return consulta

def BuscarFasesTipoInicial():
    conn = ConexaoPostgreMPL.conexaoJohn()

    consulta = """
select f."codFase" , 
f."nomeFase" ,"ObrigaInformaTamCor" as "ObrigaInformaTamCor?"
from "Easy"."Fase" f
where "FaseInicial" = 'SIM'
    """

    consulta = pd.read_sql(consulta,conn)
    conn.close()

    return consulta

def BuscarFaseEspecifica(codFase):
    conn = ConexaoPostgreMPL.conexaoJohn()

    consulta = """
    select f."codFase" , f."nomeFase", f."FaseInicial" , f."FaseFinal", "ObrigaInformaTamCor", "LeadTime"  from "Easy"."Fase" f
    where f."codFase" = %s
    """

    consulta = pd.read_sql(consulta,conn,params=(codFase,))
    conn.close()

    return consulta

def BuscarFaseEspecificaPeloNome(nome):
    conn = ConexaoPostgreMPL.conexaoJohn()

    consulta = """
    select f."codFase" , f."nomeFase", f."FaseInicial" , f."FaseFinal"  from "Easy"."Fase" f
    where f."nomeFase" = %s
    """

    consulta = pd.read_sql(consulta,conn,params=(nome,))
    conn.close()

    return consulta

def InserirFase(codFase, nomeFase, FaseInicial, FaseFinal, ObrigaInformaTamCor, LeadTime):
    consulta = BuscarFaseEspecifica(codFase)
    consultaNome = BuscarFaseEspecificaPeloNome(nomeFase)

    if consulta.empty and consultaNome.empty:
        conn = ConexaoPostgreMPL.conexaoJohn()
        inserir = """
        insert into "Easy"."Fase" ("codFase" , "nomeFase", "FaseInicial","FaseFinal", "ObrigaInformaTamCor", "LeadTime") values ( %s, %s, %s, %s , %s,  %s )
        """
        cursor = conn.cursor()
        cursor.execute(inserir,(codFase, nomeFase,FaseInicial, FaseFinal,ObrigaInformaTamCor, LeadTime))
        conn.commit()
        cursor.close()

        conn.close()

        return pd.DataFrame([{'Mensagem': "Fase cadastrada com Sucesso!", "status": True}])

    elif not consulta.empty:
        return pd.DataFrame([{'Mensagem': f"O codigo Fase {codFase} já´existe!", "status": False}])
    else:
        return pd.DataFrame([{'Mensagem': f"A Fase {nomeFase} já´existe!", "status": False}])


def UpdateFase(codFase, nomeFase, FaseInicial, FaseFinal,ObrigaInformaTamCor,LeadTime):

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

        LeadTimeAtual = consulta['LeadTime'][0]
        if LeadTimeAtual == LeadTime :
            LeadTime = LeadTimeAtual


        conn = ConexaoPostgreMPL.conexaoJohn()
        update = """
        update "Easy"."Fase"
        set  "nomeFase" = %s , "FaseInicial" = %s , "FaseFinal" = %s, "ObrigaInformaTamCor" = %s, "LeadTime" = %s
        where "codFase" = %s 
        """

        cursor = conn.cursor()
        cursor.execute(update,(nomeFase,FaseInicial, FaseFinal, ObrigaInformaTamCor, int(LeadTime), codFase ))
        conn.commit()
        cursor.close()

        conn.close()
        return pd.DataFrame([{'Mensagem': "FASE Alterada com Sucesso!", "status": True}])
