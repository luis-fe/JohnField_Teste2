import pandas as pd
import ConexaoPostgreMPL
import datetime

def obterHoraAtual():
    agora = datetime.datetime.now()
    hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
    return hora_str


def BuscarFaseInicio():

    consulta = """
select f."codFase" ,"nomeFase"  from "Easy"."Fase" f 
where f."FaseInicial" = 'SIM'
    """

    conn = ConexaoPostgreMPL.conexaoJohn()
    consulta = pd.read_sql(consulta,conn)
    conn.close()

    return consulta

def CrirarOP(codOP,idUsuarioCriacao,codCategoria,codCliente,nomeFaseInicial):

    InserirOP = """
    INSERT INTO "Easy"."OrdemProducao" ("codOP","idUsuarioCriacao","codCategoria","codCliente", "DataCriacao")
	VALUES (%s ,%s , %s ,%s );
    """
    DataCriacao = obterHoraAtual()

    conn = ConexaoPostgreMPL.conexaoJohn()


    ChaveOP = codOP +'||'+codCliente
    FaseInicial = nomeFaseInicial

    InserirOPFase = """
    INSERT INTO "Easy"."Fase/OP" ("idOP", "DataMov", "codFase","Situacao") 
    VALUES (%s ,%s , %s ,%s );
    """


    conn.close()

def GradePadraoCriacaoOP():
    consulta = """
    select * from "Easy"."gradePadrao"
    """
    conn = ConexaoPostgreMPL.conexaoJohn()
    consulta = pd.read_sql(consulta, conn)
    conn.close()

    return consulta




