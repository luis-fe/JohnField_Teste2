import pandas as pd
import ConexaoPostgreMPL
import datetime

def obterHoraAtual():
    agora = datetime.datetime.now()
    hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
    return hora_str


def BuscandoOPEspecifica(idOP):
    consulta = """
    select op."idOP"  from "Easy"."OrdemProducao" op 
where "idOP" = %s
    """
    conn = ConexaoPostgreMPL.conexaoJohn()
    consulta = pd.read_sql(consulta,conn, params=(idOP,))
    conn.close()

    return consulta


def BuscarFaseInicio(codFase):

    consulta = """
select f."codFase" ,"nomeFase"  from "Easy"."Fase" f 
where f."FaseInicial" = 'SIM' and "codFase" = %s
    """

    conn = ConexaoPostgreMPL.conexaoJohn()
    consulta = pd.read_sql(consulta,conn,params=(codFase,))
    conn.close()

    return consulta

def CrirarOP(codOP,idUsuarioCriacao,codCategoria,codCliente,codFaseInicial):

    ChaveOP = codOP +'||'+codCliente

    #Pesquisando se existe uma determinda OP
    buscar = BuscandoOPEspecifica(ChaveOP)
    if buscar.empty:
        return pd.DataFrame([{'Mensagem': f'OP {codOP} ja existe para o cliente {codCliente}', 'Status': False}])
    else:
        verificarFaseInicial = BuscarFaseInicio(codFaseInicial)
        if verificarFaseInicial.empty:
            return pd.DataFrame([{'Mensagem': f'A Fase {codFaseInicial}  não é fase de Inicio!', 'Status': False}])

        else:

            InserirOP = """
            INSERT INTO "Easy"."OrdemProducao" ("codOP","idUsuarioCriacao","codCategoria","codCliente", "DataCriacao")
            VALUES (%s ,%s , %s ,%s );
            """
            DataCriacao = obterHoraAtual()

            conn = ConexaoPostgreMPL.conexaoJohn()
            cursor = conn.cursor()
            cursor.execute(InserirOP,(codOP, idUsuarioCriacao, codCategoria, codCliente, DataCriacao))
            conn.commit()


            InserirOPFase = """
            INSERT INTO "Easy"."Fase/OP" ("idOP", "DataMov", "codFase","Situacao") 
            VALUES (%s ,%s , %s ,%s );
            """
            cursor.execute(InserirOPFase,(ChaveOP, DataCriacao, codCategoria, codFaseInicial, 'Em Processo'))
            conn.commit()


            cursor.close()
            conn.close()

            return pd.DataFrame([{'Mensagem':'OP Gerada com Sucesso!', 'Status':True}])





