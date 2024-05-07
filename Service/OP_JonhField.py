import pandas as pd
import ConexaoPostgreMPL
import datetime
import pytz


def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
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

def CrirarOP(codOP,idUsuarioCriacao,codCategoria,codCliente,codFaseInicial,descricaoOP):

    ChaveOP = codOP +'||'+str(codCliente)

    #Pesquisando se existe uma determinda OP
    buscar = BuscandoOPEspecifica(ChaveOP)
    if not buscar.empty:
        return pd.DataFrame([{'Mensagem': f'OP {codOP} ja existe para o cliente {codCliente}', 'Status': False}])
    else:
        verificarFaseInicial = BuscarFaseInicio(codFaseInicial)
        if verificarFaseInicial.empty:
            return pd.DataFrame([{'Mensagem': f'A Fase {codFaseInicial}  não é fase de Inicio!', 'Status': False}])

        else:

            InserirOP = """
            INSERT INTO "Easy"."OrdemProducao" ("codOP","idUsuarioCriacao","codCategoria","codCliente", "DataCriacao", "descricaoOP")
            VALUES (%s ,%s , %s ,%s , %s , %s );
            """
            DataCriacao = obterHoraAtual()

            conn = ConexaoPostgreMPL.conexaoJohn()
            cursor = conn.cursor()
            cursor.execute(InserirOP,(codOP, idUsuarioCriacao, codCategoria, codCliente, DataCriacao, descricaoOP))
            conn.commit()


            InserirOPFase = """
            INSERT INTO "Easy"."Fase/OP" ("idOP", "DataMov", "codFase","Situacao") 
            VALUES (%s ,%s , %s ,%s );
            """
            cursor.execute(InserirOPFase,(ChaveOP, DataCriacao, codFaseInicial, 'Em Processo'))
            conn.commit()


            cursor.close()
            conn.close()

            return pd.DataFrame([{'Mensagem':'OP Gerada com Sucesso!', 'Status':True}])

def ObterOP_EMAberto():
    consulta = """
    select * from "Easy"."DetalhaOP_Abertas"
    """
    conn = ConexaoPostgreMPL.conexaoJohn()
    consulta = pd.read_sql(consulta,conn)
    consulta['idOP'] = consulta['codOP']  + "||"+consulta['codCliente'].astype(str)
    quantidade ="""
    select "idOP" , sum("quantidade") as quantidade from "Easy"."OP_Cores_Tam" group by "idOP" 
    """
    quantidade = pd.read_sql(quantidade,conn)
    consulta = pd.merge(consulta,quantidade, on ='idOP', how='left')
    consulta['quantidade'].fillna("-",inplace= True)

    conn.close()

    return consulta



def BuscarGradeOP(codOP, codCliente):
    ChaveOP = codOP +'||'+str(codCliente)

    consulta = """
    select distinct "idOP" , tamanho as "Tamanhos" from "Easy"."OP_Cores_Tam" oct 
    where "idOP" = %s
    """
    conn = ConexaoPostgreMPL.conexaoJohn()

    consulta = pd.read_sql(consulta,conn,params=(ChaveOP,))
    consulta['codOP'] = codOP
    consulta['codCliente'] = codCliente

    # Convertendo a coluna 'Tamanhos' para lista de strings
    consulta['Tamanhos'] = consulta['Tamanhos'].apply(lambda x: [x])

    # Agrupar tamanhos em uma lista
    df_summary = consulta.groupby(['codOP', 'codCliente'])['Tamanhos'].sum().reset_index()


    conn.close()

    return df_summary

