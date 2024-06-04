import pandas as pd
import ConexaoPostgreMPL
import datetime
import pytz
from Service import Grades


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

def CrirarOP(codOP,idUsuarioCriacao,codCategoria,codCliente,descricaoOP, codGrade, codRoteiro):

    ChaveOP = codOP +'||'+str(codCliente)



    #Pesquisando se existe uma determinda OP
    buscar = BuscandoOPEspecifica(ChaveOP)
    if not buscar.empty:
        return pd.DataFrame([{'Mensagem': f'OP {codOP} ja existe para o cliente {codCliente}', 'Status': False}])
    else:
            consultaPrimeiraFase = """
              select r."codRoteiro" ,r."codFase"  from "Easy"."Roteiro" r 
                where r."codRoteiro" = %s limit 1;
            """

            InserirOP = """
            INSERT INTO "Easy"."OrdemProducao" ("codOP","idUsuarioCriacao","codCategoria","codCliente", "DataCriacao", "descricaoOP","codGrade","codRoteiro")
            VALUES (%s ,%s , %s ,%s , %s , %s , %s , %s );
            """
            DataCriacao = obterHoraAtual()

            conn = ConexaoPostgreMPL.conexaoJohn()

            consultaPrimeiraFase = pd.read_sql(consultaPrimeiraFase,conn,params=(int(codRoteiro),))
            codFaseInicial = consultaPrimeiraFase['codFase'][0]

            cursor = conn.cursor()
            cursor.execute(InserirOP,(codOP, idUsuarioCriacao, codCategoria, codCliente, DataCriacao, descricaoOP, codGrade,codRoteiro))
            conn.commit()


            InserirOPFase = """
            INSERT INTO "Easy"."Fase/OP" ("idOP", "DataMov", "codFase","Situacao") 
            VALUES (%s ,%s , %s ,%s );
            """
            cursor.execute(InserirOPFase,(ChaveOP, DataCriacao, int(codFaseInicial), 'Em Processo'))
            conn.commit()


            inserirRoteiro = """
            INSERT INTO "Easy"."RoteiroOP" ("codRoteiro", "nomeRoteiro", "codFase", "id", "chave")
            SELECT "codRoteiro", "nomeRoteiro", "codFase", "id", "chave" FROM "Easy"."Roteiro" where "codRoteiro" = %s ;"""
            cursor.execute(inserirRoteiro,(codRoteiro,))
            conn.commit()

            update = """
            update "Easy"."RoteiroOP"
            set "idOP" = %s
            where "idOP" is null
            """
            cursor.execute(update,(ChaveOP,))
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
    consulta['idOP'] = consulta['idOP'].str.replace('||','&')

    consulta['dataCriacaoOP'] = pd.to_datetime(consulta['dataCriacaoOP'], format='%a, %d %b %Y %H:%M:%S %Z')
    consulta['dataCriacaoOP'] = consulta['dataCriacaoOP'].dt.strftime('%d/%m/%Y')

    return consulta



def BuscarGradeOP(codOP, codCliente):
    ChaveOP = codOP +'||'+str(codCliente)


    consulta = """
    select  "idOP" , tamanho as "Tamanhos" from "Easy"."OP_Cores_Tam" oct 
    where "idOP" = %s
    """
    conn = ConexaoPostgreMPL.conexaoJohn()

    consulta = pd.read_sql(consulta,conn,params=(ChaveOP,))

    if consulta.empty:

        consulta2 = """
        select  "idOP" , "codGrade"  from "Easy"."OrdemProducao" op
        where "idOP" = %s
        """

        consulta2 = pd.read_sql(consulta2, conn, params=(ChaveOP,))
        consulta2['codOP'] = codOP
        consulta2['codCliente'] = codCliente

        grade = consulta2['codGrade'][0]
        dataFrame = Grades.BuscarGradeEspecifica(grade)
        df_summary = pd.merge(consulta2, dataFrame ,on='codGrade')
        df_summary.drop(['nomeGrade','idOP','codGrade'],axis=1,inplace=True)


    else:
        consulta['codOP'] = codOP
        consulta['codCliente'] = codCliente
        consulta['contagem'] = 1
        consulta['contagem'] = consulta.groupby(['codOP','Tamanhos'])['Tamanhos'].cumcount()
        consulta = consulta.sort_values(by=['contagem'], ascending=True)  # escolher como deseja classificar
        consulta = consulta[consulta['contagem']==0]
        print(consulta)
        # Convertendo a coluna 'Tamanhos' para lista de strings
        consulta['Tamanhos'] = consulta['Tamanhos'].apply(lambda x: [x])

        # Agrupar tamanhos em uma lista
        df_summary = consulta.groupby(['codOP', 'codCliente'])['Tamanhos'].sum().reset_index()


    conn.close()

    return df_summary

def ConsultaFaseAtualOP(codOP, codCliente):
    conn = ConexaoPostgreMPL.conexaoJohn()

    consulta = """"""
    conn.close()

    return consulta


def ConsultaRoteiroOP(codOP, codCliente):

    ChaveOP = codOP +'||'+str(codCliente)
    print(ChaveOP)
    conn = ConexaoPostgreMPL.conexaoJohn()


######          Passo 1 : Consulta SQL da Ordem de Producao e das Fases:
######
    consulta = """
select fo."idOP" , op."codRoteiro", fo."codFase" , "Situacao" from "Easy"."Fase/OP" fo 
inner join "Easy"."Fase" f on f."codFase" = fo."codFase" 
inner join "Easy"."OrdemProducao" op on op."idOP" = fo."idOP" 
where fo."idOP"  = %s and "Situacao" = 'Em Processo'
        """
    print(f'chave{ChaveOP}')
    consulta = pd.read_sql(consulta,conn,params=(ChaveOP,))
    print(consulta)

    consulta2 = """
    select r.* , f."nomeFase" as "nomefaseRoteiro",
     f."ObrigaInformaTamCor" as  "ObrigaInformaTamCor?"
     from "Easy"."RoteiroOP" r 
    inner join "Easy"."Fase" f on f."codFase" = r."codFase" 
    where r."codRoteiro" = %s order by r."id" asc 
    """
    roteiro = consulta['codRoteiro'][0]
    consulta2 = pd.read_sql(consulta2,conn,params=(int(roteiro),))


    consulta2['Sequencia'] = consulta2.groupby(['codRoteiro'])['codFase'].cumcount()+1
    consulta = pd.merge(consulta,consulta2,on='codFase').reset_index()
    consulta.drop('ObrigaInformaTamCor?',axis=1).reset_index()
    print(consulta)

    print(consulta2.loc[:,['codFase','ObrigaInformaTamCor?']])

    try:
        sequenciaAtual = consulta['Sequencia'][0]
    except:
        sequenciaAtual = 1
    sequenciaNova = sequenciaAtual + 1

    consulta2 = consulta2[consulta2['Sequencia']==sequenciaNova].reset_index()

    if consulta2.empty:
        consulta['102-ProximaFase'] = 'Encerramento'
        consulta['101-codProximaFase'] = 'Encerramento'
        consulta["ObrigaInformaTamCor?"] = 'NAO'

    else:
        consulta['102-ProximaFase'] = consulta2['nomefaseRoteiro'][0]
        consulta['101-codProximaFase'] = consulta2['codFase'][0]
        consulta["ObrigaInformaTamCor?"] = consulta2['ObrigaInformaTamCor?'][0]


    conn.close()

    consulta.rename(
        columns={'codFase': '001-codFaseAtual',"Situacao":"000-SituacaoOP","ObrigaInformaTamCor?":"103-ObrigaInformaTamCor?"},
        inplace=True)

    consulta = consulta.loc[:,["000-SituacaoOP",'001-codFaseAtual',"102-ProximaFase","101-codProximaFase","103-ObrigaInformaTamCor?"]]
    consulta = consulta.drop_duplicates()

    return consulta