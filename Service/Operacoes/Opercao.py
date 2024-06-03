import pandas as pd
import ConexaoPostgreMPL
from Service import FaseJohnField

def BuscarOperacoes():
    conn = ConexaoPostgreMPL.conexaoJohn()

    consulta = """
        select  c.*, f."nomeFase",c2."nomeCategoria"  ,to2."tempoPadrao"  from "Easy"."Operacao" c
    inner join "Easy"."Fase" f on f."codFase" = c."codFase"
    inner join "Easy"."TemposOperacao" to2 on to2."codOperacao" = c."codOperacao" 
    inner join "Easy"."Categoria" c2 on c2.codcategoria = to2."codCategoria" 
    """

    consulta = pd.read_sql(consulta,conn)
    conn.close()

    return consulta

def BuscarOperacaoEspecifica(codOperacao):
    conn = ConexaoPostgreMPL.conexaoJohn()

    consulta = """
    select c.*  from "Easy"."Operacao" c  
    where c."codOperacao" = %s
    """

    consulta = pd.read_sql(consulta,conn,params=(codOperacao,))
    conn.close()

    return consulta

def InserirOperacao(codOperacao, nomeOperacao, nomeFase, Maq_Equipamento):
    consulta = BuscarOperacaoEspecifica(codOperacao)

    if consulta.empty:
        codFase = FaseJohnField.BuscarFases()
        codFase = codFase[codFase['nomeFase']==nomeFase].reset_index()
        if codFase.empty:
            return pd.DataFrame([{'Mensagem': f"Fase {nomeFase} nao ´existe!", "status": False}])
        else:

            codFase = codFase['codFase'][0]


            conn = ConexaoPostgreMPL.conexaoJohn()
            inserir = """
            insert into "Easy"."Operacao" ("codOperacao" , "codFase", "Maq/Equipamento","nomeOperacao") values ( %s, %s,  %s, %s )
            """
            cursor = conn.cursor()
            cursor.execute(inserir,(codOperacao,int(codFase) ,Maq_Equipamento,nomeOperacao,))
            conn.commit()
            cursor.close()

            conn.close()

            return pd.DataFrame([{'Mensagem': "Operacão cadastrada com Sucesso!", "status": True}])

    else:
        return pd.DataFrame([{'Mensagem': "Operacão já´existe!", "status": False}])

def UpdateOperacao(codOperacao, nomeOperacao,nomeFase,  Maq_Equipamento):

    consulta = BuscarOperacaoEspecifica(codOperacao)
    print(consulta)

    if consulta.empty:
        return pd.DataFrame([{'Mensagem':"Operacao Nao encontrada!","status":False}])
    else:

            codFase = FaseJohnField.BuscarFases()
            codFase = codFase[codFase['nomeFase'] == nomeFase].reset_index()

            if codFase.empty:
                return pd.DataFrame([{'Mensagem': f"Fase {nomeFase} nao ´existe!", "status": False}])
            else:

                codFase = codFase['codFase'][0]

                nomeOperacaoAtual = consulta['nomeOperacao'][0]
                if nomeOperacaoAtual == nomeOperacao :
                    nomeOperacao = nomeOperacaoAtual

                codFaseAtual = consulta['codFase'][0]
                if codFaseAtual == codFase :
                    codFase = codFaseAtual

                Maq_EquipamentoAtual = consulta['Maq/Equipamento'][0]
                if Maq_EquipamentoAtual == Maq_Equipamento :
                    Maq_Equipamento = Maq_EquipamentoAtual


                conn = ConexaoPostgreMPL.conexaoJohn()
                update = """
                    update "Easy"."Operacao"
                    set  "codFase" = %s , "Maq/Equipamento" = %s , "nomeOperacao" = %s
                    where "codOperacao" = %s 
                    """

                cursor = conn.cursor()
                cursor.execute(update,(int(codFase),Maq_Equipamento,nomeOperacao, codOperacao,))
                conn.commit()
                cursor.close()

                conn.close()
                return pd.DataFrame([{'Mensagem': "Operacao Alterado com Sucesso!", "status": True}])