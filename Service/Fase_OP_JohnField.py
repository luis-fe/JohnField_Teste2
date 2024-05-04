import pandas as pd
import ConexaoPostgreMPL
from Service import OP_JonhField, FaseJohnField

def MovimentarOP(idUsuarioMovimentacao, codOP, codCliente ,novaFase):
    idOP = str(codOP)+'||'+str(codCliente)
    nomeFaseNova = ObterNomeFase(novaFase)

    verifica = OP_JonhField.BuscandoOPEspecifica(idOP)
    verificaFaseAtual = OPAberto(codOP, codCliente)

    fasesDisponiveis = FasesDisponivelPMovimentarOP(codOP,codCliente)
    fasesDisponiveis = fasesDisponiveis[fasesDisponiveis['codFase']==novaFase]

    if verifica.empty:
        return pd.DataFrame([{'Mensagem':f'A OP {codOP} nao existe para o cliente {codOP} !','status':False}])

    elif verificaFaseAtual.empty:
        return pd.DataFrame([{'Mensagem':f'A OP {codOP}||{codCliente} nao está em aberto !','status':False}])

    elif verificaFaseAtual['FaseAtual'][0] == novaFase:
        return pd.DataFrame([{'Mensagem':f'A OP {codOP}||{codCliente} já exta aberta nessa fase {novaFase}-{nomeFaseNova} !','status':False}])

    elif fasesDisponiveis.empty:
        return pd.DataFrame([{'Mensagem':f'A Fase {novaFase}-{nomeFaseNova} nao esta disponivel para movimentacao!','status':False}])

    else:
        conn = ConexaoPostgreMPL.conexaoJohn()
        updateSituacao = """
        update "Easy"."Fase/OP"
        set "Situacao" = %s
        where "idOP" = %s
        """
        cursor = conn.cursor()
        cursor.execute(updateSituacao,('Movimentada',idOP,))
        conn.commit()
        cursor.close()

        insert = """
        insert into "Easy"."Fase/OP" ("codFase","idOP","idUsuarioMov","DataMov", "Situacao") values (%s, %s,  %s, %s, %s)
        """

        DataHora = OP_JonhField.obterHoraAtual()
        cursor = conn.cursor()
        cursor.execute(insert,(novaFase,idOP,idUsuarioMovimentacao,DataHora,'Em Processo'))
        conn.commit()
        cursor.close()

        conn.close()
        return pd.DataFrame([{'Mensagem':f'A OP {codOP}||{codOP} movimentada com sucesso!','status':True}])

def OPAberto(codOP, codCliente):
    ObterOP_EMAberto = OP_JonhField.ObterOP_EMAberto()
    ObterOP_EMAberto = ObterOP_EMAberto[(ObterOP_EMAberto['codOP']==codOP) &(ObterOP_EMAberto['codCliente']==codCliente)].reset_index()


    return ObterOP_EMAberto

def ObterNomeFase(codFase):
    fase = FaseJohnField.BuscarFaseEspecifica(codFase)
    nomeFase = fase['nomeFase'][0]
    return nomeFase


def FasesDisponivelPMovimentarOP(codOP, codCliente):
    idOP = str(codOP)+'||'+str(codCliente)
    fases = FaseJohnField.BuscarFases()
    consulta = """
    select "codFase", 'utilizado' as "faseUsada" from "Easy"."Fase/OP" fo 
    where fo."idOP" = %s
    """
    conn = ConexaoPostgreMPL.conexaoJohn()
    consulta = pd.read_sql(consulta,conn,params=(idOP,))

    consulta = pd.merge(fases,consulta,on='codFase', how='left')
    consulta.fillna('-',inplace=True)
    consulta = consulta[consulta['faseUsada'] == '-']
    conn.close()

    consulta = consulta.loc[:,['codFase','nomeFase']]

    return consulta

def EncerrarOP(idUsuarioMovimentacao, codOP, codCliente):
    idOP = str(codOP)+'||'+str(codCliente)
    verifica = OP_JonhField.BuscandoOPEspecifica(idOP)

    if verifica.empty:
        return pd.DataFrame([{'Mensagem':f'A OP {codOP} nao existe para o cliente {codOP} !','status':False}])
    else:
        conn = ConexaoPostgreMPL.conexaoJohn()
        updateUsuarioBaixa = """
               update "Easy"."Fase/OP"
               set "idUsuarioMovimentacao" = %s
               where "idOP" = %s and "Situacao" ='Em Processo'; 
               """
        cursor = conn.cursor()
        cursor.execute(updateUsuarioBaixa, (idUsuarioMovimentacao, idOP,))
        conn.commit()
        cursor.close()

        updateSituacao = """
               update "Easy"."Fase/OP"
               set "Situacao" = %s
               where "idOP" = %s
               """
        cursor = conn.cursor()
        cursor.execute(updateSituacao, ('Movimentada', idOP,))
        conn.commit()
        cursor.close()


        conn.close()

        return pd.DataFrame([{'Mensagem':'OP Encerrada com sucesso!','status':True}])