import pandas as pd
import ConexaoPostgreMPL
from Service import OP_JonhField, FaseJohnField, UsuariosJohnFild

def MovimentarOP(idUsuarioMovimentacao, codOP, codCliente ,novaFase):
    idOP = str(codOP)+'||'+str(codCliente)
    nomeFaseNova = ObterNomeFase(novaFase)

    usuarioPesquisa = UsuariosJohnFild.ConsultaUsuariosID(idUsuarioMovimentacao)

    verifica = OP_JonhField.BuscandoOPEspecifica(idOP)
    verificaFaseAtual = OPAberto(codOP, codCliente)

    fasesDisponiveis = FasesDisponivelPMovimentarOP(codOP,codCliente)
    fasesDisponiveis = fasesDisponiveis[fasesDisponiveis['codFase']==novaFase]

    if usuarioPesquisa.empty:
        return pd.DataFrame([{'Mensagem':f'O usuario  {idUsuarioMovimentacao} nao foi encontrado !','status':False}])

    elif verifica.empty:
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
        set "Situacao" = %s , "idUsuarioMov" = %s
        where "idOP" = %s and "Situacao" = 'Em Processo'
        """
        cursor = conn.cursor()
        cursor.execute(updateSituacao,('Movimentada',idUsuarioMovimentacao,idOP,))
        conn.commit()
        cursor.close()

        insert = """
        insert into "Easy"."Fase/OP" ("codFase","idOP","DataMov", "Situacao") values (%s,  %s, %s, %s)
        """

        DataHora = OP_JonhField.obterHoraAtual()
        cursor = conn.cursor()
        cursor.execute(insert,(novaFase,idOP,DataHora,'Em Processo'))
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
    idOP = str(codOP) + '||' + str(codCliente)

    fases = """
    SELECT f."codFase", 
           f."nomeFase", 
           f."FaseInicial" AS "FaseInical?",
           f."FaseFinal" AS "FaseFinal?", 
           f."ObrigaInformaTamCor" AS "ObrigaInformaTamCor?", 
           f."LeadTime"
    FROM "Easy"."Fase" f
    INNER JOIN "Easy"."Roteiro" r ON r."codFase" = f."codFase"
    WHERE r."codRoteiro" = (
        SELECT "codRoteiro" 
        FROM "Easy"."DetalhaOP_Abertas" doa 
        WHERE doa."codOP" = %s 
          AND doa."codCliente" = %s
    )
    """

    consulta = """
    SELECT "codFase", 'utilizado' AS "faseUsada" 
    FROM "Easy"."Fase/OP" fo 
    WHERE fo."idOP" = %s
    """

    conn = ConexaoPostgreMPL.conexaoJohn()

    try:
        consulta_df = pd.read_sql(consulta, conn, params=(idOP,))
        fases_df = pd.read_sql(fases, conn, params=(codOP, codCliente))

        result = pd.merge(fases_df, consulta_df, on='codFase', how='left')
        result.fillna('-', inplace=True)
        result = result[result['faseUsada'] == '-']
        result = result.loc[:, ['codFase', 'nomeFase']]
    finally:
        conn.close()

    return result

def EncerrarOP(idUsuarioMovimentacao, codOP, codCliente):
    idOP = str(codOP)+'||'+str(codCliente)
    verifica = OP_JonhField.BuscandoOPEspecifica(idOP)

    if verifica.empty:
        return pd.DataFrame([{'Mensagem':f'A OP {codOP} nao existe para o cliente {codOP} !','status':False}])
    else:
        conn = ConexaoPostgreMPL.conexaoJohn()
        updateUsuarioBaixa = """
               update "Easy"."Fase/OP"
               set "idUsuarioMov" = %s
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