import pandas as pd
import ConexaoPostgreMPL

def cancelamentoOP(codOP, codCliente):
    chaveOP = str(codOP)+'||'+str(codCliente)
    conn = ConexaoPostgreMPL.conexaoJohn()

    OP_Cores_Tam = """delete from "Easy"."OP_Cores_Tam"
    where "idOP"  = %s """
    cursor = conn.cursor()
    cursor.execute(OP_Cores_Tam,(chaveOP))
    conn.commit()


    OP_Cores_Tam = """delete from "Easy"."OP_Cores_Tam"
    where "idOP"  = %s """
    cursor = conn.cursor()
    cursor.execute(OP_Cores_Tam,(chaveOP))
    conn.commit()


    FaseOP = """delete from "Easy"."Fase/OP"
    where "idOP"  = %s """
    cursor = conn.cursor()
    cursor.execute(FaseOP,(chaveOP))
    conn.commit()

    OrdemProducao = """delete from "Easy"."OrdemProducao"
    where "idOP"  = %s """
    cursor = conn.cursor()
    cursor.execute(FaseOP,(OrdemProducao))
    conn.commit()

    cursor.close()
    conn.close()

    return pd.DataFrame([{'status':True, 'Mensagem':'OP cancalada com sucesso !'}])


def AutentificacaoCancelamento(nomeLogin, senha):
    conn = ConexaoPostgreMPL.conexaoJohn()
    sql = """select idusuario , "nomeUsuario" , "nomeLogin" , "Senha" , permite_cancelar_op  from "Easy"."Usuario" u 
        where nomeLogin = %s """

    consulta = pd.read_sql(sql,conn,params=(nomeLogin))
    consulta['permite_cancelar_op'].fillna('NAO',inplace=True)
    conn.close()
    permissao = consulta['permite_cancelar_op'][0]
    senhaAtual = consulta['Senha'][0]

    if permissao == 'NAO':
        return pd.DataFrame([{'status':False ,'Mensagem':'Usuario nao habilitado para cancelar op'}])
    elif senhaAtual != senha:
        return pd.DataFrame([{'status':False ,'Mensagem':'Senha informada nao corresponde '}])

    else:
        return pd.DataFrame([{'status':True ,'Mensagem':'Habilitado para cancelar OP'}])

