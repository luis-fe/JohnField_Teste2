import pandas as pd
import ConexaoPostgreMPL
import datetime
import pytz

def cancelamentoOP(codOP, codCliente, idusuario):
    chaveOP = str(codOP)+'||'+str(codCliente)
    conn = ConexaoPostgreMPL.conexaoJohn()

    OP_Cores_Tam = """delete from "Easy"."OP_Cores_Tam"
    where "idOP"  = %s """
    cursor = conn.cursor()
    cursor.execute(OP_Cores_Tam,(chaveOP,))
    conn.commit()


    OP_Cores_Tam = """delete from "Easy"."OP_Cores_Tam"
    where "idOP"  = %s """
    cursor = conn.cursor()
    cursor.execute(OP_Cores_Tam,(chaveOP,))
    conn.commit()


    FaseOP = """delete from "Easy"."Fase/OP"
    where "idOP"  = %s """
    cursor = conn.cursor()
    cursor.execute(FaseOP,(chaveOP,))
    conn.commit()

    OrdemProducao = """delete from "Easy"."OrdemProducao"
    where "idOP"  = %s """
    cursor = conn.cursor()
    cursor.execute(FaseOP,(OrdemProducao,))
    conn.commit()

    RegistroExclusao = """insert into  "Easy"."RegistroOPCancelada"
    ("codOP", "codCliente", "dataCancelamento","usuario_autentificacao") values
    (%s, %s , %s , %s)"""
    datahora = obterHoraAtual()

    cursor = conn.cursor()
    cursor.execute(RegistroExclusao,(codOP,codCliente,datahora, int(idusuario)))
    conn.commit()

    cursor.close()
    conn.close()

    return pd.DataFrame([{'status':True, 'Mensagem':'OP cancalada com sucesso !'}])


def cancelarOP(nomeLogin, senha, codOP, codCliente):
    conn = ConexaoPostgreMPL.conexaoJohn()
    sql = """select idusuario , "nomeUsuario" , "nomeLogin" , "Senha" , permite_cancelar_op  from "Easy"."Usuario" u 
        where "nomeLogin" = %s """

    consulta = pd.read_sql(sql,conn,params=(nomeLogin,))
    consulta['permite_cancelar_op'].fillna('NAO',inplace=True)
    conn.close()
    permissao = consulta['permite_cancelar_op'][0]
    senhaAtual = consulta['Senha'][0]

    if permissao == 'NAO':
        return pd.DataFrame([{'status':False ,'Mensagem':'Usuario nao habilitado para cancelar op'}])
    elif senhaAtual != senha:
        return pd.DataFrame([{'status':False ,'Mensagem':'Senha informada nao corresponde '}])

    else:
        usuario = consulta['idusuario'][0]
        cancelamentoOP(codOP, codCliente, usuario )
        return pd.DataFrame([{'status':True ,'Mensagem':'Op Cancelada com sucesso'}])

def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
    return hora_str
