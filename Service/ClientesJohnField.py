import pandas as pd
import ConexaoPostgreMPL

def ConsultaClientes():
    conn = ConexaoPostgreMPL.conexaoJohn()
    consulta = pd.read_sql("""
    select "codcliente" ,"nomeCliente" from "Easy"."Cliente" c  
    """,conn)
    conn.close()

    return consulta

def ConsultaClientesEspecifico(id_cliente):
    conn = ConexaoPostgreMPL.conexaoJohn()
    consulta = pd.read_sql("""
    select "codcliente" ,"nomeCliente" from "Easy"."Cliente" c 
    where  "codcliente" = %s
    """,conn, params=(id_cliente,))
    conn.close()

    return consulta


def inserirCliente(idCliente, nomeCliente):
    consulta = ConsultaClientesEspecifico(idCliente)

    if consulta.empety:
        conn = ConexaoPostgreMPL.conexaoJohn()

        insert = """
        insert into "Easy"."Cliente" ("codcliente" ,"nomeCliente" ) values ( %s , %s )
        """

        cursor = conn.cursor()
        cursor.execute(insert,(idCliente, nomeCliente,))
        conn.commit()
        cursor.close()

        conn.close()

        return pd.DataFrame([{'Mensagem': "Cliente cadastrado com Sucesso!", "status": True}])

    else:
        return pd.DataFrame([{'Mensagem': "Cliente já´existe!", "status": False}])

def UpdateCliente(id_cliente, nomeCliente):

    consulta = ConsultaClientesEspecifico(id_cliente)

    if consulta.empty:
        return pd.DataFrame([{'Mensagem':"Cliente Nao encontrado!","status":False}])
    else:
        nomeClienteAtual = consulta['nomeCliente'][0]
        if nomeClienteAtual == nomeCliente :
            nomeCliente = nomeClienteAtual


        conn = ConexaoPostgreMPL.conexaoJohn()
        update = """
        update "Easy"."Cliente"
        set  "nomeCliente" = %s 
        where "codcliente" = %s 
        """

        cursor = conn.cursor()
        cursor.execute(update,(nomeCliente,id_cliente,))
        conn.commit()
        cursor.close()

        conn.close()
        return pd.DataFrame([{'Mensagem': "Usuario Alterado com Sucesso!", "status": True}])