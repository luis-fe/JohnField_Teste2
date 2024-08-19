import pandas as pd
import ConexaoPostgreMPL


def EstornoOP(codOP, codCliente, idUsuarioEstorno):
    idUsuarioEstorno = int(idUsuarioEstorno)
    chaveOP = str(codOP)+'||'+str(codCliente)

    # Conferir se existe op em aberto
    conferir = """select * from railway."Easy"."DetalhaOP_Abertas" doa 
    where doa."codOP" = %s and doa."codCliente" = %s
    """

    roteiroSql = """select id, "codFase" from railway."Easy"."Roteiro" r where r."codRoteiro" = %s """

    with ConexaoPostgreMPL.conexaoJohn() as conn:
        conferirPD = pd.read_sql(conferir, conn, params=(codOP, codCliente,)).reset_index()

    if conferirPD.empty:
        return pd.DataFrame([{'status':False , 'Mensagem':'A OP nao se encontra em aberto'}])
    else:
        # Conferir se é a primeira fase
        roteiro = int(conferirPD['codRoteiro'][0])
        with ConexaoPostgreMPL.conexaoJohn() as conn:
            roteiroPD = pd.read_sql(roteiroSql, conn, params=(roteiro,))
            roteiroid = roteiroPD[roteiroPD['id']==1].reset_index()
            roteiroid = roteiroid['codFase'][0]

        if int(roteiroid) == int(conferirPD['codFase'][0]):
            return pd.DataFrame([{'status':False , 'Mensagem':'OP está na fase inicial ja'}])
        else:

            delete = """
            delete from railway."Easy"."Fase/OP" fo  where fo."Situacao" = 'Em Processo'
            and "idOP" = %s
            """

            update = """
            update  railway."Easy"."Fase/OP" fo 
            set "idUsuarioMov" = %s , "Situacao"= 'Em Processo'
            where fo."idOP" = %s
            and "DataMov" =(
            select  max("DataMov") from railway."Easy"."Fase/OP" fo 
            where fo."idOP" = %s and "Situacao" = 'Movimentada')
            """

            with ConexaoPostgreMPL.conexaoJohn() as conn:
                with conn.cursor() as cur:
                    cur.execute(delete,(chaveOP,))
                    conn.commit()

                    cur.execute(update,(idUsuarioEstorno, chaveOP,chaveOP,))
                    conn.commit()
            print('ok')
            return pd.DataFrame([{'status':True , 'Mensagem':'OP estornada com sucesso'}])

#EstornoOP('134985-1','1',1414)




