import pandas as pd
import ConexaoPostgreMPL
from Service import OP_JonhField

def InserirCoresTamanhos(codOP, codCliente, arrayCorTamQuantiades, codEmpresa = '1'):
    idOP = str(codOP)+'||'+str(codCliente)+'||'+str(codEmpresa)
    VerificaOP = OP_JonhField.BuscandoOPEspecifica(idOP)
    consulta = ConsultaTamCor_OP(codOP,codCliente)
    if VerificaOP.empty:
        return pd.DataFrame([{'mensagem':f'A OP {codOP}, cliente {codCliente} nao foi indentificada!', 'status':False}])

    elif consulta['status'][0] == True:
        return pd.DataFrame([{'mensagem':f'JA existe tamanho e cor cadastrado para  OP {codOP}, cliente {codCliente}  !', 'status':False}])


    else:

        conn = ConexaoPostgreMPL.conexaoJohn()

        for matriz in arrayCorTamQuantiades:
            cor = matriz[0]
            t = matriz[1]
            quant = matriz[2]

            inserir = """
                INSERT INTO "Easy"."OP_Cores_Tam" ("idOP", "descCor", "tamanho", "quantidade")
                VALUES (%s, %s, %s, %s)
                """
            cursor = conn.cursor()
            cursor.execute(inserir, (idOP, cor, t,quant))
            conn.commit()
            cursor.close()

        conn.close()
        return pd.DataFrame([{'mensagem':f'Tamanhos e Cores inseridos com Sucesso!', 'status':True}])



def ConsultaTamCor_OP(codOP, codCliente, codEmpresa = '1'):
    idOP = str(codOP)+'||'+str(codCliente)+'||'+str(codEmpresa)
    consulta = """
    select "descCor", "tamanho", "quantidade" from "Easy"."OP_Cores_Tam" op
    inner join "Easy"."Tamanhos" t on t."DescricaoTamanho" = op.tamanho 
    where "idOP" = %s order by t.codsequencia asc
    """
    conn = ConexaoPostgreMPL.conexaoJohn()
    consulta = pd.read_sql(consulta,conn,params=(idOP,))
    conn.close()
    if consulta.empty:

        return pd.DataFrame([{"Mensagem":"Nao foi encontrado grade para a OP informada!",'status':False}])
    else:
        consulta['codOP'] = codOP
        consulta['codCliente'] = codCliente

        resumoConsulta = consulta.groupby(['codOP', 'codCliente','descCor']).agg(
            {'tamanho': list, 'quantidade': list}).reset_index()
        resumoConsulta['status'] = True

        return resumoConsulta


def AtualizarCoresTamanhos(codOP, codCliente, arrayCorTamQuantiades,codEmpresa = '1'):
    idOP = str(codOP)+'||'+str(codCliente)+'||'+str(codEmpresa)
    VerificaOP = OP_JonhField.BuscandoOPEspecifica(idOP)

    if VerificaOP.empty:
        return pd.DataFrame([{'mensagem':f'A OP {codOP}, cliente {codCliente} nao foi indentificada!', 'status':False}])
    else:

        conn = ConexaoPostgreMPL.conexaoJohn()

        Deletar = """
        delete from "Easy"."OP_Cores_Tam" 
        where "idOP" = %s
        """

        cursor = conn.cursor()
        cursor.execute(Deletar, (idOP,))
        conn.commit()
        cursor.close()

        for matriz in arrayCorTamQuantiades:
            cor = matriz[0]
            t = matriz[1]
            quant = matriz[2]

            inserir = """
                INSERT INTO "Easy"."OP_Cores_Tam" ("idOP", "descCor", "tamanho", "quantidade")
                VALUES (%s, %s, %s, %s)
                """
            cursor = conn.cursor()
            cursor.execute(inserir, (idOP, cor, t,quant))
            conn.commit()
            cursor.close()

        conn.close()
        return pd.DataFrame([{'mensagem':f'Tamanhos e Cores inseridos com Sucesso!', 'status':True}])