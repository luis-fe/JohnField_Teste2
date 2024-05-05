import pandas as pd
import ConexaoPostgreMPL
from Service import OP_JonhField

def InserirCoresTamanhos(codOP, codCliente, arrayCorTamQuantiades):
    idOP = str(codOP)+'||'+str(codCliente)
    VerificaOP = OP_JonhField.BuscandoOPEspecifica(idOP)
    consulta = ConsultaTamCor_OP(codOP,codCliente)
    if VerificaOP.empty:
        return pd.DataFrame([{'mensagem':f'A OP {codOP}, cliente {codCliente} nao foi indentificada!', 'status':False}])

    elif not consulta.empty:
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



def ConsultaTamCor_OP(codOP, codCliente):
    idOP = str(codOP)+'||'+str(codCliente)
    consulta = """
    select "descCor", "tamanho", "quantidade" from "Easy"."OP_Cores_Tam" 
    where "idOP" = %s
    """
    conn = ConexaoPostgreMPL.conexaoJohn()
    consulta = pd.read_sql(consulta,conn,params=(idOP,))
    conn.close()
    if consulta.empty:

        return pd.DataFrame([{"Mensagem":"Nao foi encontrado grade para a OP informada!",'status':False}])
    else:
        consulta['codOP'] = codOP
        consulta['codCliente'] = codCliente

        # Convertendo a coluna 'Tamanhos' para lista de strings
        consulta['Tamanhos'] = consulta['Tamanhos'].apply(lambda x: [x])
        consulta['descCor'] = consulta['descCor'].apply(lambda x: [x])

        # Agrupar tamanhos em uma lista
        df_summary = consulta.groupby(['codOP', 'codCliente'])['Tamanhos','descCor'].sum().reset_index()

        return df_summary


def AtualizarCoresTamanhos(codOP, codCliente, arrayCorTamQuantiades):
    idOP = str(codOP)+'||'+str(codCliente)
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