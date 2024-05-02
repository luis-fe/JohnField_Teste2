import pandas as pd
import ConexaoPostgreMPL
from Service import OP_JonhField

def InserirCoresTamanhos(codOP, codCliente, arrayCorTamQuantiades):
    idOP = str(codOP)+'||'+str(codCliente)
    VerificaOP = OP_JonhField.BuscandoOPEspecifica(idOP)

    if VerificaOP.empty:
        return pd.DataFrame([{'mensagem':f'A OP {codOP}, cliente {codCliente} nao foi indentificada!', 'status':False}])
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
    select "idOP" , "descCor", "tamanho", "quantidade" from "Easy"."OP_Cores_Tam" 
    where "idOP" = %s
    """
    conn = ConexaoPostgreMPL.conexaoJohn()
    consulta = pd.read_sql(consulta,conn,params=(idOP,))
    conn.close()
    return consulta


