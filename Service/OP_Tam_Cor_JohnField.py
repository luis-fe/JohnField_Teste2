import pandas as pd
import ConexaoPostgreMPL
import OP_JonhField

def InserirCoresTamanhos(codOP, codCliente, arrayCores, arrayTamanhos, arrayQuantiades):
    idOP = str(codOP)+'||'+str(codCliente)
    VerificaOP = OP_JonhField.BuscandoOPEspecifica(idOP)

    if VerificaOP.empty:
        return pd.DataFrame([{'mensagem':f'A OP {codOP}, cliente {codCliente} nao foi indentificada!', 'status':False}])
    else:

        conn = ConexaoPostgreMPL
        cursor = conn.cursor()

        for t in arrayTamanhos:
            for cor in arrayCores:
                for quant in arrayQuantiades:

                    inserir = """
                    INSERT INTO "Easy"."OP_Cores_Tam" ("idOP", "descCor", "tamanho", "quantidade")
                    VALUES (%s, %s, %s, %s)
                    """
                    cursor.execute(inserir, (idOP, cor, t,quant))
                    conn.commit()

        cursor.close()
        conn.close()
        return pd.DataFrame([{'mensagem':f'Tamanhos e Cores inseridos com Sucesso!', 'status':True}])

