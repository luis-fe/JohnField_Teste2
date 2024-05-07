import pandas as pd 
import ConexaoPostgreMPL


def InserirRoteiroPadrao(codRoteiro, nomeRoteiro, arrayFases ):
  conn = ConexaoPostgreMPL.conexaoJohn()


  inserir = """
  insert into "Easy"."Roteiro" ("codRoteiro", "nomeRoteiro", "codFase") values ( %s , %s , %s )
  """
  for fase in arrayFases

    cursor = conn.cursor()
    cursor.execute(inserir,(codRoteiro, nomeRoteiro, fase))
    conn.commit()  
    cursor.close()
    
  conn.close()

  return pd.DataFrame([{'Mensagem':"Roteiro Padrao cadastrado com sucesso", 'status':True}])
