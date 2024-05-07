import pandas as pd 
import ConexaoPostgreMPL


def InserirRoteiroPadrao(codRoteiro, nomeRoteiro, arrayFases ):
  conn = ConexaoPostgreMPL.conexaoJohn()
  verificar = BuscarRoteiroEspecifico(codRoteiro)

  if verificar.empty:
      inserir = """
      insert into "Easy"."Roteiro" ("codRoteiro", "nomeRoteiro", "codFase") values ( %s , %s , %s )
      """
      for fase in arrayFases:

        cursor = conn.cursor()
        cursor.execute(inserir,(codRoteiro, nomeRoteiro, fase))
        conn.commit()
        cursor.close()

      conn.close()

      return pd.DataFrame([{'Mensagem':"Roteiro Padrao cadastrado com sucesso", 'status':True}])

  else:
    return pd.DataFrame([{'Mensagem': "Roteiro Padrao Ja existe", 'status': False}])


def BuscarRoteiroEspecifico(codRoteiro):
  consulta = """
  select * from "Easy"."Roteiro" where "codRoteiro" = %s
  """
  conn = ConexaoPostgreMPL.conexaoJohn()
  consulta = pd.read_sql(consulta, conn,params=(codRoteiro,))
  conn.close()

  return consulta

def UpdateRoteiro(codRoteiro, nomeRoteiro, arrayFases):
  verificar = BuscarRoteiroEspecifico(codRoteiro)

  if verificar.empty:
    return pd.DataFrame([{'Mensagem': "O Roteiro Padrao Nao foi encontrado", 'status': False}])


  else:

    consulta = """ 
    delete from  "Easy"."Roteiro"
    where "codRoteiro" = %s 
     """
    conn = ConexaoPostgreMPL.conexaoJohn()

    cursor = conn.cursor()
    cursor.execute(consulta,(codRoteiro,))

    InserirRoteiroPadrao(codRoteiro, nomeRoteiro, arrayFases)

    conn.close()
    return pd.DataFrame([{'Mensagem': "Roteiro Padrao Atualizado com sucesso", 'status': True}])

