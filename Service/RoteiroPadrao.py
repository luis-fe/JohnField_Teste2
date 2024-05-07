import pandas as pd 
import ConexaoPostgreMPL
from Service import FaseJohnField

def BuscarRoteiros():
  consulta = """
  SELECT "codRoteiro", "nomeRoteiro", "codFase" FROM "Easy"."Roteiro"
  """
  conn = ConexaoPostgreMPL.conexaoJohn()
  consulta = pd.read_sql(consulta, conn)
  conn.close()


  Fases = FaseJohnField.BuscarFases()
  consulta = pd.merge(consulta,Fases,on='codFase')

  # Função para obter a primeira ocorrência de cada grupo
  def primeira_ocorrencia(group):
      return group.iloc[0]['ObrigaInformaTamCor?']

# Aplicando a função ao agrupar por 'roteiro'
  consulta['ObrigaInformaTamCor?'] = df.groupby('codRoteiro').apply(primeira_ocorrencia).reset_index(drop=True)

  consulta.drop(['codFase','FaseInical?',"FaseFinal?" ],axis=1,inplace=True)

  # Convertendo a coluna 'Tamanhos' para lista de strings
  consulta['nomeFase'] = consulta['nomeFase'].apply(lambda x: [x])

  # Agrupar tamanhos em uma lista
  df_summary = consulta.groupby(['codRoteiro', 'nomeRoteiro','ObrigaInformaTamCor?'])['nomeFase'].sum().reset_index()

  return df_summary


def InserirRoteiroPadrao(codRoteiro, nomeRoteiro, arrayFases ):
  conn = ConexaoPostgreMPL.conexaoJohn()
  verificar = BuscarRoteiroEspecifico(codRoteiro)

  if verificar.empty:

      # 1: Buscando os codFases

      consulta = pd.DataFrame({"nomeFase": arrayFases})
      Fases = FaseJohnField.BuscarFases()
      consulta = pd.merge(consulta, Fases, on='nomeFase')
      consulta.drop(['nomeFase', 'FaseInical?', "ObrigaInformaTamCor?", "FaseFinal?"], axis=1, inplace=True)
      arraycodFases = consulta['codFase'].values

      inserir = """
      insert into "Easy"."Roteiro" ("codRoteiro", "nomeRoteiro", "codFase") values ( %s , %s , %s )
      """
      for fase in arraycodFases:

        cursor = conn.cursor()
        cursor.execute(inserir,(codRoteiro, nomeRoteiro, int(fase)))
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
    conn.commit()

    InserirRoteiroPadrao(codRoteiro, nomeRoteiro, arrayFases)

    conn.close()
    return pd.DataFrame([{'Mensagem': "Roteiro Padrao Atualizado com sucesso", 'status': True}])

