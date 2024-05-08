import pandas as pd
import ConexaoPostgreMPL


def OPsAbertoPorCliente():

    consulta = """
    select * from "Easy"."DetalhaOP_Abertas" doa 
    """

    quantidade = """
      select "idOP" , sum(quantidade) as quantidade  from "Easy"."OP_Cores_Tam" oct 
  group by oct."idOP" 
    """

    conn = ConexaoPostgreMPL.conexaoJohn()
    consulta = pd.read_sql(consulta,conn)
    quantidade = pd.read_sql(quantidade,conn)
    consulta['idOP'] = consulta["codOP"] +'||'+consulta["codCliente"].astype(str)

    conn.close()


    consulta = pd.merge(consulta,quantidade,on='idOP',how='left')
    consulta['quantidade'].fillna(0,inplace=True)
    pcsAberto = consulta['quantidade'].sum()

    dados = {
        '0-Total De pçs em Aberto': f'{pcsAberto} Pçs ',
        '3 -Detalhamento': consulta.to_dict(orient='records')}

    return pd.DataFrame([dados])
