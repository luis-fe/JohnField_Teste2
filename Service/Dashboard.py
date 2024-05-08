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
    pcsAberto = round(pcsAberto)
    pcsAberto = '{:,.0f}'.format(pcsAberto)
    pcsAberto = pcsAberto.replace(',', '.')

    OPAberto = consulta['codOP'].count()
    OPAberto = round(OPAberto)
    OPAberto = '{:,.0f}'.format(OPAberto)
    OPAberto = OPAberto.replace(',', '.')

    ClienteAberto = consulta['codCliente'].drop_duplicates().count()
    ClienteAberto = round(ClienteAberto)
    ClienteAberto = '{:,.0f}'.format(ClienteAberto)
    ClienteAberto = ClienteAberto.replace(',', '.')

    dados = {
        '0-Total De pçs em Aberto': f'{pcsAberto} Pçs ',
        '1- Total De OPs em Abero': f'{OPAberto} OPs ',
        '2- Total de Clientes em Aberto':f'{ClienteAberto} Clientes ',
        '3 -Detalhamento': consulta.to_dict(orient='records')}

    return pd.DataFrame([dados])
