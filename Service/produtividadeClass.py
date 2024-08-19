import pandas as pd
import ConexaoPostgreMPL
from datetime import datetime

class Produtividade():
    def __init__(self, dataInicio = None, dataFinal =None):
        self.dataInicio = dataInicio
        self.dataFinal = dataFinal

    def ProdutividadePorCategoriaOperacao(self):
        sql = """
        select
	        co.nomecategoria,
	        max(co.metadiaria) as "MetaDia",
	        sum(cp."qtdPcs") as "Realizado"
        from
	        "Easy".categoriaoperacao co
        inner join "Easy"."Operacao" o on
	        o.categoriaoperacao = id_categoria::Varchar
        inner join "Easy"."ColetasProducao" cp on
	        cp."nomeOperacao" = o."nomeOperacao"
        where
	        cp."Data" >= %s and cp."Data" <= %s 
        group by
	        co.nomecategoria
        """

        sql2 = """
        	select
	            co.nomecategoria, cp."nomeOperador" 
            from 
	            "Easy".categoriaoperacao co
            inner join "Easy"."Operacao" o on
	            o.categoriaoperacao = id_categoria::Varchar
            inner join "Easy"."ColetasProducao" cp on
	            cp."nomeOperacao" = o."nomeOperacao"
            where
	        cp."Data" >= %s and cp."Data" <= %s 
            group by
	            co.nomecategoria, cp."nomeOperador" 
        """
        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql,conn,params=(self.dataInicio, self.dataFinal))

        #Implementar o Numerero de Operadores que realizacao a funcao no dia
        consulta2 = pd.read_sql(sql2,conn,params=(self.dataInicio, self.dataFinal))
        consulta2['QtdOperadores'] = 1
        consulta2 = consulta2.groupby(['nomecategoria']).agg(
        QtdOperadores=('QtdOperadores', 'sum')).reset_index()
        consulta =pd.merge(consulta, consulta2, on = 'nomecategoria', how='left')


        consulta['Realizado%'] = (consulta['Realizado'] / consulta['MetaDia'])*100
        consulta['Realizado%'] = consulta['Realizado%'].round(1)

        return consulta
    

    def CalcularTempo(self,InicioOperacao, FimOperacao, tempoInicio, tempoFim):
        # Converte as horas de início e fim em objetos datetime
        tempoInicio = datetime.strptime(tempoInicio, "%H:%M:%S")
        tempoFim = datetime.strptime(tempoFim, "%H:%M:%S")
        # Gera uma sequência de datas entre as duas datas
        datas = pd.date_range(start=InicioOperacao, end=FimOperacao)

        # Verifica se algum domingo está presente
        tem_domingo = any(datas.weekday == 6)

        delta_dias = (FimOperacao - InicioOperacao).days


        if InicioOperacao == FimOperacao:
            # Calcular a diferença entre os horários
            delta = tempoFim - tempoInicio
            return delta.total_seconds() / 60
    
        elif delta_dias == 1:
            tempoFImEscala = "17:30:00"
            tempoInicioEscala = "07:30:00"
            tempoFImEscala = datetime.strptime(tempoFImEscala, "%H:%M:%S")
            tempoInicioEscala = datetime.strptime(tempoInicioEscala, "%H:%M:%S")

            delta1 = tempoFImEscala - tempoInicio
            delta2 = tempoFim - tempoFImEscala
        
            delta = delta1.total_seconds() + delta2.total_seconds()
        
            return delta / 60
        elif delta_dias == 3 and tem_domingo ==True:
            
            tempoFImEscala = "16:20:00"
            tempoInicioEscala = "07:30:00"
            tempoFImEscala = datetime.strptime(tempoFImEscala, "%H:%M:%S")
            tempoInicioEscala = datetime.strptime(tempoInicioEscala, "%H:%M:%S")

            delta1 = tempoFImEscala - tempoInicio
            delta2 = tempoFim - tempoFImEscala
        
            delta = delta1.total_seconds() + delta2.total_seconds()
        
            return delta / 60


        else:
            # Se as datas forem diferentes, considera-se uma diferença de 24h para simplificar
        
            return '-'
    def ProdutividadeOperadores(self):

        sql = """
        SELECT
            "Codigo Registro",
            "Data",
            "DiaInicial",
            "Hr Inicio",
            "Hr Final",
            "codOperador",
            "nomeOperador",
            "nomeOperacao",
            "paradas min",
            "tempoTotal(min)",
            "qtdPcs",
            "Meta(pcs/hr)"
        FROM
            "Easy"."ColetasProducao" cp
        WHERE
            "Data" >= %s AND "Data" <= %s
        ORDER BY
            "Data", "codOperador", "Codigo Registro"
        """

        conn = ConexaoPostgreMPL.conexaoJohn()
        produtividade = pd.read_sql(sql, conn, params=(self.dataInicio, self.dataFinal))

        if produtividade.empty:
            return pd.DataFrame([])

        else:
        # Adicionar uma coluna calculada usando a função CalcularTempo
            produtividade['TempoRealizado(Min)'] = produtividade.apply(
            lambda row: self.CalcularTempo(
            row['DiaInicial'], row['Data'], row['Hr Inicio'], row['Hr Final']
        ), axis=1)

        return produtividade





