from encodings.punycode import T
from sqlite3 import Connection
import ConexaoPostgreMPL
import pandas as pd
from datetime import datetime, timedelta
import pytz
import numpy as np
from routes.Dashboard import Feriados


"""
REGRAS DO APONTAMENTO:
ok! 1) No ato do registro é considerado como intervalo de producao a dataAtual - dataUltimoRegistro 
ok! 2) Caso o usuario registrar operacoes em ate 10 min de diferenca da ultima, será contabilizado o intervalo da penultimaOperacao
OK! 3) Caso o sistema nao encontre operacao anterior ( "primeira vez"), considera o tempo de entrada na "escala" do colaborador
ok! 4) O sistema sempre desconta os domingos e sabados ( se o ultimo apontamento for sexta )
ok! 5) O sistema sempre desconta os domingo ( se o ultimo apontamento for sabado )
6) O sistema desconta os feriados Nacionais (caso tenha feriado local, o usuario deve informar)
7) O sistema nao considera apontamentos maiores que 3 dias, nesse caso é informado o horario da entrada 
"""



class ColetaProdutividade():
    '''Classe criada para a gestao da coleta de produtividade'''

    def __init__(self, codOperador = None, limiteTempoMinApontamento = None, 
                 nomeOperacao = None, qtdePc = None, dataInicio = None, dataFinal = None,
                 dataHoraOpcional = None):
        
        self.codOperador = str(codOperador)
        self.limiteTempoMinApontamento = limiteTempoMinApontamento
        self.nomeOperacao = str(nomeOperacao)
        self.qtdePc = qtdePc
        self.horarioInicial = '-'
        self.tempoRealizado = 0
        self.delta_dias = 0
        self.desconto = 0
        self.dataInicio = dataInicio
        self.dataFinal = dataFinal
        self.dataHoraOpcional = dataHoraOpcional

        #2 - buscar a DataHora atual do sistema
        self.dataHoraAtual()
        self.escalaInicial_Trabalho()


    def apontarProdutividade(self):
        '''Metodo utilizado para apontar a producao'''

        #1 - Buscar a data e hora anterior
        self._buscarUltimoApontamentoOperador()

        #2 - Registrar a produtiviade
        registro = self.inserirProdutividade()

        return registro



    def _buscarUltimoApontamentoOperador(self):
        '''Método que busca a ultima coleta realizado para o operador especificado '''


        sql = """
            SELECT 
                (MAX("dataHoraApontamento"::varchar))::time AS "utimoTempo", 
                COUNT("dataHoraApontamento") AS registros ,
                MAX("dataHoraApontamento"::varchar) AS "utimaData",
                MAX(("dataHoraApontamento"::date)::varchar) AS "dataUltimoApontamento"
            FROM 
                "Easy"."FolhaRegistro" rp 
            WHERE 
                "codOperador" = %s
                AND "dataHoraApontamento" < %s;
            """
        
        conn = ConexaoPostgreMPL.conexaoJohn()
        consulta = pd.read_sql(sql, conn, params=(self.codOperador,self.dataHoraApontamento))

        if consulta['registros'][0]>0:
            
            self.ultimoTempo = str(consulta['utimoTempo'][0])
            self.dataUltimoApontamento = consulta['utimaData'][0]
            
            if self.dataUltimoApontamento == None:
                self.dataUltimoApontamento = self.horarioManha

            self.dataUltimoApontamento_A_M_D = consulta['dataUltimoApontamento'][0]
            self.dataUltimoApontamento_tempo = datetime.strptime(self.dataUltimoApontamento, 
                                                                 "%Y-%m-%d %H:%M:%S")
            self.dataHoraApontamento_tempo = datetime.strptime(self.dataHoraApontamento, 
                                                                 "%Y-%m-%d %H:%M:%S")

            
            delta1 = (self.dataHoraApontamento_tempo - self.dataUltimoApontamento_tempo).total_seconds()
            limite_hms = datetime.strptime(self.limiteTempoMinApontamento, "%H:%M:%S")
            delta2 = timedelta(hours=limite_hms.hour, minutes=limite_hms.minute, seconds=limite_hms.second).total_seconds()
            # Formatando a saída
            self.validador = f"{delta1}|{delta2}|{self.dataUltimoApontamento_A_M_D}||{self.dataApontamento}"

            
            
            self.dataHoraApontamento__Dia = datetime.strptime(self.dataApontamento, 
                                                                 "%Y-%m-%d")
            self.dataUltimoApontamento_Dia = datetime.strptime(self.dataUltimoApontamento_A_M_D, 
                                                                 "%Y-%m-%d")
            
            self.delta_dias = (self.dataHoraApontamento__Dia - self.dataUltimoApontamento_Dia).days

            self.horarioInicial = self.ultimoTempo
            self.ultimoTempo_tempo = datetime.strptime(self.ultimoTempo, 
                                                                 "%H:%M:%S")
            self.tempoApontamento_tempo = datetime.strptime(self.tempoApontamento, 
                                                                 "%H:%M:%S")
            self.contar_finais_de_semana()

            # Verifica se o ultimo horario foi no mesmo dia 
            self.__obtendoTempoRealizado()
            




            if self.dataUltimoApontamento_A_M_D == self.dataApontamento and delta1<=delta2 :
                '''Aqui é feito um if para verificar se o apontamento ocorreu nos ultimos n minutos'''
                dataTarget = self.subtrair_minutos()     
             
                consulta = pd.read_sql(sql, conn, params=(self.codOperador,dataTarget))
                
                self.ultimoTempo = str(consulta['utimoTempo'][0])
                if self.ultimoTempo == 'None':
                    self.ultimoTempo = self.horarioManha    

                self.dataUltimoApontamento = consulta['utimaData'][0]
                if self.dataUltimoApontamento == None:
                    self.dataUltimoApontamento = self.dataApontamento+' '+self.horarioManha    
                self.dataUltimoApontamento_A_M_D = consulta['dataUltimoApontamento'][0]
                if self.dataUltimoApontamento_A_M_D == None:
                    self.dataUltimoApontamento_A_M_D = self.dataApontamento

                self.validador = str(delta1) +'|'+ str(delta2)+'|'+ str(dataTarget)
                # Verifica se o ultimo horario foi no mesmo dia 
                self.horarioInicial = self.ultimoTempo
                self.ultimoTempo_tempo = datetime.strptime(self.ultimoTempo, 
                                                                 "%H:%M:%S")
                self.tempoApontamento_tempo = datetime.strptime(self.tempoApontamento, 
                                                                 "%H:%M:%S")
                self.contar_finais_de_semana()

                # Verifica se o ultimo horario foi no mesmo dia 
                self.__obtendoTempoRealizado()


        else:
            self.ultimoTempo = self.horarioManha
            self.horarioInicial = self.ultimoTempo 
            self.dataUltimoApontamento = self.dataApontamento+' '+self.horarioManha
            self.validador = '-'
            self.dataUltimoApontamento_A_M_D = self.dataApontamento
            self.ultimoTempo_tempo = datetime.strptime(self.ultimoTempo, 
                                                                 "%H:%M:%S")
            
            self.tempoApontamento_tempo = datetime.strptime(self.tempoApontamento, 
                                                                 "%H:%M:%S")
            self.__obtendoTempoRealizado()


    
    def dataHoraAtual(self):
        '''Busca a data e hora atual do Sistema'''

        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
        data_str = agora.strftime('%Y-%m-%d')
        tempo_str = agora.strftime('%H:%M:%S')

        if self.dataHoraOpcional == None:

            self.dataHoraApontamento = hora_str
            self.dataApontamento = data_str
            self.tempoApontamento = tempo_str
        else:
            self.dataHoraOpcional = datetime.strptime(self.dataHoraApontamento, 
                                                                 "%Y-%m-%d %H:%M:%S")
            hora_str = self.dataHoraOpcional.strftime('%Y-%m-%d %H:%M:%S')
            data_str = self.dataHoraOpcional.strftime('%Y-%m-%d')
            tempo_str = self.dataHoraOpcional.strftime('%H:%M:%S')
            
            self.dataHoraApontamento = hora_str
            self.dataApontamento = data_str
            self.tempoApontamento = tempo_str

        

    def _conversaoDeStr_To_time(self, tempo):
            # Converte as datas de início e fim em objetos datetime se forem strings
        if isinstance(tempo, str):
            tempo = datetime.strptime(tempo, "%Y-%m-%d %H:%M:%S")

        return tempo
    
    def _conversaoDeTime_To_Str(tempo):
        if isinstance(tempo, datetime):
            return tempo.strftime("%Y-%m-%d %H:%M:%S")  # Converte datetime para string no formato desejado
        return tempo  # Retorna o valor original caso não seja um datetime
    
    def inserirProdutividade(self):
        '''Metodo para inserir no Banco de Dados a produtividade'''

        insert = """
        insert into 
            "Easy"."FolhaRegistro" 
            (
                "codOperador", "nomeOperacao" , "qtdePcs" , "dataApontamento",
                "dataHoraApontamento","ultimoTempo","dataUltimoApontamento", validador,
                "descontoFimSemana", "horarioInicial","tempoRealizado","delta_dias", tempo_desconto
            )
            values
            ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
        """

        
        with ConexaoPostgreMPL.conexaoJohn() as conn:
            try:
                fimSemana = self.contar_finais_de_semana()
            except:
                fimSemana = 0

            with conn.cursor() as curr:
                
                curr.execute(insert,
                            (
                    self.codOperador, self.nomeOperacao, self.qtdePc, self.dataApontamento,
                    self.dataHoraApontamento, self.ultimoTempo, self.dataUltimoApontamento,
                    self.validador, fimSemana, self.horarioInicial, 
                    self.tempoRealizado,self.delta_dias, self.desconto)
                            )
                conn.commit()

        return pd.DataFrame([{'status':True,"mensagem":"Registrado com sucesso !"}])
    
    def contar_finais_de_semana(self) -> int:
        data_inicio = datetime.strptime(self.dataUltimoApontamento_A_M_D, "%Y-%m-%d").date()
        data_fim = datetime.strptime(self.dataApontamento, "%Y-%m-%d").date()
        
        # Verifica se há algum domingo na sequência de datas
        try:
            datas = pd.date_range(start=self.dataUltimoApontamento_A_M_D, end=self.dataApontamento)
        except ValueError as e:
            raise ValueError(f"Erro ao gerar o range de datas: {e},inico operacao{self.dataUltimoApontamento_A_M_D},fim{self.dataApontamento}")

        self.tem_domingo = any(date.weekday() == 6 for date in datas)


        if data_fim.weekday() in [5, 6]:  # 5 = Sábado, 6 = Domingo
            data_fim -= timedelta(days=1)  # Remove um dia para desconsiderar a saída
        
        contador = 0
        data_atual = data_inicio
        
        while data_atual <= data_fim:
            if data_atual.weekday() in [5, 6]:  # 5 = Sábado, 6 = Domingo
                contador += 1
            data_atual += timedelta(days=1)
        
        return contador
    
    def subtrair_minutos(self) -> str:
        dt = datetime.strptime(self.dataHoraApontamento, "%Y-%m-%d %H:%M:%S")
        dt -= timedelta(minutes=10)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    

    def escalaInicial_Trabalho(self):
        '''metodo que encontra a escala inicial de trabalho'''
        sql = """
        select
            periodo1_inicio,
            periodo1_fim,
            periodo2_inicio,
            periodo2_fim
        from
            "Easy"."EscalaTrabalho" et 
            """
        
        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql, conn)

        self.horarioManha = consulta['periodo1_inicio'][0]
        self.horarioTarde = consulta['periodo2_inicio'][0]
        self.horarioTardeFinal = consulta['periodo2_fim'][0]

        self.horarioManhaFim = consulta['periodo1_fim'][0]
        self.horarioTarde_tempo = datetime.strptime(self.horarioTarde, 
                                                                 "%H:%M:%S")
        self.horarioManhaFim_tempo = datetime.strptime(self.horarioManhaFim, 
                                                                 "%H:%M:%S")
        self.descontoAlmoco = (self.horarioManhaFim_tempo-self.horarioTarde_tempo).total_seconds()


        

    def __obtendoTempoRealizado(self):
        if self.dataUltimoApontamento_A_M_D == self.dataApontamento:

                if self.ultimoTempo_tempo < self.horarioTarde_tempo and self.tempoApontamento_tempo>self.horarioTarde_tempo:
                    self.desconto = self.descontoAlmoco
                else:
                    self.desconto = 0

                self.tempoRealizado = (self.tempoApontamento_tempo-self.ultimoTempo_tempo).total_seconds()
                self.tempoRealizado = round((self.tempoRealizado+self.desconto)/60,3)
            
        elif self.delta_dias == 1:
                tempoFImEscala = "17:20:00"
                tempoInicioEscala = "07:10:00"
                tempoFImEscala = datetime.strptime(tempoFImEscala, "%H:%M:%S")
                tempoInicioEscala = datetime.strptime(tempoInicioEscala, "%H:%M:%S")
                
                delta1 = tempoFImEscala - self.ultimoTempo_tempo
                delta2 = self.tempoApontamento_tempo - tempoInicioEscala
        
                self.tempoRealizado  = delta1.total_seconds() + delta2.total_seconds() 

                if self.ultimoTempo_tempo < self.horarioTarde_tempo:
                    self.desconto = self.descontoAlmoco
                
                if self.tempoApontamento_tempo > self.horarioTarde_tempo:
                    self.desconto = self.descontoAlmoco + self.desconto


                self.tempoRealizado = round((self.tempoRealizado+self.desconto)/60,3)
                
        elif (self.delta_dias == 3 or self.delta_dias == 2 ) and self.tem_domingo:
                tempoFImEscala = "17:20:00"
                tempoInicioEscala = "07:10:00"
                tempoFImEscala = datetime.strptime(tempoFImEscala, "%H:%M:%S")
                tempoInicioEscala = datetime.strptime(tempoInicioEscala, "%H:%M:%S")
                
                delta1 = tempoFImEscala - self.ultimoTempo_tempo
                delta2 = self.tempoApontamento_tempo - tempoInicioEscala
        
                self.tempoRealizado  = delta1.total_seconds() + delta2.total_seconds() 

                if self.ultimoTempo_tempo < self.horarioTarde_tempo:
                    self.desconto = self.descontoAlmoco
                
                if self.tempoApontamento_tempo > self.horarioTarde_tempo:
                    self.desconto = self.descontoAlmoco + self.desconto

                self.validador = f'caso de sexta que aponta seg{str(delta2)}'
                self.tempoRealizado = round((self.tempoRealizado+self.desconto)/60,3)

        elif self.delta_dias == 2:
                tempoFImEscala = "17:20:00"
                tempoInicioEscala = "07:10:00"
                tempoFImEscala = datetime.strptime(tempoFImEscala, "%H:%M:%S")
                tempoInicioEscala = datetime.strptime(tempoInicioEscala, "%H:%M:%S")
                
                delta1 = tempoFImEscala - self.ultimoTempo_tempo
                delta2 = self.tempoApontamento_tempo - tempoInicioEscala
        
                self.tempoRealizado  = delta1.total_seconds() + delta2.total_seconds() 

                if self.ultimoTempo_tempo < self.horarioTarde_tempo:
                    self.desconto = self.descontoAlmoco
                
                if self.tempoApontamento_tempo > self.horarioTarde_tempo:
                    self.desconto = self.descontoAlmoco + self.desconto


                self.tempoRealizado = round((self.tempoRealizado+self.desconto)/60,3)
                self.tempoRealizado = self.tempoRealizado +510
        else:

                self.tempoRealizado = (self.tempoApontamento_tempo-self.ultimoTempo_tempo).total_seconds()
                self.tempoRealizado = round((self.tempoRealizado+self.desconto)/60,3)

    
    def dashboardProdutividade(self):

        sql = """
             select
	            "dataHoraApontamento" ,
	            "dataUltimoApontamento",
                "dataApontamento" as "Data",
	            "codOperador" ,
	            "nomeOperacao" ,
	            "qtdePcs"::dec, 
	            "tempoRealizado"::dec 
            from
	            "Easy"."FolhaRegistro" fr
            where 
	            fr."dataApontamento" >= %s
	            and fr."dataApontamento" <= %s
            """
        
        sql2 = """
            select
	            o."nomeOperacao" ,
	            "tempoPadrao" as "tempoPadrao(s)"
            from
	            "Easy"."TemposOperacao" to2
            inner join 
                "Easy"."Operacao" o on
	            o."codOperacao" = to2."codOperacao" 
            """

        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.dataInicio, self.dataFinal))

        consulta2 = pd.read_sql(sql2, conn)

        consulta = pd.merge(consulta, consulta2 , on='nomeOperacao',how='left')
        consulta['tempoPadrao(min)'] =(consulta['tempoPadrao(s)']*consulta['qtdePcs'])/60
        consulta['chave'] = consulta['codOperador']+'||'+consulta['dataUltimoApontamento']
        # Agrupando os dados pela coluna 'chave'
        consultaGroupBy = consulta.groupby("chave").agg({
            "nomeOperacao": lambda x: "/".join(sorted(set(x))),  # Concatena operações únicas
            "codOperador":"first",
            "tempoPadrao(min)": "sum",  # Soma os tempos
            "qtdePcs": "max",  # Obtém o máximo de qtdPeças
            "tempoRealizado":"first",
            "Data":"first"
        }).reset_index()
        consultaGroupBy['contagem'] = 1
        consultaGroupBy['contagem'] = consultaGroupBy.groupby(['Data', 'codOperador'])[
                'contagem'].cumsum()
        consultaGroupBy['tempoTotal(min)Acum'] = consultaGroupBy.groupby(['Data', 'codOperador'])[
                'tempoRealizado'].cumsum()
        consultaGroupBy['tempo PrevistoAcum'] = consultaGroupBy.groupby(['Data', 'codOperador'])[
                'tempoPadrao(min)'].cumsum()
        consultaGroupBy['tempo PrevistoAcum'] = consultaGroupBy['tempo PrevistoAcum'].round(2)
        consultaGroupBy['qtdPcsAcum'] = consultaGroupBy.groupby(['Data', 'codOperador'])['qtdePcs'].cumsum()


        consulta2 = consultaGroupBy.groupby(['Data', 'codOperador']).agg({
                "contagem": 'max'}).reset_index()

        consulta2 = pd.merge(consulta2, consultaGroupBy, on=['Data', 'codOperador', 'contagem'])
        consulta2 = consulta2.drop_duplicates()


        consulta2['Eficiencia'] = round(consulta2['tempo PrevistoAcum'] / consulta2['tempoTotal(min)Acum'], 3) * 100
        consulta2['Eficiencia'] = consulta2['Eficiencia'].round(1)

        consulta3 = consulta2.groupby('codOperador').agg({
               # 'nomeOperador': 'first',
                'qtdPcsAcum': 'sum',
                'tempo PrevistoAcum': 'sum',
                'tempoTotal(min)Acum': 'sum'
            }).reset_index()
        
        consulta3['tempoTotal(min)Acum'] = consulta3['tempoTotal(min)Acum'].round(4)
        consulta3['Eficiencia'] = round(consulta3['tempo PrevistoAcum'] / consulta3['tempoTotal(min)Acum'], 3) * 100
        consulta3['Eficiencia'] = consulta3['Eficiencia'].round(1)

        consulta3 = consulta3.sort_values(by=['Eficiencia'], ascending=False)
        consulta3['Eficiencia'] = consulta3['Eficiencia'].astype(str) + '%'

        efiMedia = round(consulta3['tempo PrevistoAcum'].sum() / consulta3['tempoTotal(min)Acum'].sum(), 3) * 100

        dados = {
                '0-Eficiencia Média Periodo': f'{efiMedia}%',
                '1-Detalhamento': consulta3.to_dict(orient='records')}

        return pd.DataFrame([dados])
    
    def analisePeriodo(self):
        '''Metodo para analisar a produtividade no periodo'''

        sql = """
                select
                    *
                from
                    "Easy".feriados f
                where
                    f."data" >= %s
                    and 
                    f."data" <= %s
            """
        
        sqlEscala = """
        
        SELECT 
            EXTRACT(EPOCH FROM 
                (periodo1_fim::time - periodo1_inicio::time) 
                + (periodo2_fim::time - periodo2_inicio::time)
            ) / 60 AS tempo_em_minutos
        FROM "Easy"."EscalaTrabalho" et;
        
        """
        conn = ConexaoPostgreMPL.conexaoEngine()
        feriados = pd.read_sql(sql, conn, params=(self.dataInicio, self.dataFinal) )
        escala = pd.read_sql(sqlEscala, conn )
        
        if feriados.empty:
            descontoFeriado = 0
        else:
            # Convertendo a coluna "data" para datetime
            feriados["data"] = pd.to_datetime(feriados["data"])
            # Criando a coluna do dia da semana (ajustando para que domingo = 1, segunda = 2, ..., sábado = 7)
            feriados["dia_semana"] = feriados["data"].dt.weekday + 1  # Como segunda é 0, somamos 1 para ajustar
            feriado = feriados[feriados['dia_semana']!=0]
            feriado = feriado[feriado['dia_semana']!=7]
            descontoFeriado = feriado['dia_semana'].count()

        # Converter as datas para formato datetime
        data_inicial = pd.to_datetime(self.dataInicio)
        data_final = pd.to_datetime(self.dataFinal)
        diasUteis = np.busday_count(data_inicial.date(), (data_final.date() + timedelta(days=1))) - descontoFeriado
        
        tempoTrabalho = escala['tempo_em_minutos'][0]

        sqlApontamentosOperadores ="""
        select
	        fr."codOperador" ,
            (select o."nomeOperador"  from "Easy"."Operador" o where o."codOperador"::varchar = fr."codOperador") as "nomeOperador",
	        fr."nomeOperacao",
	        "dataApontamento",
	        "dataUltimoApontamento",
	        "qtdePcs"::dec, 
        case 
            when delta_dias::int >3 then 0 
            when "dataUltimoApontamento"::date > %s then 0
            else 
            extract(EPOCH FROM ('17:30:00' - "dataUltimoApontamento"::time))::int/60 end as "tempoAnterior" ,
        delta_dias 
        from
            "Easy"."FolhaRegistro" fr
        where
            fr."dataApontamento" >= %s
            and 
            fr."dataApontamento" <= %s
        """

        sql2 = """
            select
	            o."nomeOperacao" ,
	            "tempoPadrao" as "tempoPadrao(s)"
            from
	            "Easy"."TemposOperacao" to2
            inner join 
                "Easy"."Operacao" o on
	            o."codOperacao" = to2."codOperacao" 
            """
        consulta2 = pd.read_sql(sql2, conn)
        ApontamentosOperadores = pd.read_sql(sqlApontamentosOperadores, conn, params=(self.dataInicio, self.dataInicio, self.dataFinal) )
        
        
        ApontamentosOperadores = pd.merge(ApontamentosOperadores, consulta2 , on='nomeOperacao',how='left')
        ApontamentosOperadores['tempoPadrao(min)'] =(ApontamentosOperadores['tempoPadrao(s)']*ApontamentosOperadores['qtdePcs'])/60


        ApontamentosOperadoresGroupBy = ApontamentosOperadores.groupby("codOperador").agg({
            "tempoAnterior":"max",
            "tempoPadrao(min)":"sum",
            "nomeOperador":"first",
            "qtdePcs":"sum"
        }).reset_index()

        ApontamentosOperadoresGroupBy['0-Feriados Periodo'] = descontoFeriado
        ApontamentosOperadoresGroupBy['diasUteis'] = diasUteis
        ApontamentosOperadoresGroupBy['tempoTrabalho'] = diasUteis * tempoTrabalho
        ApontamentosOperadoresGroupBy['dataHora'] = self.tempoApontamento 
        ApontamentosOperadoresGroupBy['horarioTardeFinal'] = self.horarioTardeFinal 




        # Convertendo para datetime
        ApontamentosOperadoresGroupBy["Hora"] = pd.to_datetime(ApontamentosOperadoresGroupBy["dataHora"], format="%H:%M:%S").dt.time
        ApontamentosOperadoresGroupBy["horaFinal"] = pd.to_datetime(ApontamentosOperadoresGroupBy["horarioTardeFinal"], format="%H:%M:%S").dt.time

        # Função para calcular os minutos
        def calcular_minutos(row):
            if row["Hora"] > row["horaFinal"]:
                return 0
            else:
                hora_inicial = pd.Timestamp.combine(pd.Timestamp.today(), row["Hora"])
                hora_final = pd.Timestamp.combine(pd.Timestamp.today(), row["horaFinal"])
                return int((hora_final - hora_inicial).total_seconds() // 60)
            
        ApontamentosOperadoresGroupBy["minutosDescontados"] = ApontamentosOperadoresGroupBy.apply(calcular_minutos, axis=1)

        ApontamentosOperadoresGroupBy.drop(['Hora', 'horaFinal'], axis=1, inplace=True)
        ApontamentosOperadoresGroupBy['tempoRealizado']= ApontamentosOperadoresGroupBy['tempoTrabalho']+ApontamentosOperadoresGroupBy['tempoAnterior']-ApontamentosOperadoresGroupBy["minutosDescontados"]
        ApontamentosOperadoresGroupBy['Eficiencia'] = round(ApontamentosOperadoresGroupBy['tempoPadrao(min)'] / ApontamentosOperadoresGroupBy['tempoRealizado'], 3) * 100
        ApontamentosOperadoresGroupBy['Eficiencia'] = ApontamentosOperadoresGroupBy['Eficiencia'].round(1)
        ApontamentosOperadoresGroupBy.rename(
            columns={'qtdePcs': 'qtdPcsAcum',
                     "tempoPadrao(min)":"tempo PrevistoAcum",
                     "tempoRealizado":"tempoTotal(min)Acum"},
            inplace=True)
        
        eficienciaGlobal = ApontamentosOperadoresGroupBy['tempo PrevistoAcum'].sum()/ApontamentosOperadoresGroupBy['tempoTotal(min)Acum'].sum()
        eficienciaGlobal = round(eficienciaGlobal * 100 ,2)
        ApontamentosOperadoresGroupBy = ApontamentosOperadoresGroupBy.sort_values(by=['Eficiencia'], ascending=False)

        
        dados = {
                '0-Eficiencia Média Periodo':f'{eficienciaGlobal}%',
                '1-Detalhamento':ApontamentosOperadoresGroupBy.to_dict(orient='records') 
            }

        return pd.DataFrame([dados])


    