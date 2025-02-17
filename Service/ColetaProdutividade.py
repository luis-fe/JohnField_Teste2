from sqlite3 import Connection
import ConexaoPostgreMPL
import pandas as pd
from datetime import datetime, timedelta
import pytz


class ColetaProdutividade():
    '''Classe criada para a gestao da coleta de produtividade'''

    def __init__(self, codOperador = None, limiteTempoMinApontamento = None, 
                 codOperacao = None, qtdePc = None):
        
        self.codOperador = str(codOperador)
        self.limiteTempoMinApontamento = limiteTempoMinApontamento
        self.codOperacao = str(codOperacao)
        self.qtdePc = qtdePc
        self.horarioInicial = '-'
        self.tempoRealizado = 0
        self.delta_dias = 1000

        #2 - buscar a DataHora atual do sistema
        self.dataHoraAtual()


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
                MAX("dataHoraApontamento"::time) AS "utimoTempo", 
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

        if not consulta.empty:
            
            self.ultimoTempo = str(consulta['utimoTempo'][0])
            self.dataUltimoApontamento = consulta['utimaData'][0]
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

            self.delta_dias = (self.dataHoraApontamento_tempo - self.dataUltimoApontamento_tempo).days

            self.horarioInicial = self.ultimoTempo
            self.ultimoTempo_tempo = datetime.strptime(self.ultimoTempo, 
                                                                 "%H:%M:%S")
            self.tempoApontamento_tempo = datetime.strptime(self.tempoApontamento, 
                                                                 "%H:%M:%S")
            self.contar_finais_de_semana()

            # Verifica se o ultimo horario foi no mesmo dia 
            self.__obtendoTempoRealizado
            




            if self.dataUltimoApontamento_A_M_D == self.dataApontamento and delta1<=delta2 :
                '''Aqui é feito um if para verificar se o apontamento ocorreu nos ultimos n minutos'''
                dataTarget = self.subtrair_minutos()     
             
                consulta = pd.read_sql(sql, conn, params=(self.codOperador,dataTarget))
                
                self.ultimoTempo = str(consulta['utimoTempo'][0])
                self.dataUltimoApontamento = consulta['utimaData'][0]
                self.dataUltimoApontamento_A_M_D = consulta['dataUltimoApontamento'][0]

                self.validador = str(delta1) +'|'+ str(delta2)+'|'+ str(dataTarget)
                # Verifica se o ultimo horario foi no mesmo dia 
                self.horarioInicial = self.ultimoTempo
                self.ultimoTempo_tempo = datetime.strptime(self.ultimoTempo, 
                                                                 "%H:%M:%S")
                self.tempoApontamento_tempo = datetime.strptime(self.tempoApontamento, 
                                                                 "%H:%M:%S")
                self.contar_finais_de_semana()

                # Verifica se o ultimo horario foi no mesmo dia 
                self.__obtendoTempoRealizado


        else:
            self.ultimoTempo = self.horarioManha
            self.dataUltimoApontamento = None
            self.validador = '-'


    def dataHoraAtual(self):
        '''Busca a data e hora atual do Sistema'''

        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
        data_str = agora.strftime('%Y-%m-%d')
        tempo_str = agora.strftime('%H:%M:%S')

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
                "codOperador", "codOperacao" , "qtdePcs" , "dataApontamento",
                "dataHoraApontamento","ultimoTempo","dataUltimoApontamento", validador,
                "descontoFimSemana", "horarioInicial","tempoRealizado","delta_dias"
            )
            values
            ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
        """

        
        with ConexaoPostgreMPL.conexaoJohn() as conn:
            with conn.cursor() as curr:
                
                curr.execute(insert,
                            (
                    self.codOperador, self.codOperacao, self.qtdePc, self.dataApontamento,
                    self.dataHoraApontamento, self.ultimoTempo, self.dataUltimoApontamento,
                    self.validador, self.contar_finais_de_semana(), self.horarioInicial, 
                    self.tempoRealizado,self.delta_dias)
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
            periodo2_inicio
        from
            "Easy"."EscalaTrabalho" et 
            """
        
        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql, conn)

        self.horarioManha = consulta['periodo1_inicio'][0]
        self.horarioTarde = consulta['periodo2_inicio'][0]

        

    def __obtendoTempoRealizado(self):
        if self.dataUltimoApontamento_A_M_D == self.dataApontamento:


                self.tempoRealizado = (self.tempoApontamento_tempo-self.ultimoTempo_tempo).total_seconds()
                self.tempoRealizado = round(self.tempoRealizado/60,3)
            
        elif self.delta_dias == 1:
                tempoFImEscala = "17:20:00"
                tempoInicioEscala = "07:10:00"
                tempoFImEscala = datetime.strptime(tempoFImEscala, "%H:%M:%S")
                tempoInicioEscala = datetime.strptime(tempoInicioEscala, "%H:%M:%S")
                
                delta1 = tempoFImEscala - self.ultimoTempo_tempo
                delta2 = self.tempoApontamento_tempo - tempoInicioEscala
        
                self.tempoRealizado  = delta1.total_seconds() + delta2.total_seconds() 
                self.tempoRealizado = round(self.tempoRealizado/60,3)
                
        elif (self.delta_dias == 3 or self.delta_dias == 2 ) and self.tem_domingo:
                tempoFImEscala = "16:20:00"
                tempoInicioEscala = "07:10:00"
                tempoFImEscala = datetime.strptime(tempoFImEscala, "%H:%M:%S")
                tempoInicioEscala = datetime.strptime(tempoInicioEscala, "%H:%M:%S")
                
                delta1 = tempoFImEscala - self.ultimoTempo_tempo
                delta2 = self.tempoApontamento_tempo - tempoInicioEscala
        
                self.tempoRealizado  = delta1.total_seconds() + delta2.total_seconds() 
                self.tempoRealizado = round(self.tempoRealizado/60,3)
            