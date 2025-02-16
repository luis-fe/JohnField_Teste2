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
                AND "dataHoraApontamento" <= %s;
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

                        
            if self.dataUltimoApontamento_A_M_D == self.dataApontamento and delta1<=delta2 :
                '''Aqui é feito um if para verificar se o apontamento ocorreu nos ultimos n minutos'''
                dataTarget = self.subtrair_minutos()     
             
                consulta = pd.read_sql(sql, conn, params=(self.codOperador,dataTarget))
                
                self.ultimoTempo = str(consulta['utimoTempo'][0])
                self.dataUltimoApontamento = consulta['utimaData'][0]
                self.validador = str(delta1) +'|'+ str(delta2)+'|'+ str(dataTarget)
        else:
            self.ultimoTempo = '-'
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
                "dataHoraApontamento","ultimoTempo","dataUltimoApontamento", validador
            )
            values
            ( %s, %s, %s, %s, %s, %s, %s, %s )
        """


        with ConexaoPostgreMPL.conexaoJohn() as conn:
            with conn.cursor() as curr:
                
                curr.execute(insert,
                            (
                    self.codOperador, self.codOperacao, self.qtdePc, self.dataApontamento,
                    self.dataHoraApontamento, self.ultimoTempo, self.dataUltimoApontamento,
                    self.validador)
                            )
                conn.commit()

        return pd.DataFrame([{'status':True,"mensagem":"Registrado com sucesso !"}])
    
    def contar_finais_de_semana(self) -> int:
        data_inicio = datetime.strptime(self.dataUltimoApontamento_A_M_D, "%Y-%m-%d").date()
        data_fim = datetime.strptime(self.dataApontamento, "%Y-%m-%d").date()
        
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