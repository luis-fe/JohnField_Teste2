from sqlite3 import Connection
import ConexaoPostgreMPL
import pandas as pd
from datetime import datetime
import pytz


class ColetaProdutividade():
    '''Classe criada para a gestao da coleta de produtividade'''

    def __init__(self, codOperador = None, limiteTempoMinApontamento = None, 
                 codOperacao = None, qtdePc = None):
        
        self.codOperador = codOperador
        self.limiteTempoMinApontamento = limiteTempoMinApontamento
        self.codOperacao = codOperacao
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
                MAX("DataHora"::time) AS "utimoTempo", 
                COUNT("DataHora") AS registros ,
                MAX("DataHora"::varchar) AS "utimaData",
                MAX(("DataHora"::date)::varchar) AS "dataApontamento"
            FROM 
                "Easy"."RegistroProducao" rp 
            WHERE 
                "codOperador" = %s
                AND (("DataHora"::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date 
                <= ( %s AT TIME ZONE 'America/Sao_Paulo')::date;
            """
        
        conn = ConexaoPostgreMPL.conexaoJohn()
        consulta = pd.read_sql(sql, conn, params=(self.codOperador,self.dataHoraApontamento))

        if not consulta.empty:
            self.ultimoTempo = consulta['utimoTempo'][0]
            self.dataUltimoApontamento = consulta['dataApontamento'][0]

            # 1.1: caso o intervalo entre apontamentos aconteca em n minutos, 
            # considerar como verdade a ocorrencia anterior

            self.tempoApontamento_Time = datetime.strptime(self.tempoApontamento, "%H:%M:%S")
            
            self.limiteTempoMinApontamento_Time =  self._conversaoDeStr_To_time(self.limiteTempoMinApontamento)
            delta = self.tempoApontamento_Time - (self.tempoApontamento_Time - self.limiteTempoMinApontamento_Time )
            delta1 = delta.total_seconds()
            delta2 = self.limiteTempoMinApontamento_Time.total_seconds()
            
            if self.dataHoraApontamento == self.dataApontamento and delta1<=delta2 :
                '''Aqui é feito um if para verificar se o apontamento ocorreu nos ultimos n minutos'''
                dataTarget = self._conversaoDeTime_To_Str(delta)
                consulta = pd.read_sql(sql, conn, params=(self.codOperador,dataTarget))
                
                self.ultimoTempo = consulta['utimoTempo'][0]
                self.dataUltimoApontamento = consulta['dataApontamento'][0]

        else:
            self.ultimoTempo = '-'
            self.dataUltimoApontamento = None


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
                "dataHoraApontamento","ultimoTempo","dataUltimoApontamento"
            )
            values
            ( %s, %s, %s, %s, %s, %s, %s )
        """


        with ConexaoPostgreMPL.conexaoJohn() as conn:
            with conn.cursor() as curr:
                
                curr.execute(insert,
                            (
                    self.codOperador, self.codOperacao, self.qtdePc, self.dataApontamento,
                    self.dataHoraApontamento, self.ultimoTempo, self.dataUltimoApontamento)
                            )
                conn.commit()

        return pd.DataFrame([{'status':True,"mensagem":"Registrado com sucesso !"}])