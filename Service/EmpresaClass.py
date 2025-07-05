import ConexaoPostgreMPL
import pandas as pd


class Empresa():

    def __init__(self, codEmpresa='01', nomeEmpresa='', CNPJ=''):
        self.codEmpresa = codEmpresa
        self.nomeEmpresa = nomeEmpresa
        self.CNPJ = CNPJ

    def get_empresas(self):
        '''Método para obter as empresas cadastradas.
        
        Retorna:
            DataFrame: Um DataFrame contendo as empresas cadastradas.
        '''
        sql = """
            SELECT * FROM "Easy"."Empresa"
        """
        
        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql, conn)

        return consulta

    def __adicionar_empresa(self):
        '''Método para inserir uma nova empresa.
        
        Retorna:
            str: Mensagem de sucesso ao inserir a empresa.
        '''
        sql = """
            INSERT INTO "Easy"."Empresa" ("codEmpresa", "nomeEmpresa", "CNPJ") VALUES (%s, %s, %s)
        """
        
        try:
            with ConexaoPostgreMPL.conexaoJohn() as conn:
                with conn.cursor() as curr:
                    curr.execute(sql, (self.codEmpresa, self.nomeEmpresa, self.CNPJ))
                    conn.commit()
            return pd.DataFrame([{'Mensagem':'Empresa inserida com sucesso!'}])
        except Exception as e:
            
            return pd.DataFrame([{'Mensagem':f'Erro ao inserir empresa: {e}'}])

        

    def __pesquisar_EmpresaEspecifica(self):
        '''Metodo que pesquisa uma empresa especifica'''

        sql = """
            SELECT * FROM "Easy"."Empresa"
            where "codEmpresa" = %s
        """
        
        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.codEmpresa,))

        return consulta
    
    def inserir_atualizar_Empresa(self):
        '''Metodo que inseri ou atualiza a empresa '''

        # pesquisar se a empresa ja exite:
        verificar = self.__pesquisar_EmpresaEspecifica()
        
        if verificar.empty :
            
            self.__adicionar_empresa()
        else:
            self.__atualizar_Empresa()

    def __atualizar_Empresa(self):
        '''Atualizar empresa'''

        sql = """
                update "Easy"."Empresa"
                    set "nomeEmpresa" = %s, 
                        "CNPJ" = %s
                where 
                    "codEmpresa" = %s
        """
        
        try:
            with ConexaoPostgreMPL.conexaoJohn() as conn:
                with conn.cursor() as curr:
                    curr.execute(sql, (self.nomeEmpresa, self.CNPJ, self.codEmpresa, ))
                    conn.commit()
            return pd.DataFrame([{'Mensagem':'Empresa alterada com sucesso!'}])
        except Exception as e:
            return pd.DataFrame([{'Mensagem':f'Erro ao alterar empresa: {e}'}])

    def excluir_empresa(self):
        '''Método que realiza a exclusao da empresa'''

        sql = """
            DELETE 
                FROM "Easy"."Empresa"
            where 
                "codEmpresa" = %s
        """    
        
        try:
            with ConexaoPostgreMPL.conexaoJohn() as conn:
                with conn.cursor() as curr:
                    curr.execute(sql, (self.codEmpresa, ))
                    conn.commit()
            return pd.DataFrame([{'Mensagem':'Empresa excluida com sucesso!'}])
        except Exception as e:
            return pd.DataFrame([{'Mensagem':f'Erro ao exluir empresa: {e}'}])


