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

    def adicionar_empresa(self):
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
            return 'Empresa inserida com sucesso!'
        except Exception as e:
            return f'Erro ao inserir empresa: {e}'
        

    def pesquisar_EmpresaEspecifica(self):
        '''Metodo que pesquisa uma empresa especifica'''

        sql = """
            SELECT * FROM "Easy"."Empresa"
            where "codEmpresa" = %s
        """
        
        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.codEmpresa,))

        return consulta
