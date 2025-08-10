import ConexaoPostgreMPL
import pandas as pd



class Usuario_empresa():
    '''Classe relativa a relacao de usuario e empresa'''

    def __init__(self, codEmpresa = '1', codUsuario = '', arrayEmpresa = ''):

        self.codEmpresa = codEmpresa
        self.codUsuario = codUsuario
        self.arrayEmpresa = arrayEmpresa


    def inserir_empresa_por_usuario(self):
        '''metodo que inseri as empresas autorizadas para o usuario '''

        insert = """
            insert into "easy"."UsuarioEmpresa" ("codEmpresa", "codUsuario") values ( %s, %s)
        """

        with ConexaoPostgreMPL.conexaoJohn() as conn:
            with  conn.cursor() as curr:

                curr.execute(insert, (self.codEmpresa, self.codUsuario))
                conn.commit()


    def inserir_array(self):

        

        for emp in self.arrayEmpresa:

            self.codEmpresa = emp

            verifica = self.consulta_empresa_usuario()

            if verifica.empty:

              self.inserir_empresa_por_usuario()


    def consulta_empresa_usuario(self):

        conn = ConexaoPostgreMPL.conexaoEngine()

        select = """
                    select * 
                    from 
                    "easy"."UsuarioEmpresa" where "codEmpresa" = %s  and "codUsuario" = %s
                """
        
        consulta = pd.read_sql(select, conn , params=(self.codEmpresa,))
        return consulta


    def exclusao_usuario_empresa(self):

        delete = """delete from "easy"."UsuarioEmpresa"
                    where "codEmpresa" = %s  and "codUsuario" = %s
        """

        with ConexaoPostgreMPL.conexaoJohn() as conn:
            with  conn.cursor() as curr:

                curr.execute(delete, (self.codEmpresa, self.codUsuario))
                conn.commit()


    def array_deletar_usuario_empresa(self):

        for emp in self.arrayEmpresa:
            self.codEmpresa = emp 
            self.exclusao_usuario_empresa()


    def consulta_usuarios_empresa(self):
            
        conn = ConexaoPostgreMPL.conexaoEngine()

        select = """
                    select * 
                    from 
                    "easy"."UsuarioEmpresa"
                """
        
        consulta = pd.read_sql(select, conn)
        return consulta
