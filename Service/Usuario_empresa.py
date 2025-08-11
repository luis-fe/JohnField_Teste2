import ConexaoPostgreMPL
import pandas as pd
from Service import EmpresaClass


class Usuario_empresa():
    '''Classe relativa a relacao de usuario e empresa'''

    def __init__(self, codEmpresa = '1', codUsuario = '', arrayEmpresa = ''):

        self.codEmpresa = codEmpresa
        self.codUsuario = codUsuario
        self.arrayEmpresa = arrayEmpresa


    def inserir_empresa_por_usuario(self):
        '''metodo que inseri as empresas autorizadas para o usuario '''

        insert = """
            insert into "Easy"."UsuarioEmpresa" ("codEmpresa", "codUsuario") values ( %s, %s)
        """
        
        with ConexaoPostgreMPL.conexaoJohn() as conn:
            with  conn.cursor() as curr:
                
                curr.execute(insert, (self.codEmpresa, self.codUsuario))
                conn.commit()


    def inserir_array(self):
        mensagem = []
        for emp in self.arrayEmpresa:

            empresa = EmpresaClass.Empresa(emp)
            verifica_empresa = empresa.pesquisar_EmpresaEspecifica()

            verifica = self.consulta_empresa_usuario()


            if verifica.empty and not verifica_empresa.empty :
              mensagem.append(f'{emp} vinculada')
              self.inserir_empresa_por_usuario()
            
            elif verifica_empresa.empty:
                mensagem.append(f'{emp} nao existe')



        return pd.DataFrame([{'Status':True,'Mensagem':f'{mensagem}'}])


    def consulta_empresa_usuario(self):
        conn = ConexaoPostgreMPL.conexaoEngine()

        select = """
                    select * 
                    from 
                    "Easy"."UsuarioEmpresa" where "codEmpresa" = %s  and "codUsuario" = %s
                """
        
        consulta = pd.read_sql(select, conn , params=(self.codEmpresa,self.codUsuario))
        return consulta


    def exclusao_usuario_empresa(self):

        delete = """delete from "Easy"."UsuarioEmpresa"
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

        return pd.DataFrame([{'Status':True,'Mensagem':f'Empresas {self.arrayEmpresa} Desvinculadas ao usuario {self.codUsuario}'}])



    def consulta_usuarios_empresa(self):
            
        conn = ConexaoPostgreMPL.conexaoEngine()

        select = """
                    select * 
                    from 
                    "Easy"."UsuarioEmpresa"
                """
        
        consulta = pd.read_sql(select, conn)
        return consulta
