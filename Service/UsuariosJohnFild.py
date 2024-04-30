import pandas as pd
import ConexaoPostgreMPL


def ConsultaUsuarios():
    conn = ConexaoPostgreMPL.conexaoJohn()
    consulta = pd.read_sql("""
    select idusuario ,"nomeLogin" ,"nomeUsuario" , "Perfil"  from "Easy"."Usuario" u    
    """,conn)
    conn.close()

    return consulta

def NovoUsuario(idUsuario, nomeUsuario,login , Perfil, Senha):

    consulta = ConsultaUsuariosID(idUsuario)

    if consulta.empty:

        conn = ConexaoPostgreMPL.conexaoJohn()

        insert = """
        insert into "Easy"."Usuario" ( idusuario , "nomeUsuario" , "nomeLogin","Perfil" ,"Senha") values (%s , %s, %s ,%s, %s)
        """
        cursor = conn.cursor()
        cursor.execute(insert,(idUsuario, nomeUsuario, login, Perfil, Senha))
        conn.commit()
        cursor.close()

        conn.close()

        return pd.DataFrame([{'Mensagem': "Usuario Inserido com sucesso!", "status": True}])

    else:
        return pd.DataFrame([{'Mensagem': "Usuario já´existe!", "status": False}])



def ConsultaUsuariosID(idUsuario):
    conn = ConexaoPostgreMPL.conexaoJohn()
    consulta = pd.read_sql("""
    select idusuario , "nomeUsuario" , "Perfil", "Senha" , "nomeLogin"  from "Easy"."Usuario" u    
    where idusuario = %s 
    """,conn,params=(idUsuario,))
    conn.close()

    return consulta

def AtualizarUsuario(idUsuario, nomeUsuario, Perfil, Senha ,login):
    consulta = ConsultaUsuariosID(idUsuario)

    if consulta.empty:
        return pd.DataFrame([{'Mensagem':"Usuario Nao encontrado!","status":False}])
    else:
        nomeUsuarioAtual = consulta['nomeUsuario'][0]
        if nomeUsuarioAtual == nomeUsuario :
            nomeUsuario = nomeUsuarioAtual

        PerfilAtual = consulta['Perfil'][0]
        if PerfilAtual == Perfil :
            Perfil = PerfilAtual

        SenhaAtual = consulta['Senha'][0]
        if SenhaAtual == Senha :
            Senha = SenhaAtual

        loginAtual = consulta['nomeLogin'][0]
        if loginAtual == login :
            login = loginAtual

        conn = ConexaoPostgreMPL.conexaoJohn()
        update = """
        update "Easy"."Usuario"
        set  "nomeUsuario" = %s , "Perfil" = %s ,"Senha" = %s, "nomeLogin" = %s
        where idusuario = %s 
        """

        cursor = conn.cursor()
        cursor.execute(update,(nomeUsuario, Perfil, Senha, login, idUsuario))
        conn.commit()
        cursor.close()

        conn.close()
        return pd.DataFrame([{'Mensagem': "Usuario Alterado com Sucesso!", "status": True}])

def AutentificacaoUsuario(login, senha):
    conn = ConexaoPostgreMPL.conexaoJohn()
    consulta = """
    select "Senha" from "Easy"."Usuario" u where u."nomeLogin" = %s
    """
    consulta = pd.read_sql(consulta,conn,params=(login,))

    conn.close()
    if consulta.empty:
        return pd.DataFrame([{'status':False,'Mensagem':'Login nao Encontrado!'}])

    elif senha == consulta['Senha'][0]:
        return pd.DataFrame([{'status':True,'Mensagem':'Senha Encontrada!'}])

    else:
        return pd.DataFrame([{'status':False,'Mensagem':'Senha Nao Validada!'}])


def InativarUsuario(idUsuario):
    consulta = ConsultaUsuariosID(idUsuario)
    if not consulta.empty:
        conn = ConexaoPostgreMPL.conexaoJohn()

        consulta = """
        update "Easy"."Usuario" 
        set "situacaoUsuario" = "INATIVO"
        where idusuario = %s  
        """

        cursor = conn.cursor()
        cursor.execute(consulta, (idUsuario))
        conn.commit()
        cursor.close()

        conn.close()
    else:
        return pd.DataFrame([{'Mensagem':"Usuario Nao encontrado!","status":False}])


