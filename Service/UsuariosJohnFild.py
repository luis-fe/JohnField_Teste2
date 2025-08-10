import pandas as pd
import ConexaoPostgreMPL
from Service import Usuario_empresa

def ConsultaUsuarios():
    conn = ConexaoPostgreMPL.conexaoJohn()
    consulta = pd.read_sql("""
            select 
                idusuario ,
                "nomeLogin" ,
                "nomeUsuario", 
                "Perfil", 
                permite_cancelar_op  
            from 
                "Easy"."Usuario" u  
            where 
                u."situacaoUsuario" = 'ATIVO'  
    """,conn)
    conn.close()
    consulta['permite_cancelar_op'].fillna('NAO',inplace=True)


    # buscar usuarios por empresa

    user_emp = Usuario_empresa.Usuario_empresa()
    
    buscar = user_emp.consulta_usuarios_empresa()
    buscar['codUsuario'] = buscar['codUsuario'].astype(str) 
    consulta['codUsuario'] = consulta['codUsuario'].astype(str) 
    
        # Agrupar empresas por usuário
    empresas_por_usuario = (
        buscar
        .rename(columns={"codEmpresa": "empresasAutorizadas","codUsuario":"idusuario"})
        .groupby("idusuario")["empresasAutorizadas"]   # supondo que a coluna se chama idempresa
        .agg(lambda x: list(map(str, x)))    # lista de strings
        .reset_index()
    )

    # Fazer o merge
    resultado = consulta.merge(empresas_por_usuario, on="idusuario", how="left")

    resultado.fillna('-',inplace = True)

    return resultado

def NovoUsuario(idUsuario, nomeUsuario,login , Perfil, Senha, permite_cancelar_op):

    consulta = ConsultaUsuariosID(idUsuario)

    if consulta.empty:

        conn = ConexaoPostgreMPL.conexaoJohn()

        insert = """
        insert into "Easy"."Usuario" ( idusuario , "nomeUsuario" , "nomeLogin","Perfil" ,"Senha" ,"situacaoUsuario", permite_cancelar_op ) values (%s , %s, %s ,%s, %s, 'ATIVO', %s)
        """
        cursor = conn.cursor()
        cursor.execute(insert,(idUsuario, nomeUsuario, login, Perfil, Senha, permite_cancelar_op))
        conn.commit()
        cursor.close()

        conn.close()

        return pd.DataFrame([{'Mensagem': "Usuario Inserido com sucesso!", "status": True}])

    else:
        return pd.DataFrame([{'Mensagem': "Usuario já´existe!", "status": False}])



def ConsultaUsuariosID(idUsuario):
    conn = ConexaoPostgreMPL.conexaoJohn()
    consulta = pd.read_sql("""
    select idusuario , "nomeUsuario" , "Perfil", "Senha" , "nomeLogin", permite_cancelar_op  from "Easy"."Usuario" u    
    where idusuario = %s 
    """,conn,params=(int(idUsuario),))
    conn.close()
    consulta['permite_cancelar_op'].fillna('NAO',inplace=True)

    return consulta

def AtualizarUsuario(idUsuario, nomeUsuario, Perfil ,login, permite_cancelar_op):
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


        loginAtual = consulta['nomeLogin'][0]
        if loginAtual == login :
            login = loginAtual


        permite_cancelar_opAtual = consulta['permite_cancelar_op'][0]
        if permite_cancelar_opAtual == permite_cancelar_op :
            permite_cancelar_op = permite_cancelar_opAtual

        conn = ConexaoPostgreMPL.conexaoJohn()
        update = """
        update "Easy"."Usuario"
        set  "nomeUsuario" = %s , "Perfil" = %s , "nomeLogin" = %s, permite_cancelar_op = %s
        where idusuario = %s 
        """

        cursor = conn.cursor()
        cursor.execute(update,(nomeUsuario, Perfil, login, permite_cancelar_op, idUsuario ))
        conn.commit()
        cursor.close()

        conn.close()
        return pd.DataFrame([{'Mensagem': "Usuario Alterado com Sucesso!", "status": True}])

def AutentificacaoUsuario(login, senha):
    conn = ConexaoPostgreMPL.conexaoEngine()
    consulta = """
    select "Senha", "idusuario" from "Easy"."Usuario" u where u."nomeLogin" = %s
    """
    consulta = pd.read_sql(consulta,conn,params=(login,))

    if consulta.empty:
        return pd.DataFrame([{'status':False,'Mensagem':'Login nao Encontrado!'}])

    elif senha == str(consulta['Senha'][0]):
        return pd.DataFrame([{'status':True,'Mensagem':'Senha Encontrada!','idUsuario':consulta['idusuario'][0], 'senha':senha , 'consulta' : consulta['Senha'][0]}])

    else:
        return pd.DataFrame([{'status':False,'Mensagem':'Senha Nao Validada!','senha':senha , 'consulta' : consulta['Senha'][0]}])


def InativarUsuario(idUsuario):
    consulta = ConsultaUsuariosID(idUsuario)
    if not consulta.empty:
        conn = ConexaoPostgreMPL.conexaoJohn()

        consulta = """
        update "Easy"."Usuario" 
        set "situacaoUsuario" = 'INATIVO'
        where idusuario = %s  
        """

        cursor = conn.cursor()
        cursor.execute(consulta, (idUsuario,))
        conn.commit()
        cursor.close()

        conn.close()

        return pd.DataFrame([{'Mensagem':"Usuario Deletado com Sucesso!","status":True}])

    else:
        return pd.DataFrame([{'Mensagem':"Usuario Nao encontrado!","status":False}])

def AlterarSenha(nomeLogin, senhaAtual, novaSenha):
    consulta = ConsultaUsuarios()
    consulta = consulta[consulta['nomeLogin'] == nomeLogin].reset_index()

    #Avaliando a senha 
    avaliar = ConsultaUsuariosID(consulta['idusuario'][0]).reset_index()
    senhaAtualAvaliar = avaliar['Senha'][0]

    if senhaAtualAvaliar == senhaAtual:
        with ConexaoPostgreMPL.conexaoJohn() as conn:
            with conn.cursor() as cursor:
                update = """
                update "Easy"."Usuario"  
                set  "Senha" = %s 
                where idusuario = %s 
                """
                cursor.execute(update,(novaSenha, int(consulta['idusuario'][0])))
                conn.commit()    
        return pd.DataFrame([{'status':True, 'mensagem':"senha alterada com sucesso"}])                       
    else:
        return pd.DataFrame([{'status':False, 'mensagem':"senha atual nao corresponde"}])