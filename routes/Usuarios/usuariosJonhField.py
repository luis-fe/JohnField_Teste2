from flask import Blueprint, jsonify, request
from functools import wraps
from Service import UsuariosJohnFild
import pandas as pd
usuarios_routesJohn = Blueprint('usuariosJohn', __name__) # Esse é o nome atribuido para o conjunto de rotas envolvendo usuario

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'Easy445277888':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@usuarios_routesJohn.route('/api/JonhField/Usuarios', methods=['GET'])
@token_required
def usuarios_jonh_field():
    consulta = UsuariosJohnFild.ConsultaUsuarios()
    # Obtém os nomes das colunas
    column_names = consulta.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    consulta_data = []
    for index, row in consulta.iterrows():
        consulta_dict = {}
        for column_name in column_names:
            consulta_dict[column_name] = row[column_name]
        consulta_data.append(consulta_dict)
    return jsonify(consulta_data)

@usuarios_routesJohn.route('/api/JonhField/Usuario/<int:id_usuario>', methods=['GET'])
@token_required
def UsuarioJonhField(id_usuario):
    consulta = UsuariosJohnFild.ConsultaUsuariosID(id_usuario)
    # Obtém os nomes das colunas
    column_names = consulta.columns.tolist()
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    consulta_data = []
    for index, row in consulta.iterrows():
        consulta_dict = {}
        for column_name in column_names:
            consulta_dict[column_name] = row[column_name]
        consulta_data.append(consulta_dict)
    return jsonify(consulta_data)

@usuarios_routesJohn.route('/api/JonhField/Autentificacao', methods=['GET'])
@token_required
def Autentificacao():
    # Obtém o código do usuário e a senha dos parâmetros da URL
    login = request.args.get('login')
    senha = request.args.get('senha')

    consulta = UsuariosJohnFild.AutentificacaoUsuario(login, senha)
    # Obtém os nomes das colunas
    column_names = consulta.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    consulta_data = []
    for index, row in consulta.iterrows():
        consulta_dict = {}
        for column_name in column_names:
            consulta_dict[column_name] = row[column_name]
        consulta_data.append(consulta_dict)
    return jsonify(consulta_data)

@usuarios_routesJohn.route('/api/JonhField/NovoUsuario', methods=['POST'])
@token_required
def NovoUsuario():
    data = request.get_json()
    idUsuario = data.get('idUsuario')
    nomeUsuario = data.get('nomeUsuario', '-')
    login = data.get('login', '')
    Perfil = data.get('Perfil', '')
    senha = data.get('senha','informar')

    if senha == 'informar':
        consulta = pd.DataFrame([{'status':False, 'mensagem':'Por favor Informe uma senha'}])
    else:
        consulta = UsuariosJohnFild.NovoUsuario(idUsuario, nomeUsuario,login , Perfil, senha)
    # Obtém os nomes das colunas
    column_names = consulta.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    consulta_data = []
    for index, row in consulta.iterrows():
        consulta_dict = {}
        for column_name in column_names:
            consulta_dict[column_name] = row[column_name]
        consulta_data.append(consulta_dict)
    return jsonify(consulta_data)


@usuarios_routesJohn.route('/api/JonhField/AlterarUsuario', methods=['PUT'])
@token_required
def AlterarUsuario():
    data = request.get_json()
    idUsuario = data.get('idUsuario')
    nomeUsuario = data.get('nomeUsuario', '')
    login = data.get('login', '')
    Perfil = data.get('Perfil', '')
    senha = data.get('senha', 'informar')


    consulta = UsuariosJohnFild.AtualizarUsuario(idUsuario, nomeUsuario, Perfil, senha ,login)
    # Obtém os nomes das colunas
    column_names = consulta.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    consulta_data = []
    for index, row in consulta.iterrows():
        consulta_dict = {}
        for column_name in column_names:
            consulta_dict[column_name] = row[column_name]
        consulta_data.append(consulta_dict)
    return jsonify(consulta_data)

@usuarios_routesJohn.route('/api/JonhField/DeletarUsuario', methods=['DELETE'])
@token_required
def DeletarUsuario():
    data = request.get_json()
    idUsuario = data.get('idUsuario')


    consulta = UsuariosJohnFild.InativarUsuario(idUsuario)
    # Obtém os nomes das colunas
    column_names = consulta.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    consulta_data = []
    for index, row in consulta.iterrows():
        consulta_dict = {}
        for column_name in column_names:
            consulta_dict[column_name] = row[column_name]
        consulta_data.append(consulta_dict)
    return jsonify(consulta_data)


@usuarios_routesJohn.route('/api/JonhField/AlterarSenha', methods=['PUT'])
@token_required
def AlterarSenha():
    data = request.get_json()

    login = data.get('login', '')
    senhaAtual = data.get('senhaAtual', '')
    senhaNova = data.get('senhaNova', 'informar')

    consulta = UsuariosJohnFild.AlterarSenha(login, senhaAtual, senhaNova)
    # Obtém os nomes das colunas
    column_names = consulta.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    consulta_data = []
    for index, row in consulta.iterrows():
        consulta_dict = {}
        for column_name in column_names:
            consulta_dict[column_name] = row[column_name]
        consulta_data.append(consulta_dict)
    return jsonify(consulta_data)