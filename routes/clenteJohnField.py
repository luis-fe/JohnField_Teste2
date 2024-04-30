from flask import Blueprint, jsonify, request
from functools import wraps
from Service import ClientesJohnField
import pandas as pd
cliente_routesJohn = Blueprint('clienteJohn', __name__) # Esse é o nome atribuido para o conjunto de rotas envolvendo usuario

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'Easy445277888':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@cliente_routesJohn.route('/api/JonhField/Clientes', methods=['GET'])
@token_required
def Clientes_jonh_field():
    consulta = ClientesJohnField.ConsultaClientes()
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
@usuarios_routesJohn.route('/api/JonhField/NovoCliente', methods=['POST'])
@token_required
def NovoCliente():
    data = request.get_json()
    codCliente = data.get('codCliente')
    nomeCliente = data.get('nomeCliente', '-')


    consulta = ClientesJohnField.inserirCliente(codCliente, nomeCliente)
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


@usuarios_routesJohn.route('/api/JonhField/AlterarCliente', methods=['PUT'])
@token_required
def AlterarCliente():
    data = request.get_json()
    codCliente = data.get('codCliente')
    nomeCliente = data.get('nomeCliente', '-')


    consulta = ClientesJohnField.UpdateCliente(codCliente, nomeCliente)
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