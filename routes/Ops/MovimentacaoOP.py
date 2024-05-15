from flask import Blueprint, jsonify, request
from functools import wraps
from Service import Fase_OP_JohnField
import pandas as pd
MovimentaoOP_routesJohn = Blueprint('MovimentaocaoOPJohn', __name__) # Esse é o nome atribuido para o conjunto de rotas envolvendo usuario

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'Easy445277888':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function
@MovimentaoOP_routesJohn.route('/api/JonhField/FasesDisponivelPMovimentarOP', methods=['GET'])
@token_required
def FasesDisponivelPMovimentarOP():
    codOP = request.args.get('codOP','')
    codCliente = request.args.get('codCliente','')

    consulta = Fase_OP_JohnField.FasesDisponivelPMovimentarOP(codOP, codCliente)
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

@MovimentaoOP_routesJohn.route('/api/JonhField/MovimentarOP', methods=['POST'])
@token_required
def MovimentarOP():
    data = request.get_json()
    idUsuarioMovimentacao = data.get('idUsuarioMovimentacao')
    codOP = data.get('codOP')
    codCliente = data.get('codCliente')
    codnovaFase = data.get('codnovaFase')


    consulta = Fase_OP_JohnField.MovimentarOP(idUsuarioMovimentacao, codOP, codCliente, codnovaFase)
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


@MovimentaoOP_routesJohn.route('/api/JonhField/EncerrarOP', methods=['POST'])
@token_required
def EncerrarOP():
    data = request.get_json()
    idUsuarioMovimentacao = data.get('idUsuarioMovimentacao')
    codOP = data.get('codOP')
    codCliente = data.get('codCliente')


    consulta = Fase_OP_JohnField.EncerrarOP(idUsuarioMovimentacao, codOP, codCliente)
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