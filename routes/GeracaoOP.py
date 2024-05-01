from flask import Blueprint, jsonify, request
from functools import wraps
from Service import OP_JonhField
import pandas as pd
GeraoOP_routesJohn = Blueprint('GeraOPJohn', __name__) # Esse é o nome atribuido para o conjunto de rotas envolvendo usuario

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'Easy445277888':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@GeraoOP_routesJohn.route('/api/JonhField/CriarOP', methods=['POST'])
@token_required
def CriarOP():
    data = request.get_json()
    codOP = data.get('codOP')
    codCliente = data.get('codCliente')
    codCategoria = data.get('codCategoria')
    codFaseInicial = data.get('codFaseInicial')


    consulta = OP_JonhField.CrirarOP(codOP,idUsuarioCriacao,codCategoria,codCliente,codFaseInicial)
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
