from flask import Blueprint, jsonify, request
from functools import wraps
from Service.Operacoes import categoriaOperacao
import pandas as pd

CategoriaOperacao_routesJohn = Blueprint('CategoriaOperacao_routes', __name__) # Esse é o nome atribuido para o conjunto de rotas envolvendo usuario

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'Easy445277888':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function
@CategoriaOperacao_routesJohn.route('/api/JonhField/ConsultarCategoriaOperacao', methods=['GET'])
@token_required
def get_ConsultarCategoriaOperacao():


        busca = categoriaOperacao.ConsultarCategoriaOperacao()

        # Verifica se 'busca' é um DataFrame
        if not isinstance(busca, pd.DataFrame):
            return jsonify({'error': 'Unexpected data format'}), 500

        # Obtém os nomes das colunas
        column_names = busca.columns.tolist()

        # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
        consulta_data = busca.to_dict(orient='records')

        return jsonify(consulta_data)