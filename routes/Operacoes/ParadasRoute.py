from flask import Blueprint, jsonify, request
from functools import wraps
from Service import ParadasClass
import pandas as pd

Paradas_rotuesJohn = Blueprint('Paradas_rotuesJohn',__name__)  # Esse é o nome atribuido para o conjunto de rotas envolvendo usuario


def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'Easy445277888':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@Paradas_rotuesJohn.route('/api/JonhField/ConsultarRegistroParadas', methods=['GET'])
@token_required
def get_ConsultarRegistroParadas():
    dataInicio = request.args.get('dataInicio')
    dataFinal = request.args.get('dataFinal')
    try:
        paradas = ParadasClass.Paradas(dataInicio, dataFinal)
        busca = paradas.ConsultarParadasPerido()

        # Verifica se 'busca' é um DataFrame
        if not isinstance(busca, pd.DataFrame):
            return jsonify({'error': 'Unexpected data format'}), 500

        # Obtém os nomes das colunas
        column_names = busca.columns.tolist()

        # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
        consulta_data = busca.to_dict(orient='records')

        return jsonify(consulta_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500



@Paradas_rotuesJohn.route('/api/JonhField/InserirRegistroParada', methods=['POST'])
@token_required
def salvar_InserirRegistroParada():
    data = request.get_json()
    dataInicio = data.get('dataInicio', '-')
    dataFinal = data.get('dataFinal')
    horaInicio = data.get('horaInicio')
    horaFinal = data.get('horaFinal')
    codOperador = data.get('codOperador')
    motivo = data.get('motivo')

    parada = ParadasClass.Paradas(dataInicio, dataFinal, horaInicio, horaFinal, codOperador, motivo)
    consulta = parada.InserirParada()
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
