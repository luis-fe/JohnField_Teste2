from flask import Blueprint, jsonify, request
from functools import wraps
from Service import OP_JonhField, OP_Tam_Cor_JohnField
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
    idUsuarioCriacao = data.get('idUsuarioCriacao')
    descricaoOP = data.get('descricaoOP','')
    codGrade = data.get('codGrade','')
    codRoteiro = data.get('codRoteiro','')
    codEmpresa = data.get('codEmpresa','1')


    consulta = OP_JonhField.criar_OP(codOP,idUsuarioCriacao,codCategoria,
                                     codCliente,descricaoOP, codGrade, codRoteiro, codEmpresa)
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


@GeraoOP_routesJohn.route('/api/JonhField/ObterOPsAberto', methods=['GET'])
@token_required
def ObterOPsAberto():
    consulta = OP_JonhField.ObterOP_EMAberto()
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

@GeraoOP_routesJohn.route('/api/JonhField/ObterGradeOP', methods=['GET'])
@token_required
def ObterGradeOP():
    codOP = request.args.get('codOP','')
    codCliente = request.args.get('codCliente','')

    consulta = OP_JonhField.BuscarGradeOP(codOP, codCliente)
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

@GeraoOP_routesJohn.route('/api/JonhField/InserirOPTamanhoCores', methods=['POST'])
@token_required
def InserirOPTamanhoCores():
    data = request.get_json()
    codOP = data.get('codOP')
    codCliente = data.get('codCliente')
    arrayCorTamQuantiades = data.get('arrayCorTamQuantiades')
    #arrayTamanhos = data.get('arrayTamanhos')
    #arrayQuantiades = data.get('arrayQuantiades')


    consulta = OP_Tam_Cor_JohnField.InserirCoresTamanhos(codOP,codCliente,arrayCorTamQuantiades)
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

@GeraoOP_routesJohn.route('/api/JonhField/AlterarOPTamanhoCores', methods=['PUT'])
@token_required
def AlterarOPTamanhoCores():
    data = request.get_json()
    codOP = data.get('codOP')
    codCliente = data.get('codCliente')
    arrayCorTamQuantiades = data.get('arrayCorTamQuantiades')
    #arrayTamanhos = data.get('arrayTamanhos')
    #arrayQuantiades = data.get('arrayQuantiades')


    consulta = OP_Tam_Cor_JohnField.AtualizarCoresTamanhos(codOP,codCliente,arrayCorTamQuantiades)
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


@GeraoOP_routesJohn.route('/api/JonhField/OPTamanhoCores', methods=['GET'])
@token_required
def get_OPTamanhoCores():
    codOP = request.args.get('codOP','')
    codCliente = request.args.get('codCliente','')


    consulta = OP_Tam_Cor_JohnField.ConsultaTamCor_OP(codOP,codCliente)
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

@GeraoOP_routesJohn.route('/api/JonhField/ConsultaRoteiroOP', methods=['GET'])
@token_required
def ConsultaRoteiroOP():
    codOP = request.args.get('codOP','')
    codCliente = request.args.get('codCliente','')


    consulta = OP_JonhField.ConsultaRoteiroOP(codOP,codCliente)
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