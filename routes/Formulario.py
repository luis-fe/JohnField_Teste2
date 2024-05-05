from flask import Blueprint, jsonify, request, send_from_directory
from functools import wraps
from Service import FormularioOP
import pandas as pd

formulario_routesJohn = Blueprint('FormularioJohn', __name__) # Esse é o nome atribuido para o conjunto de rotas envolvendo usuario

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'Easy445277888':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@formulario_routesJohn.route('/api/JonhField/GerarPDF', methods=['GET'])
@token_required
def GerarPDF():
    # Obtém o código do usuário e a senha dos parâmetros da URL
    codCliente = request.args.get('codCliente')
    codOP = request.args.get('codOP')

    caminho_pdf = FormularioOP.criar_pdf('formulario.pdf',codCliente,str(codOP))
    # Retorna o arquivo PDF
    return send_from_directory('.','formulario.pdf')
