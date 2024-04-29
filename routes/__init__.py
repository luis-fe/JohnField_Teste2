from flask import Blueprint

# Crie um Blueprint para as rotas
routes_blueprint = Blueprint('routes', __name__)

# Importe as rotas dos arquivos individuais

from .usuariosJonhField import usuarios_routesJohn 

# Registre as rotas nos blueprints


routes_blueprint.register_blueprint(usuarios_routesJohn)
