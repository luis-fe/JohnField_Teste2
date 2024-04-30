from flask import Blueprint

# Crie um Blueprint para as rotas
routes_blueprint = Blueprint('routes', __name__)

# Importe as rotas dos arquivos individuais
from .usuariosJonhField import usuarios_routesJohn 
from .clenteJohnField import cliente_routesJohn
from .categoriaJohnField import categoria_routesJohn

# Registre as rotas nos blueprints
routes_blueprint.register_blueprint(usuarios_routesJohn)
routes_blueprint.register_blueprint(cliente_routesJohn)
routes_blueprint.register_blueprint(categoria_routesJohn)

