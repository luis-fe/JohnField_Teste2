from flask import Blueprint

# Crie um Blueprint para as rotas
routes_blueprint = Blueprint('routes', __name__)

# Importe as rotas dos arquivos individuais
from .usuariosJonhField import usuarios_routesJohn 
from .clenteJohnField import cliente_routesJohn
from .categoriaJohnField import categoria_routesJohn
from .FaseJohnField import fase_routesJohn
from .gradePadrao import gradePadrao_routesJohn
from .GeracaoOP import GeraoOP_routesJohn
from .MovimentacaoOP import MovimentaoOP_routesJohn

# Registre as rotas nos blueprints
routes_blueprint.register_blueprint(usuarios_routesJohn)
routes_blueprint.register_blueprint(cliente_routesJohn)
routes_blueprint.register_blueprint(categoria_routesJohn)
routes_blueprint.register_blueprint(fase_routesJohn)
routes_blueprint.register_blueprint(gradePadrao_routesJohn)
routes_blueprint.register_blueprint(GeraoOP_routesJohn)
routes_blueprint.register_blueprint(MovimentaoOP_routesJohn)

