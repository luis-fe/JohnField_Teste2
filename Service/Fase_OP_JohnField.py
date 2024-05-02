import pandas as pd
import ConexaoPostgreMPL


def MovimentarOP(idUsuarioMovimentacao, codOP, novaFase):


    conn = ConexaoPostgreMPL


    conn.close()