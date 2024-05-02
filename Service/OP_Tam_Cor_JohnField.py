import pandas as pd
import ConexaoPostgreMPL


def InserirCoresTamanhos(codOP, codCliente, arrayCores, arrayTamanhos):
    conn = ConexaoPostgreMPL
    conn.close()