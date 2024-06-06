import datetime
def contar_sabados_domingos(data_inicio, data_fim):
    data_inicio = datetime.datetime.strptime(data_inicio, "%Y-%m-%d")
    data_fim = datetime.datetime.strptime(data_fim, "%Y-%m-%d")

    numero_sabados = 0
    numero_domingos = 0

    data_atual = data_inicio
    while data_atual <= data_fim:
        if data_atual.weekday() == 5:  # Sábado
            numero_sabados += 1
        elif data_atual.weekday() == 6:  # Domingo
            numero_domingos += 1
        data_atual += datetime.timedelta(days=1)
    dias = numero_sabados + numero_domingos
    return dias
