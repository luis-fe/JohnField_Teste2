import datetime
import pandas as pd


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


def calcular_dias_entre_datas(data_inicio, data_fim):
    dias_naouteis = contar_sabados_domingos(data_inicio, data_fim)

    data_inicio = datetime.datetime.strptime(data_inicio, "%Y-%m-%d")
    data_fim = datetime.datetime.strptime(data_fim, "%Y-%m-%d")

    diferenca = data_fim - data_inicio
    diferenca = diferenca.days - dias_naouteis

    return diferenca


# Exemplo de uso
data_inicio = "2024-01-01"
data_fim = "2024-12-31"
numero_dias = calcular_dias_entre_datas(data_inicio, data_fim)
print(f"Número de dias entre {data_inicio} e {data_fim}: {numero_dias} dias")

# Exemplo de DataFrame de feriados
feriados_data = {
    "data": [
        "2024-01-01",
        "2024-04-21",
        "2024-05-01",
        "2024-09-07",
        "2024-10-12",
        "2024-11-02",
        "2024-11-15",
        "2024-12-25",
    ]
}

feriados_df = pd.DataFrame(feriados_data)
feriados_df["data"] = pd.to_datetime(feriados_df["data"])


# Função para contar feriados entre duas datas
def contar_feriados(data_inicio, data_fim, feriados_df):
    data_inicio = pd.to_datetime(data_inicio)
    data_fim = pd.to_datetime(data_fim)

    feriados_no_intervalo = feriados_df[(feriados_df["data"] >= data_inicio) & (feriados_df["data"] <= data_fim)]
    return len(feriados_no_intervalo)


# Exemplo de uso
data_inicio = "2024-01-01"
data_fim = "2024-12-31"
numero_feriados = contar_feriados(data_inicio, data_fim, feriados_df)
print(f"Número de feriados entre {data_inicio} e {data_fim}: {numero_feriados}")
