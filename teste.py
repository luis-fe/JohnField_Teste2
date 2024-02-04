import pandas as pd

# Criar um DataFrame de exemplo
data = {'Nome': ['João', 'Maria', 'Pedro'],
        'Idade': [25, 30, 22],
        'Cidade': ['São Paulo', 'Rio de Janeiro', 'Belo Horizonte']}

df = pd.DataFrame(data)

# Definir uma variável global para armazenar o DataFrame
global_dataframe = df

# Agora você pode acessar 'global_dataframe' em qualquer parte do seu código
# e manipular o DataFrame conforme necessário.
# Por exemplo:
def mostrar_dataframe():
    print(global_dataframe)

# Chamada da função para mostrar o DataFrame
mostrar_dataframe()
