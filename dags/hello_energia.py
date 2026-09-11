from __future__ import annotations

import logging
import pendulum
import requests
import os

from airflow.sdk import dag, task 

logger = logging.getLogger(__name__) #Faz logging do módulo em que está inserido


# Configuração das taks do DAG
config_task = {
    "retries": 3,   #número de tentativas de execução da task em caso de falha
    "retry_delay": pendulum.duration(minutes=5),    #tempo de espera entre as tentativas de execução da task
}

@dag(
    dag_id="hello_energia",  #Identificador do DAG
    default_args=config_task,     #Configuração das tasks do DAG
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),    #Data de início do DAG - confirmar qual a melhor escolha sobre
    schedule="@daily",     #Quando o DAG será executado
    catchup=False,        #tasks que não foram executadas no passado não serão executadas

)
def hello_energia():

    @task(
        task_id="extrair_dados", #Identificador da task
    )
    def extrair_dados(ds=None): # data de execução extrai o ano da execução do DAG, por isso o parâmetro ds[:4] é passado para a função extrair_dados
     logger.info("iniciando a extração dos dados do ONS") #log de início da extração dos dados do ONS

     ano_execucao = ds[:4]  # Variavel que armazena o ano da execução do DAG, extraído do parâmetro ds[:4]
     url_ons = f"https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/balanco_energia_subsistema_ho/BALANCO_ENERGIA_SUBSISTEMA_2026.csv" #Define a URL do arquivo a ser lido, com base no ano da execução do DAG

     request = requests.get(url_ons, stream=True) #Faz a requisição do arquivo CSV do ONS, com base na URL definida acima
     request.raise_for_status()  #Verifica se a requisição foi bem sucedida, caso contrário, levanta uma exceção    

     name_arquivo = f"ons_{ano_execucao}.csv" #Define o nome do arquivo a ser salvo, com base no ano da execução do DAG
     caminho_arquivo = f"/opt/airflow/data/{name_arquivo}" #caminho do arquivo a ser salvo
     os.makedirs(os.path.dirname(caminho_arquivo), exist_ok=True) #Cria a pasta data, caso não exista    

     with open(caminho_arquivo, "wb") as arquivo: #Abre o arquivo para escrita em modo binário
        for chunk in request.iter_content(chunk_size=8192): #Itera sobre o conteúdo do arquivo em blocos de 8192 bytes
            if chunk: #Remove blocos vazios
                arquivo.write(chunk) #Escreve o conteúdo do arquivo no disco
     logger.info(f"Arquivo {name_arquivo} salvo com sucesso em {caminho_arquivo}") #Log de sucesso na gravação do arquivo
     return caminho_arquivo #Retorna o caminho do arquivo salvo


    @task(
        task_id="carregar_dados", #Identificador da task
    )
    def carregar_dados():
        return [
        {
            "subsitemas": "SE/CO",
            "hidraulico": 35000, "termico": 15000, "eolica": 5000, "solar": 2000
        },
        {
            "subsitemas": "NE",
            "hidraulico": 25000, "termico": 10000, "eolica": 3000, "solar": 1500
        },
        {
            "subsitemas": "N",
            "hidraulico": 15000, "termico": 5000, "eolica": 2000, "solar": 1000
        },
        {
            "subsitemas": "S",
            "hidraulico": 20000, "termico": 8000, "eolica": 4000, "solar": 2500
        }
    ]

    @task(
        task_id="processar_dados", #Identificador da task    
    )
    def processar_dados(dados_ons):
        logger.info("iniciando o processamento dos dados do ONS") #Log de início do processamento dos dados do ONS

        #List para armazenar os resultados do processamento
        dados_transformados = []

        for dado in dados_ons:
            sub = dado["subsitemas"] # Acessa o valor do subsistema no dicionário
            hidraulico = dado ['hidraulico'] # Acessa o valor da energia hidraulica no dicionário
            termico = dado ['termico'] # Acessa o valor da energia termica no dicionário
            eolica = dado ['eolica'] # Acessa o valor da energia eolica no dicionário
            solar = dado ['solar'] # Acessa o valor da energia solar no dicionário

            #Calcula a energia total do subsistema
            total = hidraulico + termico + eolica + solar
            #Calculo energia renovavel do subsistema
            if total > 0:
                renovavel = hidraulico + eolica + solar
                pct_renovavel = (renovavel / total) * 100
            else:
                pct_renovavel = 0.0

            logger.info(f"Subsistemas {sub} processado: Total={total}MW, Renovável={pct_renovavel}%") 
            dados_transformados.append({  # adiciona os dados transformados à lista
                "subsistema": sub,         #adiciona o valor do subsistema ao dicionário
                "total": total,             #Adiciona o valor da energia total ao dicionário
                "porcentagem_renovavel": pct_renovavel  
            })
        logger.info("Processamento dos dados do ONS concluído com sucesso") #Log de conclusão do processamento dos dados do ONS

        return dados_transformados
    @task(
        task_id="salvar_dados", #Identificador da task
    )
    def salvar_dados(dados_transformados):
        logger.info("iniciando a gravação dos dados transformados") #Log de início da gravação dos dados transformados
        for registro in dados_transformados:
            sub = registro["subsistema"] # Acessa o valor do subsistema no dicionário
            total = registro["total"] # Acessa o valor da energia total no dicionário
            pct_renovavel = registro["porcentagem_renovavel"] # Acessa o valor da porcentagem de energia renovável no dicionário

            logger.info(f"Subsistemas {sub}: Total={total}MW, Renovável={pct_renovavel}%") #Log dos dados transformados
    caminho_csv = extrair_dados()
    dados_ons = carregar_dados() #Chama a task carregar_dados
    dados_transformados = processar_dados(dados_ons) #Chama a task processar_dados
    salvar_dados(dados_transformados) #Chama a task salvar_dados    
hello_energia()