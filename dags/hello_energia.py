from __future__ import annotations

import logging
import pendulum

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
    dados_ons = carregar_dados() #Chama a task carregar_dados
    dados_transformados = processar_dados(dados_ons) #Chama a task processar_dados
    salvar_dados(dados_transformados) #Chama a task salvar_dados    
hello_energia()