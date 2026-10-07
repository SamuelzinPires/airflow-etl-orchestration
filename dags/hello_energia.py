#importações de bibliotecas necessárias para o funcionamento do DAG
from __future__ import annotations
from airflow.sdk import dag, task 
#importações para o funcionamento da extracao e transformação dos dados do ONS
import logging
import pendulum
import requests
import os
import pandas as pd
#import para conferencia dos dados
import pandera as pa 
from pandera import Column, DataFrameSchema, Check

logger = logging.getLogger(__name__) #Faz logging do módulo em que está inserido

# Configuração das taks do DAG
config_task = {
    "retries": 3,   #número de tentativas de execução da task em caso de falha
    "retry_delay": pendulum.duration(minutes=5),    #tempo de espera entre as tentativas de execução da task
}

@dag(
    dag_id="ONS_energia",  #Identificador do DAG
    default_args=config_task,     #Configuração das tasks do DAG
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),    #Data de início do DAG - confirmar qual a melhor escolha sobre
    schedule="@daily",     #Quando o DAG será executado
    catchup=False,        #tasks que não foram executadas no passado não serão executadas

)
def extracao_dadosONS():

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
    def carregar_dados(ds, caminho_arquivo): # data de execução extrai o ano da execução do DAG, por isso o parâmetro ds[:4] é passado para a função carregar_dados
        logger.info("iniciando a carga dos dados do ONS") #log de início da carga dos dados do ONS
        df = pd.read_csv(caminho_arquivo, sep=";" , encoding="utf-8") #Lê o arquivo CSV do ONS e armazena em um DataFrame do Pandas
        logger.info(df.head()) #Exibe as 5 primeiras linhas do DataFrame apenas para confimar se foi realizado a leitura correta do arquivo CSV

        df['din_instante'] = pd.to_datetime(df['din_instante']) #converte a coluna din_instante para o formato datetime
        df['din_instante'] = df['din_instante'].dt.tz_localize('America/Sao_Paulo') #confirmar formato que a coluna já vem de padrão, se já vem com fuso horário ou não, caso não venha, será necessário adicionar o fuso horário de São Paulo
        df['din_instante'] = df['din_instante'].dt.tz_convert('UTC') #converte a coluna din_instante para o formato datetime e para o fuso horário UTC
        logger.info(f"Confirmação de que a coluna din_instante está no formato UTC: {df['din_instante'].head()}") #Log de confirmação de que a coluna din_instante está no formato UTC

        logger.info(f"confirmando se ainda tem dados antigos para serem processados, filtrando os dados do dia da execução do DAG") #Log de início da filtragem dos dados do dia da execução do DAG
        ds_execucao = pd.to_datetime(ds).date() #converte o parâmetro ds para o formato datetime e extrai apenas a data da execucao 
        df_filtro = df[df['din_instante'].dt.date == ds_execucao] #filtra o DataFrame para pegar apenas os dados do dia da execução do DAG
        if df_filtro.empty:
            raise ValueError(f"Não existem dados do dia da execução do DAG: {ds_execucao}") #levanta uma exceção caso não existam dados do dia da execução do DAG
        else:
            logger.info(f"Quantidade de dados filtrados do dia da execução do DAG: {len(df_filtro)}") #Log de confirmação da quantidade de dados filtrados do dia da execução do DAG
            df_filtro['din_instante'] = df_filtro['din_instante'].astype(str) #converte a coluna din_instante para o formato string, para que seja possível retornar o df filtrado para o formato de dicionário
        return df_filtro.to_dict(orient="records") #retorna o df filtrado para o formato de dicionário, com orient="records" para que cada linha do DataFrame seja um dicionário separado

    @task(
        task_id="transformar_dados", #Identificador da task
    )    
    def transformacao_dadosONS(df_filtro):
  
        df_filtro = pd.DataFrame(df_filtro) #converte o dicionário de volta para o df do Pandas
        df_filtro['val_geracao_total'] = df_filtro[['val_gerhidraulica', 'val_gertermica', 'val_gereolica', 'val_gersolar']].sum(axis=1) #sama dos valores geral 
        df_filtro ['val_geracao_renovavel']= df_filtro[['val_gerhidraulica', 'val_gersolar', 'val_gereolica']].sum(axis=1) #soma dos valores renovaveis 
        df_medias = df_filtro.groupby('nom_subsistema').agg({'val_geracao_total': 'mean', 'val_geracao_renovavel': 'mean'}).reset_index() #calcula a média dos valores das colunas val_geracao_total e val_geracao_renovavel do DataFrame, agrupando pelos subsistemas existentes
        df_medias['pct_renovavel'] = (df_medias['val_geracao_renovavel'] / df_medias['val_geracao_total']) * 100  #calcula a porcentagem de geração renovável em relação à geração total
        logger.info(f"Transformação dos dados do ONS concluída com sucesso")
        return df_medias.to_dict(orient="records") #retorna o df transformado para o formato de dicionário, com orient="records" para que cada linha do DataFrame seja um dicionário separado
    
    @task(
        task_id="auditoria_dados" #Identificador da task
    )
    def auditoria_colunas(df_medias):

        df_medias = pd.DataFrame(df_medias) #converte o dicionário de volta para o df do Pandas

        #conjuntos de colunas para auditoria
        nom_subsistema = ['NORTE', 'NORDESTE','SUL', 'SUDESTE/CENTRO-OESTE', 'SISTEMA INTERLIGADO NACIONAL'] #lista de subsistemas existentes no ONS
        
        #cria o schema para auditoria
        schema = DataFrameSchema(
            columns={
                "nom_subsistema": Column(
                    dtype=str,
                    checks=Check.isin(nom_subsistema), #verifica se os valores da coluna nom_subsistema estão dentro da lista de subsistemas existentes
                ),
                "pct_renovavel": Column(
                    dtype=float,
                    checks=Check.in_range(0, 100), #verifica se os valores da coluna pct_renovavel estão entre 0 e 100
                ),
                "val_geracao_total": Column(
                    dtype=float,
                    checks=Check.ge(0), #verifica se os valores da coluna val_geracao_total são maiores ou iguais a 0
                ),
                "val_geracao_renovavel": Column(
                    dtype=float,
                    checks=Check.ge(0), #verifica se os valores da coluna val_geracao_renovavel são maiores ou iguais a 0
                ),
            }
        )
        try:
            schema.validate(df_medias, lazy=True) #valida o DataFrame com base no schema criado acima
            logger.info(f"Auditoria das colunas do DataFrame concluída com sucesso")
        except pa.errors.SchemaErrors as erro: 
            logger.error(f"Erro na auditoria das colunas do DataFrame: {erro.failure_cases}") #log de erro caso a auditoria falhe
            raise ValueError(f"Erro na auditoria das colunas do DataFrame: {erro.failure_cases}") #levanta uma exceção caso a auditoria falhe

    caminho = extrair_dados()
    dados_filtrados = carregar_dados(caminho_arquivo=caminho )
    resultado = transformacao_dadosONS(dados_filtrados)
    auditoria_colunas(resultado)


extracao_dadosONS()