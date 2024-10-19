import psycopg2
import psycopg2.extras
import os
import csv
import argparse
from metodos import gads, meta, refresh, sienge, api_key
import database

def salvar_json_como_csv(json_data, pasta_destino):
    # Cria a pasta se não existir
    if not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino)

    # Define o caminho completo para o arquivo CSV
    caminho_csv = os.path.join(pasta_destino, 'dados.csv')

    # Abre o arquivo CSV para escrita
    with open(caminho_csv, 'w', newline='', encoding='utf-8') as csvfile:
        # Obtém os nomes das chaves do JSON (colunas do CSV)
        campos = json_data[0].keys()
        escritor = csv.DictWriter(csvfile, fieldnames=campos)

        # Escreve o cabeçalho do CSV
        escritor.writeheader()

        # Escreve os dados do JSON no CSV
        for item in json_data:
            escritor.writerow(item)

    print(f'Arquivo CSV salvo em: {caminho_csv}')

def main(metodos=None):
    var_ie_cliente = 2

    try:
        # Conectar ao banco de dados PostgreSQL
        conn = database.get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Executar a função SQL para obter as configurações da API
        cursor.execute("SELECT * FROM public.get_api_configuracao_by_cliente(%s)", (var_ie_cliente,))
        api_configs = cursor.fetchall()

        # Se não houver métodos fornecidos, consumirá todos
        if not metodos:
            metodos = ['GADS', 'META', 'REFRESH', 'SIENGE', 'APIKEY']

        # Iterar sobre as configurações de API retornadas
        for config in api_configs:
            api_codigo = config['api_codigo']
            api_client_id = config['api_client_id']
            api_client_secret = config['api_client_secret']
            api_service_url = config['api_service_url']
            api_refresh_token = config['api_refresh_token']
            api_metodo = config['api_metodo']
            api_token = config['api_token']
            act_ = config['api_account_id']
            customer_id = config['api_account_id']

            # Verifica se o método atual está na lista dos métodos desejados
            if api_metodo in metodos:
          
                if api_metodo == 'GADS':

                    gads.main(var_ie_cliente, api_token, api_client_id, api_client_secret, api_refresh_token, api_codigo, customer_id)

                if api_metodo == 'META':
                    
                    meta.main(var_ie_cliente, api_service_url, api_client_id, api_client_secret, api_codigo, act_, api_token)    
                    
                if api_metodo == 'REFRESH':
                    
                    refresh.main(var_ie_cliente, api_client_id, api_client_secret, api_refresh_token, api_service_url, api_codigo)

                if api_metodo == 'SIENGE':
                    
                    sienge.main(var_ie_cliente, api_client_id, api_client_secret, api_codigo, api_service_url, access_token)

                if api_metodo == 'APIKEY':
                    api_key.main(var_ie_cliente, api_codigo, api_client_secret)

    except Exception as e:
        print(f"Ocorreu um erro: {e}")

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Processar métodos da API.')
    parser.add_argument('--metodos', nargs='*', help='Lista de métodos para processar, ex: GADS META APIKEY')
    args = parser.parse_args()

    main(args.metodos)