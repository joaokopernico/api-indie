
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.protobuf.internal.containers import RepeatedScalarFieldContainer
import requests
import json
import sys
import database
import psycopg2
import psycopg2.extras

def consulta_ads(query, ga_service, customer_id):
    # Estrutura para armazenar a resposta como dict
    result_list = []

    try:
        print("Iniciando a solicitação com a query fornecida...")

        # Utilize um gerador para iterar sobre todas as páginas de resultados
        response = ga_service.search_stream(customer_id=customer_id, query=query)

        # Itera sobre as páginas e linhas retornadas pela API
        for batch in response:
            for row in batch.results:
                row_dict = {}

                # Acessa dinamicamente os campos que estão na query
                for field in query.split("SELECT")[1].split("FROM")[0].strip().split(","):
                    field = field.strip()
                    # Usa a notação "getattr" para acessar os atributos de forma dinâmica
                    field_path = field.split(".")
                    value = row
                    try:
                        for attr in field_path:
                            value = getattr(value, attr)

                        if type(value).__name__ == 'RepeatedScalarContainer':
                            value = ",".join(value) if len(value) > 0 else ""

                                            
                        # Verifica se é um RepeatedScalarContainer ou um campo repetido e converte para lista
                        if hasattr(value, '__iter__') and not isinstance(value, (str, bytes)):
                            value = list(value)

                        # Substituir os pontos por underscores nas chaves do dicionário
                        field = field.replace('.', '_')
                        row_dict[field] = value

                        
                    except AttributeError:
                        field = field.replace('.', '_')
                        row_dict[field] = "N/A"

                result_list.append(row_dict)

        print("Solicitação concluída com sucesso.")
        
        # Converte a lista de dicionários para JSON e retorna
        return json.dumps(result_list, indent=4)

    except GoogleAdsException as ex:
        print("A solicitação falhou com o seguinte erro:")
        print(f"Request ID: {ex.request_id}")
        print(f"Status: {ex.error.code().name}")
        for error in ex.failure.errors:
            print(f"Erro: {error.message}")
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f"\tNo campo: {field_path_element.field_name}")
        sys.exit(1)
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")
        sys.exit(1)

def main(var_ie_cliente, api_token, api_client_id, api_client_secret, api_refresh_token, api_codigo, customer_id):
# Inicializa o cliente do Google Ads dinamicamente com as credenciais da API
        try:
            client = GoogleAdsClient.load_from_dict({
                "developer_token": api_token,
                "client_id": api_client_id,
                "client_secret": api_client_secret,
                "refresh_token": api_refresh_token,
                "use_proto_plus": False
            })

            print("Credenciais carregadas com sucesso.")

            # Conectar ao banco de dados PostgreSQL
            # conn_aux1 = psycopg2.connect(**conn_params)
            conn_aux1 = database.get_connection()
            cursor_aux1 = conn_aux1.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # Executar a função SQL para obter as configurações da API
            cursor_aux1.execute("SELECT * FROM public.get_api_modulo_by_codigo(%s)", (api_codigo,))
            api_modulos = cursor_aux1.fetchall()

            for modulo in api_modulos:
                api_modulos_descricao = modulo['descricao']
                api_modulos_tabela = modulo['tabela']
                api_modulos_function_name = modulo['function_name']
                api_modulos_chave_primaria_insert = modulo['chave_primaria_insert'].strip() # Strip para evitar possíveis erros
                api_date_preset = modulo['date_preset']
                api_query = modulo['query']

                funcao = globals().get(api_modulos_function_name)

                if funcao:

                    # Carrega o client do google ads
                    ga_service = client.get_service("GoogleAdsService")
                    
                    dados = funcao(api_query, ga_service, customer_id)

                    database.inserir_ou_atualizar_dados_json(dados, api_modulos_tabela, var_ie_cliente,f'{api_modulos_chave_primaria_insert}', None, None)


        except Exception as e:
            print(f"Erro ao carregar as credenciais: {e}")
            sys.exit(1)
            
            
            
            