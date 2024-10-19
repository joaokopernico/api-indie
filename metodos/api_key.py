import database
import requests
import psycopg2.extras


def fetch_consulta_nivel1_apikey(start_number, client_secret, base_url):
    url = base_url
    headers = {
        "Accept": "application/json",
        "apikey": client_secret
    }
    params = {
        "quantity": "50",
        "start": start_number
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Erro na solicitação: {response.status_code}")
    
 
def consulta_nivel1_apikey(client_secret, base_url):
    base_url = base_url.replace("\n", "").replace("\r", "")

    start_number = 0
    all_records = []

    while True:
        records_json = fetch_consulta_nivel1_apikey(str(start_number), client_secret, base_url)

        if not records_json:  # Se a lista estiver vazia, interrompe o loop
            break

        all_records.extend(records_json)  # Adiciona os novos contatos à lista
        start_number += 50
    return all_records
   

def main(var_ie_cliente, api_codigo, api_client_secret):
    # Conectar ao banco de dados PostgreSQL
                    conn_aux1 = database.get_connection()
                    cursor_aux1 = conn_aux1.cursor(cursor_factory=psycopg2.extras.DictCursor)

                    # Executar a função SQL para obter as configurações da API
                    cursor_aux1.execute("SELECT * FROM public.get_api_modulo_by_codigo(%s)", (api_codigo,))
                    api_modulos = cursor_aux1.fetchall()

                    # Iterar sobre as configurações de API retornadas
                    for modulo in api_modulos:
                        print(api_modulos)
                        # api_modulos_codigo = modulo['codigo']
                        api_modulos_descricao = modulo['descricao']
                        api_modulos_tabela = modulo['tabela']
                        api_modulos_function_name = modulo['function_name']
                        # api_modulos_api_codigo = modulo['api_codigo']
                        api_modulos_url = modulo['url']
                        api_modulos_data_keys = modulo['data_keys']
                        api_modulos_chave_primaria_insert = modulo['chave_primaria_insert']
                        # api_modulos_quantity = modulo['quantity']
                        # api_modulos_producao = modulo['producao']

                        funcao = globals().get(api_modulos_function_name)
                        if funcao:
                            dados = funcao(api_client_secret, f'{api_modulos_url}')
                        else:
                            print(f"Função {api_modulos_function_name} não encontrada.")

                        if dados:
                            # print(api_modulos_descricao)
                            # print(dados)
                            database.inserir_ou_atualizar_dados_json(dados, api_modulos_tabela, var_ie_cliente,
                                                            f'{api_modulos_chave_primaria_insert}', None, None)

                    # Fechar a conexão
                    cursor_aux1.close()
                    conn_aux1.close()