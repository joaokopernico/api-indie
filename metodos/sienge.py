import base64
import json
import psycopg2.extras
import database
import requests

def fetch_consulta_nivel1_sienge(start_number, auth_token, base_url):
    headers = {
        'Authorization': f'Basic {auth_token}',
        'Content-Type': 'application/json'
    }

    params = {
        "limit": "100",
        "offset": start_number
        #"departmentId" : "1"
        #"customerId" : "2"
    }

    response = requests.get(base_url, headers=headers, params=params, timeout=10)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Erro na solicitação: {response.status_code}")

def consulta_nivel1_sienge(auth_token, base_url, data_keys=None):
    if data_keys is None:
        data_keys = []

    if data_keys == 'None':
        data_keys = []
    start_number = 0
    all_records = []

    while True:
        records_json = fetch_consulta_nivel1_sienge(str(start_number), auth_token, base_url)

        # Converte a string JSON para um dicionário
        if isinstance(records_json, str):
            records_json = json.loads(records_json)

        print(data_keys)
        if not data_keys == []:
            if not records_json.get(data_keys):  # Se a lista 'results' estiver vazia, interrompe o loop
                print("Nenhum dado encontrado, parando o loop.")
                break

        if data_keys == []:
            all_records.extend(records_json)  # Adiciona os novos resultados à lista
        else:
            all_records.extend(records_json[data_keys])  # Adiciona os novos resultados à lista

        if data_keys == []:
           break

        start_number += 100
        print(start_number)


    # Processa os dados de acordo com data_keys
    items = []
    for key in data_keys:
        items = all_records.get(key, []) if isinstance(all_records, dict) else []
        if items:
            break

    # Se nenhuma chave fornecida retornar itens, usar todo o conteúdo do JSON
    if not items:
        if isinstance(all_records, dict):
            items = [all_records]
        elif isinstance(all_records, list):
            items = all_records

    if items:
        return items
    else:
        print("Nenhum dado encontrado.")
        return None


def main(var_ie_cliente, api_client_id, api_client_secret, api_codigo, api_service_url, access_token):
    username = api_client_id
    password = api_client_secret

    token = f"{username}:{password}"

    # Codifique em Base64
    auth_token = base64.b64encode(token.encode()).decode()

    # Conectar ao banco de dados PostgreSQL
    conn_aux1 = database.get_connection()
    cursor_aux1 = conn_aux1.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Executar a função SQL para obter as configurações da API
    cursor_aux1.execute("SELECT * FROM public.get_api_modulo_by_codigo(%s)", (api_codigo,))
    api_modulos = cursor_aux1.fetchall()

    # Iterar sobre as configurações de API retornadas
    for modulo in api_modulos:
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
        api_modulos_dias_consulta = modulo['dias_consulta']
        api_modulos_funcao_parametro = modulo['funcao_parametro']
        api_modulos_nivel_consulta = modulo['nivel_consulta']

        funcao = globals().get(api_modulos_function_name)
        if funcao:
            if api_modulos_nivel_consulta == 1 or api_modulos_nivel_consulta is None:
                print(api_modulos_url)
                dados = funcao(auth_token, f'{api_service_url}{api_modulos_url}',data_keys=[f'{api_modulos_data_keys}'])
                print(dados)
                if dados:
                    database.inserir_ou_atualizar_dados_json(dados, api_modulos_tabela, var_ie_cliente,
                                                    f'{api_modulos_chave_primaria_insert}', None, None)
            if api_modulos_nivel_consulta == 2:
                # Conectar ao banco de dados PostgreSQL
                conn_aux2 = database.get_connection()
                cursor_aux2 = conn_aux2.cursor(cursor_factory=psycopg2.extras.DictCursor)
                # Executar a função SQL para obter as configurações da API
                cursor_aux2.execute(f"SELECT * FROM {api_modulos_funcao_parametro}(%s)",
                                    (var_ie_cliente,))
                api_consulta_nivel2 = cursor_aux2.fetchall()

                # Iterar sobre as configurações de API retornadas
                for consulta_nivel2 in api_consulta_nivel2:
                    print(api_modulos)
                    api_consulta_nivel2_campotabela = consulta_nivel2['campo_tabela']
                    api_consulta_nivel2_campo_consulta = consulta_nivel2['campo_consulta']
                    api_consulta_nivel2_valor = consulta_nivel2['valor']
                    api_consulta_nivel2_url = consulta_nivel2['url']

                    funcao = globals().get(api_modulos_function_name)
                    if api_consulta_nivel2_url is None:
                        var_url = f'{api_service_url}{api_modulos_url}'
                    else:
                        var_url = api_consulta_nivel2_url

                    var_url = var_url.format(
                        api_consulta_nivel2_campo_consulta=api_consulta_nivel2_campo_consulta,
                        api_consulta_nivel2_valor=api_consulta_nivel2_valor
                    )

                    dados = funcao(access_token, var_url,
                                api_modulos_dias_consulta, data_keys=[f'{api_modulos_data_keys}'])
                    # print(dados)
                    if dados:
                        database.inserir_ou_atualizar_dados_json(dados, api_modulos_tabela, var_ie_cliente,
                                                        f'{api_modulos_chave_primaria_insert}',
                                                        api_consulta_nivel2_campotabela,
                                                        api_consulta_nivel2_valor)
                # Fechar a conexão
                cursor_aux1.close()
                conn_aux1.close()

                # dados = funcao(access_token, f'{api_service_url}{api_modulos_url}',api_modulos_dias_consulta,data_keys=[f'{api_modulos_data_keys}'])
        else:
            print(f"Função {api_modulos_function_name} não encontrada.")
        # Fechar a conexão
        cursor_aux1.close()
        conn_aux1.close()
