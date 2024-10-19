import requests
import json
import psycopg2.extras
import database
from datetime import time, timedelta
import datetime


def consulta_nivel1_refresh(access_token, base_url, dias_consulta, data_keys=None):
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

    if dias_consulta is not None:
        # Calcular as datas de início e término (últimos 45 dias a partir de hoje)
        end_date = datetime.now()
        start_date = end_date - timedelta(dias_consulta)

        # Converter datas para o formato necessário (yyyy-mm-dd)
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        params = {
            "start_date": start_date_str,
            "end_date": end_date_str
        }

    all_items = []
    page = 1

    if data_keys is None:
        data_keys = []

    # Tentar com paginação
    while True:
        #url = f"{base_url}?page={page}"
        #url = f"{base_url}&page={page}"
        if "?" in base_url:
            # Caso já exista um "?", adiciona com "&"
            url = f"{base_url}&page={page}"
        else:
            # Caso não exista, adiciona com "?"
            url = f"{base_url}?page={page}"

        if dias_consulta is None:
            response = requests.get(url, headers=headers)
        else:
            response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()

            # Iterar sobre as chaves fornecidas para encontrar os itens
            items = []
            for key in data_keys:
                items = data.get(key, []) if isinstance(data, dict) else []
                if items:
                    break

            # Se nenhuma chave fornecida retornar itens, usar todo o conteúdo do JSON
            if not items:
                if isinstance(data, dict):
                    items = [data]
                elif isinstance(data, list):
                    items = data

            # Adicionar os itens da página atual à lista de todos os itens
            all_items.extend(items)

            # Verificar se todos os itens foram recuperados ou se não há mais páginas
            if not items or (isinstance(data, dict) and len(all_items) >= data.get('total', 0)):
                break
            # Incrementar a página para obter os próximos itens
            page += 1

        else:
            print(f"Falha ao obter URL: {url}")
            print(response.text)
            break

    # Se não houve retorno com paginação, tentar sem paginação
    if not all_items:
        response = requests.get(base_url, headers=headers)

        if response.status_code == 200:
            data = response.json()

            items = []
            for key in data_keys:
                items = data.get(key, []) if isinstance(data, dict) else []
                if items:
                    break

            if not items:
                if isinstance(data, dict):
                    items = [data]
                elif isinstance(data, list):
                    items = data

            all_items.extend(items)
        else:
            print(f"Falha ao obter URL: {base_url}")
            print(response.text)

    return all_items

def main(var_ie_cliente, api_client_id, api_client_secret, api_refresh_token, api_service_url, api_codigo):
    
    payload = {
                        "client_id": api_client_id,
                        "client_secret": api_client_secret,
                        "refresh_token": api_refresh_token
                    }
    token_url = api_service_url + "/auth/token"

    # Cabeçalhos para a solicitação do token
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    # Fazer a solicitação para obter o token de acesso
    response = requests.post(token_url, data=json.dumps(payload), headers=headers)

    # Verificar se a solicitação foi bem-sucedida
    if response.status_code == 200:
        # Extrair o token de acesso da resposta
        access_token = response.json().get("access_token")
        print(f"Access Token: {access_token}")

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
                if api_modulos_nivel_consulta == 1:
                    dados = funcao(access_token, f'{api_service_url}{api_modulos_url}',
                                api_modulos_dias_consulta, data_keys=[f'{api_modulos_data_keys}'])
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
                        time.sleep(0.5)
                    # Fechar a conexão
                    cursor_aux1.close()
                    conn_aux1.close()

            else:
                print(f"Função {api_modulos_function_name} não encontrada.")
        # Fechar a conexão
        cursor_aux1.close()
        conn_aux1.close()
    else:
        print(
            f"Falha ao obter o token de acesso para API Código {api_codigo}. Status Code: {response.status_code}")
        print(response.text)
        print('REFRESH')