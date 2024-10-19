import database
import psycopg2.extras
import requests
import json
from datetime import datetime
from datetime import timedelta

def consulta_nivel1_meta(api_token, base_url, api_date_preset, api_fun_level, api_breakdowns):
    params = {
        "access_token": api_token,
        "date_preset": api_date_preset,
        "level": api_fun_level,
        "breakdowns": api_breakdowns
    }

    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    all_data = []
    next_url = base_url

    while next_url:
        response = requests.get(next_url, params=params, headers=headers)

        if response.status_code != 200:
            print(f"Erro: {response.status_code}")
            break

        response_data = response.json()

        # Coletar dados da página atual
        if 'data' in response_data and len(response_data['data']) > 0:
            all_data.extend(response_data['data'])

        # Verificar se há uma próxima página usando o cursor
        next_url = response_data.get('paging', {}).get('next')
        # if next_url:
        #    print(f"Paginação para a próxima URL")

    if all_data:
        return all_data
    else:
        print("Nenhum dado encontrado.")
        return None

def main(var_ie_cliente, api_service_url, api_client_id, api_client_secret, api_codigo, act_, api_token):
    
    # Verificar validade do token
    token_url = api_service_url + f"/debug_token?input_token={api_token}&access_token={api_token}"
    debug_token = requests.get(token_url)

    # Verifica se o token é válido e atualiza se faltar 30 dias ou menos para expirar
    if (debug_token.status_code == 200):

        now = datetime.now()

        expires_at = debug_token.json().get("data").get("expires_at")
        expires_at_datetime = datetime.fromtimestamp(expires_at)

        diferenca = expires_at_datetime - now
        if (diferenca < timedelta(days=30)):
            url = api_service_url + f"/oauth/access_token?grant_type=fb_exchange_token&client_id={api_client_id}&client_secret={api_client_secret}&fb_exchange_token={api_token}"
            att_token = requests.get(url)
            api_token = att_token.json().get("access_token")
            # print(att_token.text)

            # Conectar ao banco de dados PostgreSQL
            conn = database.get_connection()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # Executar a função SQL para obter as configurações da API
            cursor.execute(
                'UPDATE public.ie_api_configuracao SET api_token = %s WHERE cliente_codigo = %s AND api_codigo = %s',
                (api_token, var_ie_cliente, api_codigo))
            conn.commit()

        payload = {
            "access_token": api_token
        }

        # Cabeçalhos para a solicitação do token
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }

        response = requests.get(token_url, data=json.dumps(payload), headers=headers)

        if (response.status_code == 200):

            # Conectar ao banco de dados PostgreSQL
            conn_aux1 = database.get_connection()
            cursor_aux1 = conn_aux1.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # Executar a função SQL para obter as configurações da API
            cursor_aux1.execute("SELECT * FROM public.get_api_modulo_by_codigo(%s)", (api_codigo,))
            api_modulos = cursor_aux1.fetchall()

            for modulo in api_modulos:
                api_modulos_descricao = modulo['descricao']
                api_modulos_tabela = modulo['tabela']
                api_modulos_function_name = modulo['function_name']
                api_modulos_url = modulo['url']
                api_modulos_chave_primaria_insert = modulo['chave_primaria_insert']
                api_modulos_funcao_parametro = modulo['funcao_parametro']
                api_modulos_nivel_consulta = modulo['nivel_consulta']
                api_fun_level = modulo['level']
                api_request_fields = modulo['fields']
                api_date_preset = modulo['date_preset']
                api_breakdowns = modulo['breakdowns']

                funcao = globals().get(api_modulos_function_name)
                if funcao:
                    if api_modulos_nivel_consulta == 1:

                        if (api_fun_level):

                            # Trata os levels retornados do banco de dados
                            array = api_fun_level.strip("[]").split(",")
                            array = [item.strip() for item in array]

                            for level in array:
                                print(level)
                                dados = funcao(api_token,
                                            f'{api_service_url}{api_modulos_url}?fields={api_request_fields}'.format(
                                                act_=act_), api_date_preset, level, api_breakdowns)

                                if dados:

                                    for item in dados:
                                        item['level'] = level
                                        if not item.get('ad_id'):
                                            item['ad_id'] = '0'
                                        if not item.get('adset_id'):
                                            item['adset_id'] = '0'

                                            # print(dados)
                                    database.inserir_ou_atualizar_dados_json(dados, api_modulos_tabela,
                                                                    var_ie_cliente,
                                                                    f'{api_modulos_chave_primaria_insert}',
                                                                    None, None)

                                else:
                                    print("Não tem dados")

                        else:

                            api_fun_level = ''
                            dados = funcao(api_token,
                                        f'{api_service_url}{api_modulos_url}?fields={api_request_fields}'.format(
                                            act_=act_), api_date_preset, api_fun_level, api_breakdowns)

                            if dados:

                                database.inserir_ou_atualizar_dados_json(dados, api_modulos_tabela, var_ie_cliente,
                                                                f'{api_modulos_chave_primaria_insert}',
                                                                None, None)

                            else:
                                print("Não tem dados")
        else:
            print("Erro com token META do cliente %s" % api_client_id)