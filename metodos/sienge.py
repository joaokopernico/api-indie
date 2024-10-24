import base64
import json
import psycopg2.extras
import database
import requests

def fetch_consulta_nivel1_sienge(start_number, auth_token, base_url, fields, values):
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
    
    if fields != None:       
        i = 0 
        
        fields = fields[0].split(',')
        base_url = base_url + '?'
        for field in fields:
            
            if i > 0: 
                base_url = base_url + '&' + field + '=' + str(values[i])  
            else:                          
                base_url = base_url + field + '=' + str(values[i])
            i = i + 1  


    response = requests.get(base_url, headers=headers, params=params, timeout=10)

    if response.status_code == 200:
        return response.json()
    else:
        print('Erro ao consultar ' + base_url)
        # raise Exception(f"Erro na solicitação: {response.status_code}")

def consulta_nivel1_sienge(auth_token, base_url, data_keys=None, fields=None, values=None):
 
    # Normaliza data_keys para ser sempre uma lista
    if data_keys is None or data_keys == 'None':
        data_keys = []
    elif isinstance(data_keys, str):
        data_keys = [data_keys]
    elif not isinstance(data_keys, list):
        data_keys = list(data_keys)
    
    start_number = 0
    all_records = []

    while True:

        records_json = fetch_consulta_nivel1_sienge(str(start_number), auth_token, base_url, fields, values)

        if records_json:
            # Se records_json for uma string, converte para dicionário
            if isinstance(records_json, str):
                try:
                    records_json = json.loads(records_json)
                except json.JSONDecodeError as e:
                    print(f"Erro ao decodificar JSON: {e}")
                    return None

            if data_keys:
                # Itera sobre cada chave em data_keys e coleta os dados
                found_data = False
                for key in data_keys:
                    if key in records_json:
                        data = records_json[key]
                        if isinstance(data, list):
                            all_records.extend(data)
                        elif isinstance(data, dict):
                            all_records.append(data)
                        else:
                            print(f"Dados na chave '{key}' não são list ou dict.")
                        found_data = True
                if not found_data:
                    print("Nenhum dado encontrado para as chaves fornecidas, parando o loop.")
                    break
            else:
                # Se data_keys estiver vazio, assume que os dados estão em 'results'
                data = records_json.get('results', [])
                if not data:
                    print("Nenhum dado encontrado em 'results', parando o loop.")
                    break
                all_records.extend(data)
            
            # Lógica de paginação baseada em 'resultSetMetadata'
            metadata = records_json.get('resultSetMetadata', {})
            count = metadata.get('count', 0)
            limit = metadata.get('limit', 100)
            if count < limit:
                # Se a quantidade de registros for menor que o limite, não há mais páginas
                break
            start_number += limit
            print(f"Próxima página: {start_number}")
        else:
            print("Nenhum dado encontrado na consulta.")
            break

    # Retorna os registros coletados, se houver
    if all_records:
        return all_records
    else:
        print("Nenhum dado encontrado.")
        return None



def main(var_ie_cliente, api_client_id, api_client_secret, api_codigo, api_service_url):
    username = api_client_id
    password = api_client_secret

    token = f"{username}:{password}"

    print('to na sienge')
    
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
        api_request_fields = modulo['fields']

        funcao = globals().get(api_modulos_function_name)
        if funcao:
            if api_modulos_nivel_consulta == 1 or api_modulos_nivel_consulta is None:
                # print(api_modulos_url)
                dados = funcao(auth_token, f'{api_service_url}{api_modulos_url}',data_keys=[f'{api_modulos_data_keys}'], fields=[f'{api_request_fields}'])
                print(dados)
                if dados:
                    database.inserir_ou_atualizar_dados_json(dados, api_modulos_tabela, var_ie_cliente,
                                                    f'{api_modulos_chave_primaria_insert}', None, None)
            if api_modulos_nivel_consulta == 2:
                
                print('sou a api nivel 2 e eu to aqui')
                
                # Conectar ao banco de dados PostgreSQL
                conn_aux2 = database.get_connection()
                cursor_aux2 = conn_aux2.cursor(cursor_factory=psycopg2.extras.DictCursor)
                # Executar a função SQL para obter as configurações da API
                cursor_aux2.execute(f"SELECT * FROM {api_modulos_funcao_parametro}(%s)",
                                    (var_ie_cliente,))
                api_consulta_nivel2 = cursor_aux2.fetchall()
                

                # Iterar sobre as configurações de API retornadas
                for consulta_nivel2 in api_consulta_nivel2:
                    
                    print(api_request_fields, consulta_nivel2)
                    
                    funcao = globals().get(api_modulos_function_name)

                
                    dados = funcao(auth_token, f'{api_service_url}{api_modulos_url}',data_keys=[f'{api_modulos_data_keys}'], fields=[f'{api_request_fields}'],
                                   values=consulta_nivel2)
                    
                    if dados:
                        database.inserir_ou_atualizar_dados_json(dados, api_modulos_tabela, var_ie_cliente,
                                                        f'{api_modulos_chave_primaria_insert}', None, None)
                        
                # Fechar a conexão
                cursor_aux1.close()
                conn_aux1.close()

                # dados = funcao(access_token, f'{api_service_url}{api_modulos_url}',api_modulos_dias_consulta,data_keys=[f'{api_modulos_data_keys}'])
        else:
            print(f"Função {api_modulos_function_name} não encontrada.")
        # Fechar a conexão
        cursor_aux1.close()
        conn_aux1.close()
