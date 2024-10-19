import psycopg2
import psycopg2.extras
import json 
from dotenv import load_dotenv
import os

# Carrega as variáveis do arquivo .env
load_dotenv()

# Acessa as variáveis
conn_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST')
}


def get_connection():
    conn = psycopg2.connect(**conn_params)
    return conn


def inserir_ou_atualizar_dados_json(json_data, nome_tabela, ie_cliente, campos_chave_primaria, campo_atribuir,
                                    campo_atribuir_valor):
    

    # Conectar ao banco de dados PostgreSQL
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Verificar e converter json_data para string JSON se necessário
        if isinstance(json_data, (dict, list)):
            json_data = json.dumps(json_data)

        # Ler o conteúdo do JSON
        dados = json.loads(json_data)

        # Verificar se 'dados' é uma lista e iterar sobre cada objeto
        if isinstance(dados, list):
            for item in dados:
                inserir_ou_atualizar_registro(cursor, item, nome_tabela, ie_cliente, campos_chave_primaria,
                                              campo_atribuir, campo_atribuir_valor)
        else:
            inserir_ou_atualizar_registro(cursor, dados, nome_tabela, ie_cliente, campos_chave_primaria, campo_atribuir,
                                          campo_atribuir_valor)

        # Confirmar as transações
        conn.commit()
        print(f"Dados inseridos ou atualizados com sucesso na tabela {nome_tabela}.")

    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()
        
        
def inserir_ou_atualizar_registro(cursor, dados, nome_tabela, ie_cliente, campos_chave_primaria, campo_atribuir,
                                  campo_atribuir_valor):
    try:
        # Verificar se campos_chave_primaria é uma lista ou string e processar adequadamente
        if isinstance(campos_chave_primaria, list):
            campos_chave_primaria = ', '.join(campos_chave_primaria)
        elif not isinstance(campos_chave_primaria, str):
            raise ValueError("campos_chave_primaria deve ser uma lista ou string.")

        campos_chave_primaria = campos_chave_primaria.strip('[]').replace("'", "").split(', ')

        # Preparar os dados para inserção
        campos_json = list(dados.keys())
        valores_json = list(dados.values())

        # Convertendo dicionários para strings JSON
        for i, valor in enumerate(valores_json):
            if isinstance(valor, dict) or isinstance(valor, list):
                valores_json[i] = json.dumps(valor)  # Converte o dicionário ou lista para uma string JSON
            # Convertendo strings para UTF-8
            elif isinstance(valor, str):
                valores_json[i] = convert_to_utf8(valor)

        # Adicionar o campo ie_cliente
        campos_json.insert(0, 'ie_cliente')
        valores_json.insert(0, convert_to_utf8(ie_cliente))

        if campo_atribuir is not None:
            campos_json.insert(0, campo_atribuir)
            valores_json.insert(0, convert_to_utf8(campo_atribuir_valor))

        # Construir a instrução SQL dinamicamente para inserção e atualização
        campos_str = ', '.join(campos_json)
        valores_placeholders = ', '.join(['%s'] * len(valores_json))
        update_set = ', '.join(
            [f"{campo} = EXCLUDED.{campo}" for campo in campos_json if campo not in campos_chave_primaria])
        conflict_target = ', '.join(campos_chave_primaria)

        query = f"""
        INSERT INTO {nome_tabela} ({campos_str}) 
        VALUES ({valores_placeholders})
        ON CONFLICT ({conflict_target}) 
        DO UPDATE SET {update_set}
        """

        # Executar a inserção ou atualização
        cursor.execute(query, valores_json)
    except Exception as e:
        print(f"Erro ao inserir ou atualizar registro: {e}")
        raise

def inserir_ou_atualizar_registro_bkp(cursor, dados, nome_tabela, ie_cliente, campos_chave_primaria, campo_atribuir,
                                      campo_atribuir_valor):
    try:
        # Verificar se campos_chave_primaria é uma lista ou string e processar adequadamente
        if isinstance(campos_chave_primaria, list):
            campos_chave_primaria = ', '.join(campos_chave_primaria)
        elif not isinstance(campos_chave_primaria, str):
            raise ValueError("campos_chave_primaria deve ser uma lista ou string.")

        campos_chave_primaria = campos_chave_primaria.strip('[]').replace("'", "").split(', ')

        # Preparar os dados para inserção
        campos_json = list(dados.keys())
        valores_json = list(dados.values())

        # Convertendo dicionários para strings JSON
        for i, valor in enumerate(valores_json):
            if isinstance(valor, dict) or isinstance(valor, list):
                valores_json[i] = json.dumps(valor)  # Converte o dicionário ou lista para uma string JSON

        # Adicionar o campo ie_cliente
        campos_json.insert(0, 'ie_cliente')
        valores_json.insert(0, ie_cliente)

        if campo_atribuir is not None:
            campos_json.insert(0, campo_atribuir)
            valores_json.insert(0, campo_atribuir_valor)

        # Construir a instrução SQL dinamicamente para inserção e atualização
        campos_str = ', '.join(campos_json)
        valores_placeholders = ', '.join(['%s'] * len(valores_json))
        update_set = ', '.join(
            [f"{campo} = EXCLUDED.{campo}" for campo in campos_json if campo not in campos_chave_primaria])
        conflict_target = ', '.join(campos_chave_primaria)

        query = f"""
        INSERT INTO {nome_tabela} ({campos_str}) 
        VALUES ({valores_placeholders})
        ON CONFLICT ({conflict_target}) 
        DO UPDATE SET {update_set}
        """

        # Executar a inserção ou atualização
        cursor.execute(query, valores_json)
    except Exception as e:
        print(f"Erro ao inserir ou atualizar registro: {e}")
        raise        
    
    
def convert_to_utf8(input_data):
    try:
        # Converte para UTF-8 se for uma string
        if isinstance(input_data, str):
            return input_data.encode('utf-8').decode('utf-8')
        elif isinstance(input_data, bytes):
            return input_data.decode('utf-8')
        else:
            return input_data
    except (UnicodeDecodeError, UnicodeEncodeError) as e:
        raise ValueError(f"Erro ao converter dados para UTF-8: {e}")