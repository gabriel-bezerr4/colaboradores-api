import os
import psycopg
from dotenv import load_dotenv
from typing import List, Optional
import requests
import pandas as pd
import io

from .models import ColaboradorDB, ColaboradorSheet, ColaboradorCompleto

load_dotenv()

def get_db_connection():
    """Estabelece uma conexão com o banco de dados PostgreSQL."""
    try:
        conn = psycopg.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        return conn
    except psycopg.OperationalError as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None

def get_colaboradores_from_db() -> List[ColaboradorDB]:
    """Busca colaboradores no banco de dados PostgreSQL."""
    conn = get_db_connection()
    if conn is None:
        return []

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    p.codigo, 
                    p.nome, 
                    p.cpfcnpj as cpf, 
                    p.sexo as genero
                FROM pessoas p 
                WHERE 
                    p.funcionario = true 
                    AND p.status = 'S'
                    AND p.nome !~* '(PRE VENDA|DIARISTA|CAIXA|TESTE)'
            """)
            rows = cur.fetchall()
            colaboradores = [
                ColaboradorDB(
                    codigo=row[0],
                    nome=row[1],
                    cpf=row[2],
                    genero=row[3]
                ) for row in rows
            ]
            return colaboradores
    except psycopg.Error as e:
        print(f"Erro ao buscar dados do banco de dados: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_colaboradores_from_google_sheets_csv(sheet_url: str) -> List[ColaboradorSheet]:
    """
    Lê dados de uma planilha do Google Sheets exportada como CSV via URL e aplica lógica de limpeza
    baseada no script M-query fornecido pelo usuário.
    """
    try:
        response = requests.get(sheet_url, allow_redirects=True)
        response.raise_for_status()  # Levanta um erro para códigos de status HTTP ruins (4xx ou 5xx)

        # Lê o CSV, pulando as 4 primeiras linhas conforme o script M-query.
        # A primeira linha após o skiprows será o cabeçalho.
        df = pd.read_csv(io.StringIO(response.text), skiprows=4)

        # Renomear colunas para corresponder ao modelo ColaboradorSheet
        # Mapeamento baseado no script M-query e nas colunas solicitadas:
        # 'Código' -> 'matricula'
        # 'Cargo' -> 'cargo_contabil'
        # 'Salário' -> 'salario'
        # 'Nº do C.P.F.' -> 'cpf_sheet' (para uso interno, não no modelo final se já tiver do DB)
        # 'Admissão' não está no script M-query, então será None.
        df = df.rename(columns={
            'Código': 'matricula',
            'Cargo': 'cargo_contabil',
            'Salário': 'salario',
            'Nº do C.P.F.': 'cpf_sheet' # Coluna extra para possível validação ou uso futuro
        })

        # Filtrar linhas onde 'matricula' (Código) e 'Nome' não são nulos e 'Nome' não é 'Nome'
        # O script M-query usa 'Nome', mas nosso modelo ColaboradorSheet não tem 'Nome'.
        # Vamos filtrar por 'matricula' não nula, que é a chave.
        df = df.dropna(subset=['matricula'])
        
        # Remover linhas onde 'Nome' é 'Nome' (se a coluna 'Nome' existir e for relevante)
        # O script M-query filtra 'Nome' <> 'Nome'. Se a coluna 'Nome' estiver presente no CSV após skiprows=4
        # e antes de renomear, podemos aplicar este filtro.
        # Para simplificar, vamos assumir que após o skiprows e dropna, os dados são válidos.

        # Tratar colunas mescladas: preencher valores NaN com o valor anterior válido
        # Isso é uma suposição comum para dados de planilhas com células mescladas
        # onde o valor da célula mesclada se aplica a todas as linhas que ela abrange.
        for col in ['matricula', 'cargo_contabil', 'salario']:
            if col in df.columns:
                df[col] = df[col].fillna(method='ffill')

        # Converte o DataFrame para uma lista de objetos ColaboradorSheet
        colaboradores_sheet = []
        for index, row in df.iterrows():
            try:
                # Garantir que 'matricula' seja string para consistência com a chave de união
                matricula_val = str(int(row['matricula'])) if pd.notna(row['matricula']) else None
                salario_val = float(str(row['salario']).replace('.', '').replace(',', '.')) if pd.notna(row['salario']) else None

                if matricula_val:
                    colaboradores_sheet.append(ColaboradorSheet(
                        matricula=matricula_val,
                        cargo_contabil=str(row['cargo_contabil']) if pd.notna(row['cargo_contabil']) else None,
                        salario=salario_val,
                        admissao=None # 'Admissão' não está no script M-query, então é None
                    ))
            except KeyError as ke:
                print(f"Coluna ausente no CSV após limpeza: {ke}. Verifique os nomes das colunas e o script M-query.")
                continue
            except ValueError as ve:
                print(f"Erro de conversão de tipo para a linha {index}: {ve}. Dados: {row.to_dict()}")
                continue

        return colaboradores_sheet
    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar a URL do Google Sheets: {e}")
        return []
    except pd.errors.EmptyDataError:
        print("O arquivo CSV está vazio ou o skiprows está incorreto.")
        return []
    except Exception as e:
        print(f"Ocorreu um erro inesperado ao ler o CSV do Google Sheets: {e}")
        return []

def combine_colaboradores_data(db_data: List[ColaboradorDB], sheet_data: List[ColaboradorSheet]) -> List[ColaboradorCompleto]:
    """
    Combina os dados de colaboradores do banco de dados com os dados da planilha.
    Assume que a 'matricula' da planilha corresponde ao 'codigo' do banco de dados.
    """
    sheet_data_map = {col.matricula: col for col in sheet_data}
    
    combined_data = []
    for db_colaborador in db_data:
        # Converte o codigo do DB para string para corresponder à matricula da planilha
        sheet_colaborador = sheet_data_map.get(str(db_colaborador.codigo))
        
        if sheet_colaborador:
            combined_data.append(ColaboradorCompleto(
                codigo=db_colaborador.codigo,
                nome=db_colaborador.nome,
                cpf=db_colaborador.cpf,
                genero=db_colaborador.genero,
                cargo_contabil=sheet_colaborador.cargo_contabil,
                salario=sheet_colaborador.salario,
                admissao=sheet_colaborador.admissao
            ))
        else:
            # Se não houver dados correspondentes na planilha, adiciona apenas os dados do banco
            combined_data.append(ColaboradorCompleto(
                codigo=db_colaborador.codigo,
                nome=db_colaborador.nome,
                cpf=db_colaborador.cpf,
                genero=db_colaborador.genero,
                cargo_contabil=None,
                salario=None,
                admissao=None
            ))
    return combined_data

