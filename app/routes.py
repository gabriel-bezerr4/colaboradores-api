from fastapi import APIRouter, HTTPException
from typing import List, Optional
from .models import ColaboradorDB, ColaboradorSheet
from .services import get_colaboradores_from_db, get_colaboradores_from_google_sheets_csv

router = APIRouter()

@router.get("/colaboradores/banco", response_model=List[ColaboradorDB])
async def read_colaboradores_from_db():
    """Retorna uma lista de colaboradores obtidos diretamente do banco de dados."""
    try:
        colaboradores = get_colaboradores_from_db()
        if not colaboradores:
            raise HTTPException(status_code=500, detail="Não foi possível obter colaboradores do banco de dados. Verifique a conexão e as credenciais.")
        return colaboradores
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ocorreu um erro inesperado ao buscar dados do banco: {e}")

@router.get("/colaboradores/sheets", response_model=List[ColaboradorSheet])
async def read_colaboradores_from_sheets(sheet_url: str):
    """Retorna uma lista de colaboradores obtidos de uma planilha do Google Sheets via CSV público."""
    if not sheet_url:
        raise HTTPException(status_code=400, detail="A URL da planilha não foi fornecida.")
    
    try:
        colaboradores = get_colaboradores_from_google_sheets_csv(sheet_url)
        if not colaboradores:
            raise HTTPException(status_code=500, detail="Não foi possível obter colaboradores da planilha. Verifique a URL e o formato do CSV.")
        return colaboradores
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ocorreu um erro inesperado ao buscar dados da planilha: {e}")




from .models import ColaboradorCompleto
from .services import combine_colaboradores_data

@router.get("/colaboradores/completo", response_model=List[ColaboradorCompleto])
async def get_colaboradores_completos(sheet_url: str):
    """Retorna uma lista de colaboradores combinando dados do banco de dados e de uma planilha do Google Sheets."""
    if not sheet_url:
        raise HTTPException(status_code=400, detail="A URL da planilha não foi fornecida.")

    db_data = get_colaboradores_from_db()
    if not db_data:
        raise HTTPException(status_code=500, detail="Não foi possível obter colaboradores do banco de dados.")

    sheet_data = get_colaboradores_from_google_sheets_csv(sheet_url)
    if not sheet_data:
        # Se não houver dados da planilha, ainda podemos retornar os dados do banco, mas sem as informações complementares
        # Ou levantar um erro, dependendo da regra de negócio. Por enquanto, vamos levantar um erro.
        raise HTTPException(status_code=500, detail="Não foi possível obter colaboradores da planilha. Verifique a URL e o formato do CSV.")

    combined_data = combine_colaboradores_data(db_data, sheet_data)
    return combined_data

