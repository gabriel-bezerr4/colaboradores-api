from pydantic import BaseModel
from typing import Optional

class ColaboradorDB(BaseModel):
    codigo: int
    nome: str
    cpf: str
    genero: str

class ColaboradorSheet(BaseModel):
    matricula: str # Corresponde ao 'Código' do CSV
    cargo_contabil: Optional[str] = None # Corresponde ao 'Cargo' do CSV
    salario: Optional[float] = None # Corresponde ao 'Salário' do CSV
    admissao: Optional[str] = None # Não presente no M-query, mantido como opcional

class ColaboradorCompleto(BaseModel):
    codigo: int
    nome: str
    cpf: str
    genero: str
    cargo_contabil: Optional[str] = None
    salario: Optional[float] = None
    admissao: Optional[str] = None

