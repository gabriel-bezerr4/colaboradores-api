from fastapi import FastAPI
from . import routes

app = FastAPI(
    title="API de Cadastro de Colaboradores",
    description="Uma API para gerenciar informações de colaboradores, com dados do banco de dados e, futuramente, do Google Sheets.",
    version="1.0.0"
)

# Inclui as rotas definidas no arquivo routes.py
app.include_router(routes.router)

@app.get("/")
async def root():
    """Endpoint raiz para verificar se a API está funcionando."""
    return {"message": "Bem-vindo à API de Cadastro de Colaboradores!"}

