from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os
from fastapi import HTTPException
import asyncpg
from contextlib import asynccontextmanager

load_dotenv()

# Validação de variáveis de ambiente
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")

# Verifica se todas as variáveis de ambiente necessárias estão definidas
required_vars = {
    "PG_USER": PG_USER,
    "PG_PASSWORD": PG_PASSWORD,
    "PG_HOST": PG_HOST,
    "PG_PORT": PG_PORT,
    "PG_DATABASE": PG_DATABASE
}

missing_vars = [var for var, value in required_vars.items() if not value]
if missing_vars:
    raise ValueError(f"Variáveis de ambiente obrigatórias não definidas: {', '.join(missing_vars)}")

# Constrói a URL de conexão
DB_URL = f"postgresql+asyncpg://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"


#Cria a Engine
engine = create_async_engine(DB_URL, echo=True)

# Criação do SessionMaker
async_session = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,)

# Dependência para criar e usar sessões
async def get_db():
    async with async_session() as session:
        yield session

# Conexão simples com PostgreSQL
async def get_pg_connection():
    try:
        conn = await asyncpg.connect(
            user=PG_USER,    
            password=PG_PASSWORD,
            database=PG_DATABASE,
            host=PG_HOST,
            port=int(PG_PORT)
        )
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao conectar ao PostgreSQL: {str(e)}")

# Context manager para gerenciar conexões PostgreSQL automaticamente
@asynccontextmanager
async def pg_connection_manager():
    """Context manager para gerenciar conexões PostgreSQL automaticamente"""
    conn = None
    try:
        conn = await asyncpg.connect(
            user=PG_USER,    
            password=PG_PASSWORD,
            database=PG_DATABASE,
            host=PG_HOST,
            port=int(PG_PORT)
        )
        yield conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro na operação PostgreSQL: {str(e)}")
    finally:
        if conn:
            await conn.close()    
