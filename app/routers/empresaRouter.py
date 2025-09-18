from fastapi import FastAPI, Depends, APIRouter, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from sqlalchemy.future import select
from app.db.conexaopg import get_db
from app.models.empresaModel import tbempresa
from app.schemas.empresaSchemas import EmpresaRetorno, EmpresaCadastro
from fastapi.security import OAuth2PasswordBearer
from app.auth.auth import decode_access_token

router = APIRouter()

# Recurso para autenticação com token (OAuth2)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

@router.get("/empresas", tags=["Empresas"], response_model=list[EmpresaRetorno])
async def get_empresas(token: str = Depends(oauth2_scheme), 
                       db: AsyncSession = Depends(get_db)):
    try:
        payload = decode_access_token(token)
    except HTTPException as e:
        raise e

    codusuario: str = payload.get("codusuario")
    if codusuario is None:
        raise HTTPException(status_code=401, detail="Token inválido")
    result = await db.execute(select(tbempresa))
    empresa = result.scalars().all()

    # Ajusta o caminho do banco de dados para retornar com apenas uma barra invertida
    if hasattr(empresa, 'caminhobd'):
        empresa.caminhobd = empresa.caminhobd.replace('\\\\', '\\')

    return empresa

@router.get("/empresas/{codempresa}", tags=["Empresas"], response_model=EmpresaRetorno)
async def get_empresa(codempresa: int, token: str = Depends(oauth2_scheme),  db: AsyncSession = Depends(get_db)):
    try:
        payload = decode_access_token(token)
    except HTTPException as e:
        raise e

    codusuario: str = payload.get("codusuario")
    if codusuario is None:
        raise HTTPException(status_code=401, detail="Token inválido")
    result = await db.execute(select(tbempresa).where(tbempresa.codempresa == codempresa))
    empresa = result.scalars().first()
    if empresa is None:
        raise HTTPException(status_code=404, detail="Empresa não encontrada")
    
    # Ajusta o caminho do banco de dados para retornar com apenas uma barra invertida
    if hasattr(empresa, 'caminhobd'):
        empresa.caminhobd = empresa.caminhobd.replace('\\\\', '\\')

    return empresa

@router.post("/empresas", tags=["Empresas"], response_model=EmpresaRetorno)
async def post_empresa(empresa: EmpresaCadastro, 
                       token: str = Depends(oauth2_scheme), 
                       db: AsyncSession = Depends(get_db)):
    try:
        payload = decode_access_token(token)
    except HTTPException as e:
        raise e

    codusuario: str = payload.get("codusuario")
    if codusuario is None:
        raise HTTPException(status_code=401, detail="Token inválido")
    try:
        new_empresa = tbempresa(**empresa.dict())
        empresacad = await db.execute(select(tbempresa).where(tbempresa.cnpjcpf == new_empresa.cnpjcpf))

        if empresacad.scalars().first():
            raise HTTPException(status_code=400, detail="Empresa já cadastrada")

        db.add(new_empresa)
        await db.commit()
        return new_empresa
    except IntegrityError:
        raise HTTPException(status_code=400, detail="Empresa já cadastrada")
    

@router.put("/empresas/{codempresa}", tags=["Empresas"], response_model=EmpresaRetorno)
async def put_empresa(codempresa: int, empresa: EmpresaCadastro, token: str = Depends(oauth2_scheme), db: AsyncSession = Depends(get_db)):
    try:
        payload = decode_access_token(token)
    except HTTPException as e:
        raise e

    codusuario: str = payload.get("codusuario")
    if codusuario is None:
        raise HTTPException(status_code=401, detail="Token inválido")
    
    empresaatual = await db.get(tbempresa, codempresa)
    if empresaatual is None:
        raise HTTPException(status_code=404, detail="Empresa não encontrada")
    for var, value in empresa.dict().items():
        setattr(empresaatual, var, value)
    await db.commit()
    return empresaatual

@router.delete("/empresas/{codempresa}", tags=["Empresas"], status_code=status.HTTP_204_NO_CONTENT)
async def delete_empresa(codempresa: int, token: str = Depends(oauth2_scheme), db: AsyncSession = Depends(get_db)):
    try:
        payload = decode_access_token(token)
    except HTTPException as e:
        raise e

    codusuario: str = payload.get("codusuario")
    if codusuario is None:
        raise HTTPException(status_code=401, detail="Token inválido")
    result = await db.execute(select(tbempresa).where(tbempresa.codempresa == codempresa))
    empresa = result.scalars().first()
    if empresa is None:
        raise HTTPException(status_code=404, detail="Empresa não encontrada")
    else:
        await db.delete(empresa)
        await db.commit()
        return None