from fastapi import Request, APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.db.conexaopg import get_db
from app.models.usurioModel import tbusuario
from app.schemas.usuarioSchemas import UsuarioLogin, UsuarioLoginRet
from app.auth.auth import verify_password, create_access_token


router = APIRouter()

@router.post("/login", tags=["Login"], response_model=UsuarioLoginRet, status_code=status.HTTP_202_ACCEPTED)
async def login(request: Request ,usuario: UsuarioLogin, db: AsyncSession = Depends(get_db)):

    # Busca o usuário pelo nome de usuário
    print (usuario.cpfusuario)
    result = await db.execute(select(tbusuario).where(tbusuario.cpfusuario == usuario.cpfusuario))
    user = result.scalars().first()

    if not user or not verify_password(usuario.senhausuario, user.senhausuario):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuário ou senha incorretos")

    # Gera o token JWT
    access_token = create_access_token(data={
        "sub": str(user.codempresa),
        "nomeusuario": user.nomeusuario,
        "codusuario": str(user.codusuario),
        "ativo": user.usuarioativo,
        "empresa": str(user.codempresa),
        "cpfUsuario": user.cpfusuario,
        })
    return {"access_token": access_token, "token_type": "bearer"}