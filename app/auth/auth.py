from datetime import datetime, timedelta
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
#from decouple import config

#Configurações Basicas
SECRET_KEY = "a2e4b0e1b9e3f8c7a9e1c5b4e2a3c0b1" #config("SECRET_KEY")
ALGORITHM = "HS256" #config("ALGORITHM")
ACCESS_TOKEN_EXPIRE_MINUTES = 1440 #int(config("ACCESS_TOKEN_EXPIRE_MINUTES"))

#Contexto de criptografia da Senha
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Recurso para autenticação com token (OAuth2)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Funções auxiliares
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def decode_access_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError as e:
        if "expired" in str(e):  # Verifica se o erro é de expiração
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token expirado",
                headers={"WWW-Authenticate": "Bearer"},
            )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
      
            detail="Token inválido",
            headers={"WWW-Authenticate": "Bearer"},
        )
    

