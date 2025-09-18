from json import loads, dumps
from app.db.conexaopg import get_db
import json
from app.models.auditModel import LoginAudit
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request, Response, Depends
from jose import jwt, JWTError, ExpiredSignatureError
from app.auth.auth import SECRET_KEY, ALGORITHM
from starlette.responses import JSONResponse, StreamingResponse
from typing import Any

class AuditoriaMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Obtém a sessão do banco
        db: AsyncSession = await get_db().__anext__()

        # Captura informações da requisição
        ip_address = request.client.host  # Captura o IP da Requisição
        user_agent = request.headers.get("User-Agent") # Obtém o User-Agent para saber o tipo do cliente

        token = request.headers.get("Authorization")
        usuario = None
        codEmpresa = None

        if token and token.startswith("Bearer "):
            try:
                token = token.split(" ")[1]
                payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
                usuario = payload.get("nomeusuario")
                codEmpresa = payload.get("empresa")
            except ExpiredSignatureError:
                payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM], options={"verify_exp": False})
                usuario = payload.get("nomeusuario")
                codEmpresa = payload.get("empresa")
                print("Token expirado")    
            except JWTError as e:
                print(f"Erro ao decodificar o token: {e}")

        metodo = request.method
        endpoint = str(request.url.path)
        params = dict(request.query_params)  # Query params da URL

        # Captura o body da requisição (caso exista)
        try:
            body_request = await request.json()
        except Exception:
            body_request = None  # Se não tiver body, define como None

        # Chama a API e captura a resposta
        response = await call_next(request)
        status_code = response.status_code

        # Captura o body da resposta corretamente
        response_body = b""
        async for chunk in response.body_iterator:
            response_body += chunk

        body_response = response_body.decode("utf-8")

        # Remove quebras de linha indesejadas e espaços extras
        body_response = body_response.replace("\\n", "").strip()

        # Se for JSON, convertemos para um dicionário
        try:
            body_response = json.loads(body_response)  # Se for JSON válido, converte para dict
        except (json.JSONDecodeError, TypeError):
            pass  # Se já for dict ou não for JSON, mantém como está

        # Captura o token do corpo da resposta se o endpoint for /login
        if endpoint == "/login":
            try:
                if isinstance(body_response, dict):  # Se for dict, pega o token diretamente
                    tokenRet = body_response.get("access_token")
                    if tokenRet:
                        payload = jwt.decode(tokenRet, SECRET_KEY, algorithms=[ALGORITHM])
                        usuario = payload.get("nomeusuario")
                        codEmpresa = payload.get("empresa")
            except JWTError as e:
                print(f"Erro ao decodificar o token retornado: {e}")

        # Cria o log de auditoria
        log = LoginAudit(
            usuario=usuario,
            codempresa=codEmpresa,
            metodo=metodo,
            endpoint=endpoint,
            params=params,
            body_request=body_request,
            body_response=body_response, 
            status_code=status_code,
            ip_address=ip_address,
            user_agent=user_agent
        )

        # Adiciona o log ao banco de dados
        db.add(log)
        await db.commit()

        # Ajuste no retorno da API (StreamingResponse)
        # Se response_body for um dicionário, convertemos para string JSON
        if isinstance(response_body, dict):  
            response_body = json.dumps(response_body, ensure_ascii=False)

         # Retorna a resposta original
        return StreamingResponse(
            iter([response_body]),  
            status_code=status_code,
            headers=dict(response.headers),
            media_type=response.media_type
        )
       