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

    print(usuario.username)
    print(usuario.password)

    # Busca o usuário pelo nome de usuário
    print (usuario.username)
    result = await db.execute(select(tbusuario).where(tbusuario.emailusuario == usuario.username))
    user = result.scalars().first()

    if not user or not verify_password(usuario.password, user.senhausuario):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuário ou senha incorretos")

    # Gera o token JWT
    access_token = create_access_token(data={
        "sub": str(user.codempresa),
        "nomeusuario": user.nomeusuario,
        "codusuario": str(user.codusuario),
        "ativo": user.usuarioativo,
        "empresa": str(user.codempresa),
        "email": user.emailusuario,
        })
    
    return {"access_token": access_token, "token_type": "bearer"}