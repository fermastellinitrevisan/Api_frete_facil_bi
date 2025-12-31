from fastapi import FastAPI, Depends, APIRouter, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from sqlalchemy.future import select
from app.db.conexaopg import get_db
from app.models.usurioModel import tbusuario
from app.schemas.usuarioSchemas import UsuarioRetorno, UsuarioCadastro, UsuarioAtualizacao
from app.auth.auth import get_password_hash, decode_access_token 
from fastapi.security import OAuth2PasswordBearer
from app.utils.util import ValidaCPF



router = APIRouter()

# Recurso para autenticação com token (OAuth2)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

@router.get("/users", tags=["Usuários"], response_model=list[UsuarioRetorno])
async def get_usuarios(token: str = Depends(oauth2_scheme), db: AsyncSession = Depends(get_db)):

    try:
        payload = decode_access_token(token)
    except HTTPException as e:
        raise e

    codusuario: str = payload.get("codusuario")
    if codusuario is None:
        raise HTTPException(status_code=401, detail="Token inválido")
    
    # Busca o usuário no banco
    result = await db.execute(select(tbusuario))
    usuario = result.scalars().all()
    return usuario

@router.get("/users/{cpfusuario}", tags=["Usuários"], response_model=UsuarioRetorno)
async def get_usuario(cpfusuario:str, token: str = Depends(oauth2_scheme), db: AsyncSession = Depends(get_db)):
    try:
        payload = decode_access_token(token)
    except HTTPException as e:
        raise e

    codusuarioToken: str = payload.get("codusuario")
    if codusuarioToken is None:
        raise HTTPException(status_code=401, detail="Token inválido")

    result = await db.execute(select(tbusuario).where(tbusuario.cpfusuario == cpfusuario))
    usuario = result.scalars().first()
    if usuario is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuário não encontrado")
    return usuario

@router.post("/users", tags=["Usuários"], response_model=UsuarioRetorno, status_code=status.HTTP_201_CREATED)
async def create_usuario(usuario: UsuarioCadastro, token: str = Depends(oauth2_scheme), 
                        db: AsyncSession = Depends(get_db)):

    validador = ValidaCPF(usuario.cpfusuario)
    if not validador.validar_cpf():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="CPF Inválido")

    # Decodifica o token e verifica o usuário autenticado
    try:
        payload = decode_access_token(token)
    except HTTPException as e:
        raise e

    codusuario: str = payload.get("codusuario")
    if codusuario is None:
        raise HTTPException(status_code=401, detail="Token inválido")

    # Verifica se o usuário já existe no banco
    result = await db.execute(select(tbusuario).where(tbusuario.cpfusuario == usuario.cpfusuario))
    usuario_existente = result.scalars().first()

    if usuario_existente:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Usuário já cadastrado!")

    # Hash da senha antes de salvar
    hashed_password = get_password_hash(usuario.senhausuario)

    # Criando novo usuário
    new_usuario = tbusuario(
        nomeusuario=usuario.nomeusuario,
        senhausuario=hashed_password,
        codempresa=usuario.codempresa,
        usuarioativo=usuario.usuarioativo,
        cpfusuario=usuario.cpfusuario,
        emailusuario=usuario.emailusuario
    )

    db.add(new_usuario)

    try:
        await db.commit()
        await db.refresh(new_usuario)
    except IntegrityError:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Usuário já existe")

    return new_usuario

@router.put("/users/{cpfusuario}", tags=["Usuários"], response_model=UsuarioRetorno)
async def update_usuario(cpfusuario: str, user_update: UsuarioAtualizacao, token: str = Depends(oauth2_scheme), db: AsyncSession = Depends(get_db)):
    try:
        payload = decode_access_token(token)
    except HTTPException as e:
        print(f"Erro ao decodificar o token: {e.detail}")
        raise e

    codusuario: str = payload.get("codusuario")
    if codusuario is None:
        raise HTTPException(status_code=401, detail="Token inválido")

    # Busca o usuário no banco de dados
    result = await db.execute(select(tbusuario).where(tbusuario.cpfusuario == cpfusuario))
    usuario = result.scalars().first()

    if usuario is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuário não encontrado")

    # Atualiza os campos do usuário
    for var, value in user_update.dict(exclude_unset=True).items():
        if var == "senhausuario":
            # Hash da senha antes de atualizar
            value = get_password_hash(value)
        setattr(usuario, var, value)

    db.add(usuario)  # Adiciona o usuário atualizado ao banco de dados

    try:
        await db.commit()
        await db.refresh(usuario)
    except IntegrityError as e:
        print(f"Erro de integridade ao atualizar o usuário: {e}")
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Erro ao atualizar o usuário")
    except Exception as e:
        print(f"Erro inesperado ao atualizar o usuário: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro inesperado ao atualizar o usuário")

    return usuario

@router.delete("/users/{cpfusuario}", tags=["Usuários"], status_code=status.HTTP_204_NO_CONTENT)
async def delete_usuario(cpfusuario:str, token: str = Depends(oauth2_scheme), db: AsyncSession = Depends(get_db)):
    try:
        payload = decode_access_token(token)
    except HTTPException as e:
        raise e

    codusuario: str = payload.get("codusuario")
    if codusuario is None:
        raise HTTPException(status_code=401, detail="Token inválido")
    
    #Busca o Usuario pelo ID
    resul = await db.execute(select(tbusuario).where(tbusuario.cpfusuario == cpfusuario))
    user = resul.scalars().first()

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuário não encontrado")
    else:
        await db.delete(user)
        await db.commit()
        return {"detail": "Usuário excluído com sucesso"} 