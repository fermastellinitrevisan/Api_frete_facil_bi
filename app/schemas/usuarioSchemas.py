from pydantic import BaseModel
from typing import Optional

#Modelo de Retorno de Usuarios Cadstrados (Get/Post/Put)
class UsuarioRetorno(BaseModel):
    codusuario: int = None
    nomeusuario: str = None
    codempresa: int = None    
    usuarioativo: str = None
    cpfusuario: str = None
    emailusuario: str = None
    class Config:
        orm_mode = True

#Modelo de Body para Cadastro de Usuarios (Post)
class UsuarioCadastro(BaseModel):
    #codusuario: int
    nomeusuario: str
    senhausuario: str
    codempresa: int
    usuarioativo: str 
    cpfusuario: str
    emailusuario: str    

    class Config:
        orm_mode = True 

#Modelo de Body para Atualização de Usuarios (Put)
class UsuarioAtualizacao(BaseModel):   
  #  codusuario: int = None
    nomeusuario: str = None
    senhausuario: str = None
    codempresa: int = None    
    usuarioativo: str = None 
    emailusuario: str = None      

#Modelo de Body para Login de Usuarios
class UsuarioLoginRet(BaseModel):
    access_token: str  

#modelo de Body para Login de Usuarios
class UsuarioLogin(BaseModel):
    username:str
    # cpfusuario: str
    password: str
    class Config:
        orm_mode = True       



