import uvicorn
from fastapi import FastAPI
from app.routers import usuarioRouter
from app.routers import empresaRouter
from app.routers import loginRouter
from app.middleware.auditoria import AuditoriaMiddleware
from app.routers import BIRouter

app = FastAPI(
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    title="API de integração BI-FreteFácil",
    description="API para integração do BI-FreteFácil Softcenter",
    version="1.0.0",
    root_path="/sftlogin",  # Define o prefixo base para toda a API
)


app.add_middleware(AuditoriaMiddleware)
app.include_router(loginRouter.router)
app.include_router(BIRouter.router)


#if __name__ == "__main__":
# uvicorn  app.main:app --host 0.0.0.0 --port 2929 --reload