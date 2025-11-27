import firebird.driver as fb
from fastapi import HTTPException
from contextlib import contextmanager

USER = "SYSDBA"
PASSWORD = "masterkey"

def get_firebird_connection(HOST: str, PORT: int, DATABASE: str):
    """Função para conectar ao Firebird"""
    try:
        return fb.connect(
            database=f"{HOST}/{PORT}:{DATABASE}",
            user=USER,
            password=PASSWORD,
            charset= "ISO8859_1"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao conectar ao Firebird: {str(e)}")

@contextmanager
def firebird_connection_manager(HOST: str, PORT: int, DATABASE: str):
    """Context manager para gerenciar conexões Firebird automaticamente"""
    conn = None
    cursor = None
    try:
        conn = get_firebird_connection(HOST, PORT, DATABASE)
        cursor = conn.cursor()
        yield conn, cursor
    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Erro na operação Firebird: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()