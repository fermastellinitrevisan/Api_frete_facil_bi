FROM python:3.13.1-slim

WORKDIR /app

# Instalar dependências do sistema e Firebird 3
RUN apt-get update && apt-get install -y --no-install-recommends \
     firebird3.0-server firebird3.0-utils firebird3.0-common \
     && apt-get clean \
     && rm -rf /var/lib/apt/lists/*

# Configurar o Firebird (opcional, se necessário)
RUN sed -i 's/#DatabaseAccess = None/DatabaseAccess = Full/' /etc/firebird/3.0/firebird.conf

COPY requeriments.txt .

RUN pip install --no-cache-dir -r requeriments.txt

COPY . . 

EXPOSE 2876

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "2876"]