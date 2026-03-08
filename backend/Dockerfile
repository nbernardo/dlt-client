FROM python:3.12-slim

# Instala dependências do sistema (ODBC + MSSQL)
RUN apt-get update && apt-get install -y --no-install-recommends \
    unixodbc \
    curl \
    gnupg \
    ca-certificates \
    && mkdir -p /etc/apt/keyrings \
    && curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /etc/apt/keyrings/microsoft.gpg \
    && echo "deb [arch=amd64,arm64,armhf signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Instala uv e define PATH imediatamente (sem .bashrc)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh \
    && rm -rf /root/.local/share/uv  # opcional: limpa cache desnecessário

ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /app

# Copia e instala deps com uv (agora PATH está correto)
COPY requirements.txt .
RUN uv pip install --system --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
CMD ["uv", "run", "python", "-u", "src/app.py"]