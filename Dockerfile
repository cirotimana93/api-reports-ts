# imagen base de python
FROM python:3.11-slim

# configurar zona horaria
ENV TZ=America/Lima
RUN apt-get update && apt-get install -y tzdata && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# evitar que Python genere archivos .pyc y habilitar logs en tiempo real
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# instalar dependencias del sistema necesarias para Playwright
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxcb1 \
    libxkbcommon0 \
    libx11-6 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

# directorio de trabajo
WORKDIR /app

# copiar requirements e instalar dependencias de python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# instalar navegadores
RUN playwright install chromium
RUN playwright install-deps chromium

# copiar el codigo de la aplicacion
COPY . .

# puerto
EXPOSE 8000

# arranque
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
