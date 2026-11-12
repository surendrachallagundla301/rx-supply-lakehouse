# API + built React app in one image (Azure App Service / Container Apps).
FROM node:20-alpine AS web
WORKDIR /web
COPY web/package*.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
COPY api ./api
RUN pip install --no-cache-dir ".[api]" databricks-sql-connector
COPY --from=web /web/dist ./web/dist
ENV PORT=8000
CMD ["sh", "-c", "uvicorn api.main:app --host 0.0.0.0 --port ${PORT}"]
