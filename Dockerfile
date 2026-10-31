FROM python:3.12-slim

# Spark necesita Java; el resto de dependencias son Python puras o tienen ruedas precompiladas.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
WORKDIR /app

COPY pyproject.toml ./
COPY src ./src
COPY config ./config
RUN pip install --no-cache-dir -e .

EXPOSE 8501
CMD ["python", "-m", "sismos", "--help"]
