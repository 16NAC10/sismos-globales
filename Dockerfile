FROM python:3.12-slim

# Spark 4 necesita Java 17 o 21.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java

ENV JAVA_HOME=/opt/java
WORKDIR /app

COPY pyproject.toml ./
COPY src ./src
COPY config ./config
RUN pip install --no-cache-dir -e .

EXPOSE 8501
CMD ["python", "-m", "sismos", "--help"]
