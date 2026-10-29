FROM eclipse-temurin:17.0.12_7-jre@sha256:ec569ef72b6e1a55daedc5d59b0ac8134284aed133af24cd51826cda41665ae8 AS java
FROM python:3.11.11-bookworm@sha256:4ca910a51a1a474e5d95aa52455331b2a94272eeae3c498be1ad7a2ff9b00bf3
COPY --from=java /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk PATH=/opt/java/openjdk/bin:$PATH PYTHONUNBUFFERED=1 SPARK_LOCAL_IP=127.0.0.1
WORKDIR /app
COPY requirements.lock.txt .
RUN pip install --no-cache-dir -r requirements.lock.txt
COPY src ./src
RUN python src/fetch_jars.py
ENV PYTHONPATH=/app/src
CMD ["python", "src/dashboard.py"]
