FROM apache/airflow:2.10.5

USER root
# Install Git and OpenJDK in a single layer
RUN apt-get update && \
    apt-get install -y git openjdk-17-jre && \
    apt-get clean

# Set JAVA_HOME for JVM (used by JPype)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Create a directory for JARs (optional but clean)
RUN mkdir -p /opt/airflow/libs

# Copy your shaded JAR from build context into the container
COPY airflow/java-jars/transformer-lib-1.0.0-SNAPSHOT.jar /opt/airflow/libs/

USER airflow
# Install extra Airflow provider
RUN pip install jpype1 apache-airflow-providers-mongo