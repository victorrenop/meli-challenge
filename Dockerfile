FROM python:3.8.5

# Install Java
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    apt-add-repository 'deb http://security.debian.org/debian-security stretch/updates main' && \
    apt-get update && \
    apt-get install -y openjdk-8-jdk;
    
RUN apt-get install ca-certificates-java && \
    update-ca-certificates -f;

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN export JAVA_HOME

# Install make
RUN apt-get install -y make;

# Set up API
WORKDIR westeros_api/
COPY . .
RUN make requirements
ENTRYPOINT ["python", "-m", "meli_challenge.westeros_api"]
