FROM maven:3.8.2-openjdk-11

COPY bootstrap.sh bootstrap.sh
RUN chmod +x bootstrap.sh

RUN ./bootstrap.sh

RUN mvn -N io.takari:maven:wrapper