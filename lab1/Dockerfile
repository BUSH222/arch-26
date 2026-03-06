FROM eclipse-temurin:17-jdk
COPY *.jar app.jar
ENV SERVER_PORT=8080
ENTRYPOINT ["java", "-jar", "app.jar"]