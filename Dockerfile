
FROM maven:3.8-openjdk-11-slim as builder

WORKDIR /app

##RUN mvn -DskipTests=true clean compile package

COPY pom.xml .

RUN mvn -B -f pom.xml dependency:go-offline

# Copy all other project files and build project
COPY . /app

RUN mvn -B install -DskipTests=true

### BUILD image
FROM openjdk:11-slim

EXPOSE 8182

ENV APP_HOME /app

#Create base app folder

RUN mkdir $APP_HOME

#Create folder with application logs

RUN mkdir $APP_HOME/vol

RUN mkdir $APP_HOME/vol/out

VOLUME $APP_HOME/vol

WORKDIR $APP_HOME

#COPY --chown=gradle:gradle . /home/gradle/src
COPY ./target/*.jar aggregator-0.0.1.jar

ENTRYPOINT ["java", "-jar","aggregator-cmd-0.0.1.jar" ]
#CMD ["-Djava.security.egd=file:/dev/./urandom -Ddir.resources=/app/vol/ -Ddir.small=/app/vol/small_example/ -Ddir.medium=/app/vol/medium_example/"]

