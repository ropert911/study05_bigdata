# spring-boot app
#
# VERSION               0.0.1

FROM java:8-jre-alpine

ENV JAVA_OPTS ""

COPY *.jar app.jar
RUN sh -c 'touch app.jar'

CMD java $JAVA_OPTS -jar app.jar