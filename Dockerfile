FROM openjdk:8 as builder

RUN apt-get update && apt-get install -y git

RUN git clone https://github.com/fruch/hydra-kcl.git -b master
RUN cd hydra-kcl; ./gradlew build

FROM openjdk:8 as app
RUN echo 'networkaddress.cache.ttl=0' >> $JAVA_HOME/jre/lib/security/java.security
RUN echo 'networkaddress.cache.negative.ttl=0' >> $JAVA_HOME/jre/lib/security/java.security
COPY java.policy $JAVA_HOME/jre/lib/security/java.policy

COPY --from=builder /root/.gradle/ /root/.gradle/
COPY --from=builder /hydra-kcl /hydra-kcl
