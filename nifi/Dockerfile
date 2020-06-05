FROM apache/nifi:latest
COPY logback.xml /opt/nifi/nifi-current/conf
COPY nifi-cdc-postgresql-nar-1.11.3.nar /opt/nifi/nifi-current/lib
RUN curl -o /opt/nifi/nifi-current/postgresql-42.2.11.jar https://jdbc.postgresql.org/download/postgresql-42.2.11.jar
