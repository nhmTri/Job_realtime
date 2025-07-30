FROM cassandra:latest

COPY init.cql /init.cql
COPY cmd_run.sh /cmd_run.sh
RUN chmod +x /cmd_run.sh

CMD ["/bin/bash", "-c", "/cmd_run.sh"]