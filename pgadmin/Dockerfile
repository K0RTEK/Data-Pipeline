FROM dpage/pgadmin4:latest

USER root

COPY servers.json /pgadmin4/servers.json
COPY pgpass /var/lib/pgadmin/pgpass

RUN chown pgadmin:root /pgadmin4/servers.json /var/lib/pgadmin/pgpass && \
    chmod 600 /var/lib/pgadmin/pgpass

USER pgadmin