FROM dpage/pgadmin4:latest

# Keep the server list, but do not bake any DB passwords into the image.
USER root
COPY servers.json /pgadmin4/servers.json
RUN chown pgadmin:root /pgadmin4/servers.json
USER pgadmin
