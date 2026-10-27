-- Executed by init-db.sh only for an empty postgres-data volume.
-- psql variables are passed by init-db.sh; :'<name>' safely quotes strings.
SELECT format('CREATE ROLE airflow LOGIN PASSWORD %L', :'airflow_password') \gexec
SELECT format('CREATE ROLE admin LOGIN PASSWORD %L', :'dwh_password') \gexec

CREATE DATABASE airflow OWNER airflow;
CREATE DATABASE raw OWNER admin;
CREATE DATABASE marts OWNER admin;

GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
GRANT ALL PRIVILEGES ON DATABASE raw TO admin;
GRANT ALL PRIVILEGES ON DATABASE marts TO admin;

\connect raw
ALTER SCHEMA public OWNER TO admin;
GRANT ALL ON SCHEMA public TO admin;

\connect marts
ALTER SCHEMA public OWNER TO admin;
GRANT ALL ON SCHEMA public TO admin;

\connect airflow
ALTER SCHEMA public OWNER TO airflow;
GRANT ALL ON SCHEMA public TO airflow;
