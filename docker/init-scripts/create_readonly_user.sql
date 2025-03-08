-- Replace 'readonly_user' and 'readonly_password' with your desired username and password
CREATE USER readonly WITH PASSWORD '12345';
GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO readonly_user;
GRANT USAGE ON SCHEMA public TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readonly_user;
