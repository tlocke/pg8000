ALTER SYSTEM SET ssl = on;
ALTER SYSTEM SET ssl_cert_file = '/etc/ssl/certs/ssl-cert-snakeoil.pem';
ALTER SYSTEM SET ssl_key_file = '/etc/ssl/private/ssl-cert-snakeoil.key';
ALTER SYSTEM SET password_encryption = 'scram-sha-256';
ALTER SYSTEM SET log_statement = 'all';
CREATE EXTENSION hstore;

CREATE DATABASE pg8000_gss;
