#!/bin/bash

cat > /var/lib/postgresql/data/pg_hba.conf  <<EOF
host    pg8000_md5           all        0.0.0.0/0            md5
host    pg8000_gss           all        0.0.0.0/0            gss
host    pg8000_password      all        0.0.0.0/0            password
host    pg8000_scram_sha_256 all        0.0.0.0/0            scram-sha-256
host    all                  all        0.0.0.0/0            trust
EOF


# Generate server key and certificate
openssl req -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout /var/lib/postgresql/data/server.key -out /var/lib/postgresql/data/server.crt -subj "/CN=localhost"

# Secure the server.key file
chmod 600 /var/lib/postgresql/data/server.key

# Ensure the right owner and permissions for PostgreSQL to use the certificates
chown postgres:postgres /var/lib/postgresql/data/server.crt /var/lib/postgresql/data/server.key

# Enable SSL, set proper password encryption and enable logging of all statements
cat >> /var/lib/postgresql/data/postgresql.conf <<EOF
password_encryption = 'scram-sha-256'

ssl = on
ssl_cert_file = '/var/lib/postgresql/data/server.crt'
ssl_key_file = '/var/lib/postgresql/data/server.key'

log_statement = all
EOF

# Create a user and database with the same name as the "outside" user,
#in order for code in readme.md tests to work

createuser "${OUTSIDE_USER}"
createdb "${OUTSIDE_USER}"
