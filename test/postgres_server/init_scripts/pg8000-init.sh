#!/bin/bash

cat > /var/lib/postgresql/data/pg_hba.conf  <<EOF
host    pg8000_md5           all        0.0.0.0/0            md5
host    pg8000_gss           all        0.0.0.0/0            gss
host    pg8000_password      all        0.0.0.0/0            password
host    pg8000_scram_sha_256 all        0.0.0.0/0            scram-sha-256
host    all                  all        0.0.0.0/0            trust
EOF

# Create a user and database with the same name as the "outside" user,
#in order for code in readme.md tests to work

createuser "${OUTSIDE_USER}"
createdb "${OUTSIDE_USER}"
