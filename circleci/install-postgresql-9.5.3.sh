set -x
set -e

BUILDROOT=$HOME/pg8000
PG_VERSION=9.5.3
PG_PORT=5495

source $BUILDROOT/circleci/install-postgresql-generic.sh

# Create a second PostgreSQL DB instance, which we'll run on port 5432 to enable doctests
./pgsql-9.5.3/bin/initdb -U postgres -E UTF8 $BUILDROOT/pgsql-9.5.3/data2
