set -x
set -e

BUILDROOT=$HOME/pg8000
PG_VERSION=9.1.14
PG_PORT=5491

source $BUILDROOT/circleci/install-postgresql-generic.sh
