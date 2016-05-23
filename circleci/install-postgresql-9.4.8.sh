set -x
set -e

BUILDROOT=$HOME/pg8000
PG_VERSION=9.4.8
PG_PORT=5494

source $BUILDROOT/circleci/install-postgresql-generic.sh
