set -x
set -e

BUILDROOT=$HOME/pg8000
PG_VERSION=9.2.17
PG_PORT=5492

source $BUILDROOT/circleci/install-postgresql-generic.sh
