set -x
set -e

BUILDROOT=$HOME/pg8000
PG_VERSION=9.5.3
PG_PORT=5495

source $BUILDROOT/circleci/install-postgresql-generic.sh
