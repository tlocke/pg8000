set -x
set -e

BUILDROOT=$HOME/pg8000

if [[ ! -e pgsql-9.4beta2/bin/postgres ]]; then
    wget http://ftp.postgresql.org/pub/source/v9.4beta2/postgresql-9.4beta2.tar.bz2
    tar -jxf postgresql-9.4beta2.tar.bz2
    cd ./postgresql-9.4beta2
    ./configure --prefix=$BUILDROOT/pgsql-9.4beta2
    make install
    cd $BUILDROOT
    ./pgsql-9.4beta2/bin/initdb `pwd`/pgsql-9.4beta2/data
    sed -i -e 's/#port = 5432/port = 5494/' `pwd`/pgsql-9.4beta2/data/postgresql.conf
fi
