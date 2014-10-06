set -x
set -e

BUILDROOT=$HOME/pg8000

if [[ ! -e pgsql-9.1.14/bin/postgres ]]; then
    wget http://ftp.postgresql.org/pub/source/v9.1.14/postgresql-9.1.14.tar.bz2
    tar -jxf postgresql-9.1.14.tar.bz2
    cd ./postgresql-9.1.14
    ./configure --prefix=$BUILDROOT/pgsql-9.1.14
    make install
    cd $BUILDROOT
    ./pgsql-9.1.14/bin/initdb `pwd`/pgsql-9.1.14/data
    sed -i -e 's/#port = 5432/port = 5491/' `pwd`/pgsql-9.1.14/data/postgresql.conf
fi
