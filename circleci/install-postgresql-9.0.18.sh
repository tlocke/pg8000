set -x
set -e

BUILDROOT=$HOME/pg8000

if [[ ! -e pgsql-9.0.18/bin/postgres ]]; then
    wget http://ftp.postgresql.org/pub/source/v9.0.18/postgresql-9.0.18.tar.bz2
    tar -jxf postgresql-9.0.18.tar.bz2
    cd postgresql-9.0.18
    ./configure --prefix=$BUILDROOT/pgsql-9.0.18
    make install
    cd $BUILDROOT
    ./pgsql-9.0.18/bin/initdb `pwd`/pgsql-9.0.18/data
    sed -i -e 's/#port = 5432/port = 5490/' `pwd`/pgsql-9.0.18/data/postgresql.conf
fi
