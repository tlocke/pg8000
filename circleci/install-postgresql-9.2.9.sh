set -x
set -e

if [[ ! -e pgsql-9.2.9/bin/postgres ]]; then
    wget http://ftp.postgresql.org/pub/source/v9.2.9/postgresql-9.2.9.tar.bz2
    tar -jxf postgresql-9.2.9.tar.bz2
    cd ./postgresql-9.2.9
    ./configure --prefix=`pwd`/pgsql-9.2.9
    make install
    cd ~/
    ./pgsql-9.2.9/bin/initdb `pwd`/pgsql-9.2.9/data
    sed -i -e 's/#port = 5432/port = 5492/' `pwd`/pgsql-9.2.9/data/postgresql.conf
fi
