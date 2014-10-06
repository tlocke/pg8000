set -x
set -e

if [[ ! -e pgsql-9.3.5/bin/postgres ]]; then
    wget http://ftp.postgresql.org/pub/source/v9.3.5/postgresql-9.3.5.tar.bz2 
    tar xzf postgresql-9.3.5.tar.bz2 
    ./postgresql-9.3.5/configure --prefix=`pwd`/pgsql-9.3.5 
    make install
    ./pgsql-9.3.5/bin/initdb `pwd`/pgsql-9.3.5/data
    sed -i -e 's/#port = 5432/port = 5493/' `pwd`/pgsql-9.3.5/data/postgresql.conf
fi
