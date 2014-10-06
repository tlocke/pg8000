set -x
set -e

if [[ ! -e pgsql-9.4beta2/bin/postgres ]]; then
    wget http://ftp.postgresql.org/pub/source/v9.4beta2/postgresql-9.4beta2.tar.bz2
    tar -jxf postgresql-9.4beta2.tar.bz2
    cd ./postgresql-9.4beta2
    ./configure --prefix=`pwd`/pgsql-9.4beta2
    make install
    cd ~/
    ./pgsql-9.4beta2/bin/initdb `pwd`/pgsql-9.4beta2/data
    sed -i -e 's/#port = 5432/port = 5494/' `pwd`/pgsql-9.4beta2/data/postgresql.conf
fi
