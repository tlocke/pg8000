set -x
set -e

BUILDROOT=$HOME/pg8000

if [[ ! -e pgsql-9.1.14/bin/postgres ]]; then
    wget http://ftp.postgresql.org/pub/source/v9.1.14/postgresql-9.1.14.tar.bz2
    tar -jxf postgresql-9.1.14.tar.bz2
    cd ./postgresql-9.1.14
    ./configure --prefix=$BUILDROOT/pgsql-9.1.14 --with-krb5
    make install
    cd $BUILDROOT
    ./pgsql-9.1.14/bin/initdb -U postgres `pwd`/pgsql-9.1.14/data
    sed -i -e 's/#port = 5432/port = 5491/' `pwd`/pgsql-9.1.14/data/postgresql.conf
fi

cat > `pwd`/pgsql-9.1.14/data/pg_hba.conf <<END
host    pg8000_md5      all             127.0.0.1/32            md5
host    pg8000_krb5     all             127.0.0.1/32            krb5
host    pg8000_password all             127.0.0.1/32            password
host    all             all             127.0.0.1/32            trust
END
