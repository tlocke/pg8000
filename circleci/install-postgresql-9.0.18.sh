set -x
set -e

BUILDROOT=$HOME/pg8000
PG_VERSION=9.0.18
PG_PORT=5490

if [[ ! -e pgsql-${PG_VERSION}/bin/postgres ]]; then
    wget http://ftp.postgresql.org/pub/source/v${PG_VERSION}/postgresql-${PG_VERSION}.tar.bz2
    tar -jxf postgresql-${PG_VERSION}.tar.bz2
    cd postgresql-${PG_VERSION}
    ./configure --prefix=$BUILDROOT/pgsql-${PG_VERSION} --with-krb5 --with-openssl --with-ossp-uuid --with-libxml
    make world
    make install-world
    cd $BUILDROOT
    ./pgsql-${PG_VERSION}/bin/initdb -U postgres `pwd`/pgsql-${PG_VERSION}/data

    sed -i -e "s/#port = 5432/port = ${PG_PORT}/" `pwd`/pgsql-${PG_VERSION}/data/postgresql.conf
cat > `pwd`/pgsql-${PG_VERSION}/data/pg_hba.conf <<END
host    pg8000_md5      all             127.0.0.1/32            md5
host    pg8000_krb5     all             127.0.0.1/32            krb5
host    pg8000_password all             127.0.0.1/32            password
host    all             all             127.0.0.1/32            trust
END

    ./pgsql-${PG_VERSION}/bin/postgres --single -j -D `pwd`/pgsql-${PG_VERSION}/data postgres <<END
ALTER USER postgres PASSWORD 'pw';
END
    ./pgsql-${PG_VERSION}/bin/postgres --single -j -D `pwd`/pgsql-${PG_VERSION}/data postgres < ./pgsql-${PG_VERSION}/share/contrib/uuid-ossp.sql
    ./pgsql-${PG_VERSION}/bin/postgres --single -j -D `pwd`/pgsql-${PG_VERSION}/data postgres < ./pgsql-${PG_VERSION}/share/contrib/hstore.sql

fi
