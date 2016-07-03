./pgsql-9.1.22/bin/postgres -D `pwd`/pgsql-9.1.22/data -p 5491 &> pgsql-9.1.log
./pgsql-9.2.17/bin/postgres -D `pwd`/pgsql-9.2.17/data -p 5492 &> pgsql-9.2.log
./pgsql-9.3.13/bin/postgres -D `pwd`/pgsql-9.3.13/data -p 5493 &> pgsql-9.3.log
./pgsql-9.4.8/bin/postgres -D `pwd`/pgsql-9.4.8/data -p 5494 &> pgsql-9.4.log
./pgsql-9.5.3/bin/postgres -D `pwd`/pgsql-9.5.3/data -p 5495 &> pgsql-9.5.log

cd ~postgres/pg8000-src/
tox
