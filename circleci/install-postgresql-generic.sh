set -x
set -e

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
    sed -i -e "s/#ssl = off/ssl = on/" `pwd`/pgsql-${PG_VERSION}/data/postgresql.conf

cat > `pwd`/pgsql-${PG_VERSION}/data/pg_hba.conf <<END
host    pg8000_md5      all             127.0.0.1/32            md5
host    pg8000_krb5     all             127.0.0.1/32            krb5
host    pg8000_password all             127.0.0.1/32            password
host    all             all             127.0.0.1/32            trust
END

    ./pgsql-${PG_VERSION}/bin/postgres --single -j -D `pwd`/pgsql-${PG_VERSION}/data postgres <<END
ALTER USER postgres PASSWORD 'pw';
CREATE EXTENSION "uuid-ossp";
CREATE EXTENSION hstore;
END

fi

# Temporarily re-run these commands until I rebuild the cached installs
./pgsql-${PG_VERSION}/bin/postgres --single -j -D `pwd`/pgsql-${PG_VERSION}/data postgres <<END
ALTER USER postgres PASSWORD 'pw';
END
./pgsql-${PG_VERSION}/bin/postgres --single -j -D `pwd`/pgsql-${PG_VERSION}/data postgres <<END
CREATE EXTENSION "uuid-ossp";
END
./pgsql-${PG_VERSION}/bin/postgres --single -j -D `pwd`/pgsql-${PG_VERSION}/data postgres <<END
CREATE EXTENSION hstore;
END

cat > `pwd`/pgsql-${PG_VERSION}/data/server.crt <<END
Certificate:
    Data:
        Version: 3 (0x2)
        Serial Number: 11984152055957194031 (0xa65042ccd598852f)
    Signature Algorithm: sha1WithRSAEncryption
        Issuer: C=CA, ST=Alberta, L=Calgary, O=pg8000, CN=localhost
        Validity
            Not Before: Oct  6 21:25:15 2014 GMT
            Not After : Nov  5 21:25:15 2014 GMT
        Subject: C=CA, ST=Alberta, L=Calgary, O=pg8000, CN=localhost
        Subject Public Key Info:
            Public Key Algorithm: rsaEncryption
                Public-Key: (1024 bit)
                Modulus:
                    00:c1:75:35:62:e8:ac:ad:2b:bb:30:f0:73:31:ee:
                    6a:a6:e5:77:8d:de:42:16:70:fd:bd:0f:f5:b0:66:
                    ea:bc:69:bc:f9:98:b2:df:a0:c6:09:e1:d0:ef:a8:
                    f3:b7:fd:51:59:c4:c9:70:2d:a1:46:af:e6:9a:35:
                    82:af:92:71:cb:d5:db:bc:cf:09:85:9e:85:6d:06:
                    68:c2:7b:68:00:71:d4:24:37:dc:2d:c9:de:f2:4b:
                    68:a6:ed:b5:fe:1c:76:bd:e2:99:27:59:f4:da:a0:
                    14:06:23:8a:fa:5c:6a:43:79:c2:ec:8b:ed:98:a6:
                    f4:46:88:af:3a:12:30:e7:4d
                Exponent: 65537 (0x10001)
        X509v3 extensions:
            X509v3 Subject Key Identifier: 
                5D:A0:C4:0A:0E:56:9D:29:EB:94:4C:81:8D:E6:0C:29:A0:D6:FB:C0
            X509v3 Authority Key Identifier: 
                keyid:5D:A0:C4:0A:0E:56:9D:29:EB:94:4C:81:8D:E6:0C:29:A0:D6:FB:C0

            X509v3 Basic Constraints: 
                CA:TRUE
    Signature Algorithm: sha1WithRSAEncryption
         8d:ea:71:6f:0c:1a:92:b1:91:60:54:5e:ab:6e:1a:6e:84:3d:
         7a:0e:d8:04:a5:85:0a:68:61:a8:c4:9b:1f:b0:24:95:95:b0:
         14:e1:5c:46:7e:2d:c3:30:0c:35:07:3c:df:2b:88:61:ca:c7:
         d8:7f:e0:5f:fd:16:2f:b3:10:76:f8:b3:2d:b6:74:93:1f:ce:
         f9:f1:0f:b1:9a:8d:1f:28:f9:ea:7d:f0:f2:79:77:c4:a7:43:
         46:26:90:b1:b2:bb:3d:4a:a0:c1:e2:12:54:bb:a3:93:97:59:
         cf:4d:dd:0e:82:54:cb:3f:ec:75:c7:58:10:9b:f3:f5:0e:7d:
         e8:13
-----BEGIN CERTIFICATE-----
MIICejCCAeOgAwIBAgIJAKZQQszVmIUvMA0GCSqGSIb3DQEBBQUAMFYxCzAJBgNV
BAYTAkNBMRAwDgYDVQQIDAdBbGJlcnRhMRAwDgYDVQQHDAdDYWxnYXJ5MQ8wDQYD
VQQKDAZwZzgwMDAxEjAQBgNVBAMMCWxvY2FsaG9zdDAeFw0xNDEwMDYyMTI1MTVa
Fw0xNDExMDUyMTI1MTVaMFYxCzAJBgNVBAYTAkNBMRAwDgYDVQQIDAdBbGJlcnRh
MRAwDgYDVQQHDAdDYWxnYXJ5MQ8wDQYDVQQKDAZwZzgwMDAxEjAQBgNVBAMMCWxv
Y2FsaG9zdDCBnzANBgkqhkiG9w0BAQEFAAOBjQAwgYkCgYEAwXU1YuisrSu7MPBz
Me5qpuV3jd5CFnD9vQ/1sGbqvGm8+Ziy36DGCeHQ76jzt/1RWcTJcC2hRq/mmjWC
r5Jxy9XbvM8JhZ6FbQZowntoAHHUJDfcLcne8ktopu21/hx2veKZJ1n02qAUBiOK
+lxqQ3nC7IvtmKb0RoivOhIw500CAwEAAaNQME4wHQYDVR0OBBYEFF2gxAoOVp0p
65RMgY3mDCmg1vvAMB8GA1UdIwQYMBaAFF2gxAoOVp0p65RMgY3mDCmg1vvAMAwG
A1UdEwQFMAMBAf8wDQYJKoZIhvcNAQEFBQADgYEAjepxbwwakrGRYFReq24aboQ9
eg7YBKWFCmhhqMSbH7AklZWwFOFcRn4twzAMNQc83yuIYcrH2H/gX/0WL7MQdviz
LbZ0kx/O+fEPsZqNHyj56n3w8nl3xKdDRiaQsbK7PUqgweISVLujk5dZz03dDoJU
yz/sdcdYEJvz9Q596BM=
-----END CERTIFICATE-----
END

cat > `pwd`/pgsql-${PG_VERSION}/data/server.key <<END
-----BEGIN RSA PRIVATE KEY-----
MIICXgIBAAKBgQDBdTVi6KytK7sw8HMx7mqm5XeN3kIWcP29D/WwZuq8abz5mLLf
oMYJ4dDvqPO3/VFZxMlwLaFGr+aaNYKvknHL1du8zwmFnoVtBmjCe2gAcdQkN9wt
yd7yS2im7bX+HHa94pknWfTaoBQGI4r6XGpDecLsi+2YpvRGiK86EjDnTQIDAQAB
AoGANBF+3/Mt3qIBjOd6qbq4u9jQ978VXILm2Eb+Yo8gqLtw4GDn6+aSxsAfgxGf
HyswBRBBA09Us/jOAT1bwjORKsmwSFj2NJxZQKmcbHcVF8ctZ7CYZMikiImnaRRq
8kR0kH3q8t4iHD3leonIiCka1oxKDk8SzNbcv/T5EwI0/GECQQD+lXgzbJs+Fqn7
A6I+/vKkTZaxhQ7698YdQTgW4yHpptkZ15gFQiLju5NSxt+HbE4ggFpnrCfjXsD+
1n5mH6aFAkEAwoix3/ogSRjmrTeNlhgsYOBc7ysrKJN4x1tsZ0YsP3wQz3+V53is
cbyZMwjXMNjAWCI9t0YSTUcUdHQc2V0MKQJBAJ89nScFWwF2KbKh7j1bOSi+g3Dc
mqcujpRD9DKUteSxgYChkyxO1wX9kUQosy7A9wlrX6ETVQvqe+uq/Psh9wUCQQCD
U6rZbM9s6Y2Y9H72C/2xrOWwZHEvZFdOJm8JTTtD5Gqo2hYF/NZVth+qb1Zu2HUJ
SpxlZL2oQ8sQTu7G4uT5AkEA6z25XZBJ1EwWknP6QUe6ZRqDw5XGwTvJO4lGusfs
u8/RrIJ/Nz2DmKedu0NBjxZsdBJLTwu017dXVbd3ST2O+Q==
-----END RSA PRIVATE KEY-----
END

chmod 600 `pwd`/pgsql-${PG_VERSION}/data/server.key
