set -x
set -e

BUILDROOT=$HOME/pg8000

if [[ ! -e py-2.6.9/bin/python ]]; then
    wget https://www.python.org/ftp/python/2.6.9/Python-2.6.9.tgz
    tar -zxf Python-2.6.9.tgz
    cd ./Python-2.6.9
    ./configure --prefix=$BUILDROOT/py-2.6.9
    make install
    cd $BUILDROOT
fi
