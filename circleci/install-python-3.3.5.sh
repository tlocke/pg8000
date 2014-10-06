set -x
set -e

BUILDROOT=$HOME/pg8000

if [[ ! -e py-3.3.5/bin/python ]]; then
    wget http://www.python.org/ftp/python/3.3.5/Python-3.3.5.tgz
    tar -zxf Python-3.3.5.tgz
    cd ./Python-3.3.5
    ./configure --prefix=$BUILDROOT/py-3.3.5
    make install
    cd $BUILDROOT
fi
