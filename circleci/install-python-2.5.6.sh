set -x
set -e

BUILDROOT=$HOME/pg8000

if [[ ! -e py-2.5.6/bin/python ]]; then
    wget https://www.python.org/ftp/python/2.5.6/Python-2.5.6.tar.bz2
    tar -jxf Python-2.5.6.tar.bz2
    cd ./Python-2.5.6
    ./configure --prefix=$BUILDROOT/py-2.5.6
    make install
    cd $BUILDROOT
fi
