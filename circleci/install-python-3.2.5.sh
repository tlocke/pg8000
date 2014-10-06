set -x
set -e

BUILDROOT=$HOME/pg8000

if [[ ! -e py-3.2.5/bin/python ]]; then
    wget https://www.python.org/ftp/python/3.2.5/Python-3.2.5.tar.bz2
    tar -jxf Python-3.2.5.tar.bz2
    cd ./Python-3.2.5
    ./configure --prefix=$BUILDROOT/py-3.2.5
    make install
    cd $BUILDROOT
fi
