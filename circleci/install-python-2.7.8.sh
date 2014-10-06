set -x
set -e

BUILDROOT=$HOME/pg8000

if [[ ! -e py-2.7.8/bin/python ]]; then
    wget https://www.python.org/ftp/python/2.7.8/Python-2.7.8.tgz
    tar -zxf Python-2.7.8.tgz
    cd ./Python-2.7.8
    ./configure --prefix=$BUILDROOT/py-2.7.8
    make install
    cd $BUILDROOT
fi
