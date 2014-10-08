set -x
set -e

BUILDROOT=$HOME/pg8000

if [[ ! -e py-3.4.2/bin/python3.4 ]]; then
    wget https://www.python.org/ftp/python/3.4.2/Python-3.4.2.tgz
    tar -zxf Python-3.4.2.tgz
    cd ./Python-3.4.2
    ./configure --prefix=$BUILDROOT/py-3.4.2
    make install
    cd $BUILDROOT
fi

ln -s $BUILDROOT/py-3.4.2/bin/python3.4 ~/bin/
