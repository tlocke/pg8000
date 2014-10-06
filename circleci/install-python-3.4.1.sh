set -x
set -e

BUILDROOT=$HOME/pg8000

if [[ ! -e py-3.4.1/bin/python ]]; then
    wget https://www.python.org/ftp/python/3.4.1/Python-3.4.1.tgz
    tar -zxf Python-3.4.1.tgz
    cd ./Python-3.4.1
    ./configure --prefix=$BUILDROOT/py-3.4.1
    make install
    cd $BUILDROOT
fi

ln -s $BUILDROOT/py-3.4.1/python3.4 ~/bin/
