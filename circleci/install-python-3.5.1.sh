set -x
set -e

BUILDROOT=$HOME/pg8000

if [[ ! -e py-3.5.1/bin/python3.5 ]]; then
    wget https://www.python.org/ftp/python/3.5.1/Python-3.5.1.tgz
    tar -zxf Python-3.5.1.tgz
    cd ./Python-3.5.1
    ./configure --prefix=$BUILDROOT/py-3.5.1
    make install
    cd $BUILDROOT
    rm -rf ./Python-3.5.1.tgz ./Python-3.5.1
fi

ln -s $BUILDROOT/py-3.5.1/bin/python3.5 ~/bin/
