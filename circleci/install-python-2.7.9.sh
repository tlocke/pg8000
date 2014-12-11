set -x
set -e

BUILDROOT=$HOME/pg8000

if [[ ! -e py-2.7.9/bin/python2.7 ]]; then
    wget https://www.python.org/ftp/python/2.7.9/Python-2.7.9.tgz
    tar -zxf Python-2.7.9.tgz
    cd ./Python-2.7.9
    ./configure --prefix=$BUILDROOT/py-2.7.9
    make install
    cd $BUILDROOT
fi

ln -s $BUILDROOT/py-2.7.9/bin/python2.7 ~/bin/
