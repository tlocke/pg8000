set -x
set -e

BUILDROOT=$HOME/pg8000

if [[ ! -e py-2.5.6/bin/python ]]; then
    wget https://www.python.org/ftp/python/2.5.6/Python-2.5.6.tar.bz2
    tar -jxf Python-2.5.6.tar.bz2
    cd ./Python-2.5.6
    # LDFLAGS fixes explicit definition avoids http://bugs.python.org/issue1706863
    LDFLAGS="-L/usr/lib/x86_64-linux-gnu" ./configure --prefix=$BUILDROOT/py-2.5.6
    make
    make install
    cd $BUILDROOT
fi

ln -s $BUILDROOT/py-2.5.6/python2.5 ~/bin/
