set -x
set -e

BUILDROOT=$HOME/pg8000

if [[ ! -e py-2.6.9/bin/python2.6 ]]; then
    wget https://www.python.org/ftp/python/2.6.9/Python-2.6.9.tgz
    tar -zxf Python-2.6.9.tgz
    cd ./Python-2.6.9
    # LDFLAGS here makes the build detect libssl correctly, which makes _hashlib build.
    LDFLAGS="-L/usr/lib/x86_64-linux-gnu" ./configure --prefix=$BUILDROOT/py-2.6.9
    make install
    cd $BUILDROOT
fi

ln -s $BUILDROOT/py-2.6.9/bin/python2.6 ~/bin/
