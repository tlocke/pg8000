set -x
set -e

BUILDROOT=$HOME/pg8000

if [[ ! -e py-2.5.6/bin/python2.5 ]]; then
    wget https://www.python.org/ftp/python/2.5.6/Python-2.5.6.tar.bz2
    tar -jxf Python-2.5.6.tar.bz2
    cd ./Python-2.5.6
    # LDFLAGS fixes explicit definition avoids http://bugs.python.org/issue1706863
    LDFLAGS="-L/usr/lib/x86_64-linux-gnu" ./configure --prefix=$BUILDROOT/py-2.5.6
    make
    make install
    cd $BUILDROOT/py-2.5.6
    wget https://bitbucket.org/pypa/setuptools/raw/bootstrap-py24/ez_setup.py
    ./bin/python ez_setup.py
    ./bin/easy_install nose pytz nose-testconfig
    cd $BUILDROOT
fi

ln -s $BUILDROOT/py-2.5.6/bin/python2.5 ~/bin/
