set -x
set -e

BUILDROOT=$HOME/pg8000

if [[ ! -e pypy3-2.3.1-linux64/bin/pypy ]]; then
    wget https://bitbucket.org/pypy/pypy/downloads/pypy3-2.3.1-linux64.tar.bz2
    tar -jxf pypy3-2.3.1-linux64.tar.bz2
fi

ln -s $BUILDROOT/pypy3-2.3.1-linux64/bin/pypy ~/bin/pypy3
