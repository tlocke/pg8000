set -x
set -e

BUILDROOT=$HOME/pg8000

if [[ ! -e pypy-2.4.0-linux64/bin/pypy ]]; then
    wget https://bitbucket.org/pypy/pypy/downloads/pypy-2.4.0-linux64.tar.bz2
    tar -jxf pypy-2.4.0-linux64.tar.bz2
    rm -f pypy-2.4.0-linux64.tar.bz2
fi

ln -s $BUILDROOT/pypy-2.4.0-linux64/bin/pypy ~/bin/
