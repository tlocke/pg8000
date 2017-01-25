set -x
set -e

BUILDROOT=$HOME/pg8000

if [[ ! -e pypy3.3-5.5-alpha-20161013-linux_x86_64-portable/bin/pypy ]]; then
    wget https://bitbucket.org/squeaky/portable-pypy/downloads/pypy3.3-5.5-alpha-20161013-linux_x86_64-portable.tar.bz2
    tar -jxf pypy3.3-5.5-alpha-20161013-linux_x86_64-portable.tar.bz2
    rm -f pypy3.3-5.5-alpha-20161013-linux_x86_64-portable.tar.bz2
fi

ln -s $BUILDROOT/pypy3.3-5.5-alpha-20161013-linux_x86_64-portable/bin/pypy ~/bin/pypy3
