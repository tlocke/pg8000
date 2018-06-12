set -x
set -e

BUILDROOT=$HOME/pg8000

if [[ ! -e jython-2.7.1/bin/jython ]]; then
    wget -O jython-installer-2.7.1.jar "http://search.maven.org/remotecontent?filepath=org/python/jython-installer/2.7.1/jython-installer-2.7.1.jar"
    mkdir $BUILDROOT/jython-2.7.1
    cd $BUILDROOT/jython-2.7.1
    unzip $BUILDROOT/jython-installer-2.7.1.jar
    chmod +x ./bin/jython
    cd $BUILDROOT
    rm -f jython-installer-2.7.1.jar
fi

ln -s $BUILDROOT/jython-2.7.1/bin/jython ~/bin/
