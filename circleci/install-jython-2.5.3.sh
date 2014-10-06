set -x
set -e

BUILDROOT=$HOME/pg8000

if [[ ! -e jython-2.5.3/bin/jython ]]; then
    wget -O jython-installer-2.5.3.jar "http://search.maven.org/remotecontent?filepath=org/python/jython-installer/2.5.3/jython-installer-2.5.3.jar"
    mkdir $BUILDROOT/jython-2.5.3
    cd $BUILDROOT/jython-2.5.3
    unzip $BUILDROOT/jython-installer-2.5.3.jar
    chmod +x ./bin/jython
    cd $BUILDROOT
fi

ln -s $BUILDROOT/jython-2.5.3/bin/jython ~/bin/
