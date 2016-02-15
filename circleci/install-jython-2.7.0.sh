set -x
set -e

BUILDROOT=$HOME/pg8000

if [[ ! -e jython-2.7.0/bin/jython ]]; then
    wget -O jython-installer-2.7.0.jar "http://search.maven.org/remotecontent?filepath=org/python/jython-installer/2.7.0/jython-installer-2.7.0.jar"
    mkdir $BUILDROOT/jython-2.7.0
    cd $BUILDROOT/jython-2.7.0
    unzip $BUILDROOT/jython-installer-2.7.0.jar
    chmod +x ./bin/jython
    cd $BUILDROOT
fi

ln -s $BUILDROOT/jython-2.7.0/bin/jython ~/bin/
