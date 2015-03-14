# Installing TinyOS 1.x on Mac OS X. #

## Introduction ##

This page provides instructions for installing TinyOS on Mac OS X.


## Details ##

Adapting instructions from
  * http://www.mis.informatik.tu-darmstadt.de/People/kristof/notes/tinyos-on-mac
  * http://www.eecs.berkeley.edu/~mseeman/resources/macmicro.html
  * http://www.eecs.harvard.edu/~mdw/proj/tinyos-macos/
  * http://cents.cs.berkeley.edu/tinywiki/index.php/Tmote_Macintosh_install

There is no need for the MSP430 or BSN


### Environment Setup ###

Edit .bashrc with the following:
```
#== Added while installing TinyOS:=======================
export WORKSPACEDIR=$HOME/Documents/Manchester/Workspace
export TOSROOT=$WORKSPACEDIR/tinyos-1.x
export TOSDIR=$TOSROOT/tos
export CLASSPATH="`$TOSROOT/tools/java/javapath`"
export MAKERULES=$TOSROOT/tools/make/Makerules
#=============================================
```

### Get TinyOS Code ###

```
> cd $WORKSPACEDIR
> cvs -d:pserver:anonymous@tinyos.cvs.sourceforge.net:/cvsroot/tinyos login
  (just press enter when prompted for a password)
> cvs -z3 -d:pserver:anonymous@tinyos.cvs.sourceforge.net:/cvsroot/tinyos co -P tinyos-1.x
```

Replace $TOSROOT/tools/java/net/tinyos/packet/SerialByteSource.java
with
http://www.eecs.harvard.edu/~mdw/proj/tinyos-macos/SerialByteSource.java


### Install avr-gcc ###

Download and install from
http://www.eecs.berkeley.edu/~mseeman/resources/macmicro.html


### Install nesC ###

Download platform install package
or
```
> cd $WORKSPACEDIR
> cvs -d:pserver:anonymous@nescc.cvs.sourceforge.net:/cvsroot/nescc login
(press Enter when asked for a password)
> cvs -z3 -d:pserver:anonymous@nescc.cvs.sourceforge.net:/cvsroot/nescc co nesc
```

### Install and configure ###

```
> cd nesc
> ./Bootstrap
> ./configure && make && sudo make install
> cd $TOSROOT/tools/src/ncc
> ./Bootstrap
> ./configure && make && sudo make install
```

### Install TinyOS Toolset ###

```
> cd $TOSROOT/tools
> make
> sudo make install
```

### Checking TinyOS Setup ###

```
> toscheck
```

Contains a lot of warnings but nothing seems to fail