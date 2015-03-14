# Introduction #

This page explains how to install the developer environment for SNEE.  Users of SNEE are recommended to use the QuickStart instructions.

### Dependencies ###


  * Java SDK 1.5 or newer
    * antlr (2.7.5 is recommended)
  * GraphViz (I've got version 2.20.2 for the Mac, 2.20.3 had a bug and does not work)
  * gnuplot 4.2

### Recommendations ###

  * Eclipse for code editing
  * Subclipse as an SVN client within Eclipse


### Creating an Eclipse Project ###

  * File -> New -> Other
  * SVN -> Checkout Projects from SVN
  * Create a new repository location
    * URL: http://rpc240.cs.man.ac.uk:3180/svn/SNEEql/
    * username and password for SVN
    * select trunk
    * Check out as project configured with new project wizard
  * Java project
    * Project name: SNEE


### Configuration Files ###

local.ini contains all machine dependent variables. Need to supply the following:
  * GraphViz path
  * Haskell compiler

.bashrc
Need to set the following variables
  * SNEEQLROOT
  * MOTECOM
  * DBG
  * PATH
  * JAVA5BINDIR
  * LATEXDIR currently MITEXDIR


### Running SNEE Query Compiler ###

From within Eclipse
  * Open Run Dialog
> > = Set Main class: uk.ac.manchester.cs.diasmc.querycompiler.QueryCompiler


### Compiliing and running nesC Code ###

  * Change to the outputs directory
  * For tinyos1

```
   >  make pc
   > ./build/pc/main.exe -b=1 -t=60 10
```
(where b is and t is the time for the simulation and the argument is the number of nodes.)

  * For tinyos2
```
   > make micaz sim
   > python runTossim.py
```