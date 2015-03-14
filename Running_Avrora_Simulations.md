# Introduction #

Add your content here.


# Details #

This file explain how to run Query Plan simulations using the Avrora simulator.

There are three ways to do this:
I. Running the simulation manually (command line, interactive mode)
II. Running the utility scripts (command line, interactive mode, but faster than 1)
III. Invoking the Python libraries (batch mode, from a script)

### Setup your Cygwin runtime environment ###

For the scripts to work properly, you need to set some environment variables under Cygwin.  You'll save time if you add a variation of the following text to your $HOME/.bash\_profile file (changing the directories to point at your own ones).

Note: For the CLASSPATH variable, you need to use windows directory format.  Therefore, you need to use double backslashes so that they are escaped.  For the other variables, use linux format, therefore you need to escape all spaces with a backslash.

```

#Set SNEEQL root dir
declare -x SNEEQLROOT="/cygdrive/c/dias-mc/work2/SNEEql-test"
echo "SNEEQLROOT set to $SNEEQLROOT"

#Put avrora in the classpath
declare -x CLASSPATH="$CLASSPATH;C:\\dias-mc\\work2\\avrora\\christian-avrora\\src"
echo "Java CLASSPATH updated to include Avrora"

#Pythonpath
declare -x PYTHONPATH="$PYTHONPATH:$SNEEQLROOT/scripts/lib"
echo "Scripts PYTHONPATH updated to $PYTHONPATH"

#Serial listener
declare -x  MOTECOM="network@127.0.0.1:2390"
echo "SerialListener MOTECOM set to $MOTECOM"

#DBG for tossim
declare -x DBG="usr1"
echo "Tossim DBG set to $DBG"

#Add util/batch scripts to PATH
declare -x PATH="$PATH:$SNEEQLROOT/scripts/batch:$SNEEQLROOT/scripts/utils"
echo "PATH updated to include scripts"

#Java 5 bin directory
declare -x JAVA5BINDIR="/cygdrive/c/Program Files\Java\jdk1.5.0_15/bin"

cd $SNEEQLROOT 
```


Also, Compile the Avrora Source code (use Java 5 or higher)


### Running the simulation Manually ###

The basic steps are:

  1. Run the SNEEql optimizer
  1. Go to the nesc directory
  1. For each mote directory, you need compile the code the mica platform, and convert it into an OD file.
```
	$ make mica2
	$ avr-objdump -zhD ./build/mica2/main.exe > ../mote0.od
```
  1. You should have all the od files in the nesc directory now.  If there are any nodes which do not participate in the query plan, you will need to copy across the empty.od file:
```
	$ cp $SNEEQLROOT/scripts/lib/empty.od empty.od
```
  1. Now, from the nesc directory, invoke the simulation:
```
	$ <java-exe-full-path> avrora.Main -simulation=sensor-network -colors=false -seconds=100 -monitors=packet,serial -random-seed=1 -sensor-data=light:4:.,light:5:.,light:7:. -nodecount=1,1,1,1,1,1,1,1,1,1 mote0.od mote1.od mote2.od mote3.od mote4.od mote5.od empty.od mote7.od empty.od empty.od
```
for example:
```
	$ "/cygdrive/c/Program Files\Java\jdk1.5.0_15/bin/java" avrora.Main -simulation=sensor-network -colors=false -seconds=100 -monitors=packet,serial -random-seed=1 -sensor-data=light:4:.,light:5:.,light:7:. -nodecount=1,1,1,1,1,1,1,1,1,1 mote0.od mote1.od mote2.od mote3.od mote4.od mote5.od empty.od mote7.od empty.od empty.od
```
  1. Then, invoke the packet listener, redirecting the output to packets.txt

> $ java net.tinyos.tools.Listen > packets.txt

  1. When the Avrora simulation finishes in the first window, use CTRL-C to terminate the packet listener. Then use the readPackets script to decipher the packet messages:

> $ readPackets.py packets.txt


### Running the utility scripts (command line, interactive mode, but faster than 1) ###


Instead of doing step 3 manually, the makeODs utility script compiles the nesC code into mica2 format and then generates OD files.
