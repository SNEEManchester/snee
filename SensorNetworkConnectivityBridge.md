# Introduction #

This page describes the commands used by the Sensor Network Connectivity Bridge.

# Dependencies #

  * TinyOS 2.1.1
  * SNEE unzipped and compiled according to these instructions (TODO).
  * Set $SNEEROOT to point to the root directory of SNEE.
  * Add `$SNEEROOT/etc/sncb/tools/python` and `$SNEEROOT/etc/sncb/tools/python/utils` directories to your PATH.
  * Set BASESTATION to be your basestation id (e.g., serial@/dev/tty.usbserial-XBTFSN2Q) -- find this out by running a motelist command.

# Setting up the Nodes #

The first thing that you need to do is install the Over-the-Air Programmer and Metadata Collector into slots 1 and 0 respectively of the External data flash, so that they can be booted into as required.

Slot 2 is reserved for the SNEE query plan that will be disseminated.

The code for these programs is located at `$SNEEROOT/etc/sncb/tools/nesC`.

## Install the OTA tool into the Nodes' External Flash Memory (Slot 1) ##

  * First, manually with a USB cable, add OTAServer to each non-basestation mote (from within the `OtaServer` directory):
> ` make telosb install,<node_id> `

  * Then, manually with a USB cable, install OTABasestation on the basestation mote (from within the `OtaBasestation` directory)
> ` make telosb install,<node_id> `

  * Then, with the basestation plugged in, install the OTAServer image into each node except the basestation at slot 1 (back in the `OtaServer` directory)
> ` tos-ota <source> <destination> -i 1 <tos_image.xml>  `
    * `<source>` is the serial port that the basestation is attached to.  NOTE, you must include the speed of the port, e.g., `serial@/dev/tty.usbserial-USB0:115200`
    * `<destination>` is the id of the node.
    * `<tos_image.xml>` is located at `$SNEEROOT/etc/sncb/tools/nesC/OtaServer/build/telosb/tos_image.xml`.

### Example ###
If you have a 2 node network, with node 1 as the basestation and node 2 as a sensing node, the sequence of commands is:
```
cd OtaServer
make telosb Install,2
cd ../OtaBasestation
make telosb Install,1
cd ../OtaServer
tos-ota $BASESTATION:115200 2 -i 1 $SNEEROOT/SNEE/etc/sncb/tools/nesC/OtaServer/build/telosb/tos_image.xml
```

## Installing the Metadata Collector Image on the Nodes' External Flash Memory (slot 0) ##
  * For each non-basestation node:
  * Change to nesC/MetadataCollector directory
  * Compile executable
> > ` make telosb `
  * Send the executable to the node at slot 0

> `tos-ota <source> <destination> -i 0 <tos_image.xml> `

# SNEE Lifecycle (automated) #
> Network initialisation, metadata data collection, query plan dissemination etc. can be invoked automatically by the the SNEE sensor network connectivity bridge. Just run the in-network client  in the SNEE project, without any parameters.

> Note that the initialisation file that is used is currently hard-coded.

# SNEE Lifecycle (standalone scripts) #
> You may wish to invoke the python standalone scripts to run the process manually.  Below is a summary of each command, in the order invoked by the SNEE SNCB.

  * init
    * This needs the metadata collector installed on the nodes' program memory.
    * Forms the network and collects metadata.
> > ` init <topology_file> <resources_file> <basestation_id> `
  * register
    * (This assumes that the OTA image is in external flash slot 1)
    * Installs an image to a specific node.
> > ` register <image absoulute path>  <dest node id>  <basestation_id> `
  * start
    * Boots nodes into the query plan image, and Initiates query execution.
> > ` start `
  * stop
    * Stops query execution, and reboots nodes back into OTA mode.
> > ` stop <basestation id> `
  * deregister
    * Uninstalls a query image on a specific node.
> > ` deregister <id1> <id2> ... <idn> <basestation_id> `