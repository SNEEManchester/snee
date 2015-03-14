The SNEE (SNEE for Sensor NEtwork Engine) query optimizer is a novel query optimizer for sensor networks and other streaming data sources.  SNEE combines a rich, expressive query language with a software architecture based on an extension of traditional distributed query processing techniques.

### Unified SNEE ###
_The is the new version, available from the SVN, which allows querying both sensor networks and other sources.  It supports TinyOS 2.1._

  * [Instructions for the SNEE developer on setting up the development environment](SettingUpDeveloperEnvironment.md)
  * [Commands for using the Sensor Network Connectivity Bridge](SensorNetworkConnectivityBridge.md)
  * [SNEE developer coding conventions](SNEEDeveloperCodingConventions.md)
  * [Releasing SNEE](SNEERelease.md)
  * [SNEE test queries](SNEETestQueries.md)

### In-Network SNEE ###
_This is the older version, which only allows querying sensor networks and supports TinyOS 1.x.  It can currently be obtained from the_Downloads_section, but is not longer actively being developed._

  * See the [getting started guide](Getting_Started.md) to get up and running quickly.
  * See a list of [SNEE parameters](SNEE_Parameters.md).
  * Read an overview of the [SNEE architecture](Architecture_Overview.md).
  * Watch an [DBClip](ICDE_DBClip.md) describing SNEE.
  * See an [example query and query plan](ICDE_Example_Query.md).

More technical:
  * [Set up the SNEE development environment](Installing_Developer_Environment.md).
  * [Install TinyOS 1.x on Mac OS X](Installing_TinyOS_1_x_on_Mac_OS_X.md)
  * [Run a Tossim simulation of a SNEE query plan](Running_Tossim1_Simulations.md)
  * [Run an Avrora simulation of a SNEE query plan](Running_Avrora_Simulations.md)
  * [NesC generation rules](NesC_T1_Generation_Rules.md)