### An Example Query ###
```
SELECT RSTREAM river.time, hilltop.rain, river.depth 
FROM river[NOW], hilltop[AT NOW - 15 MINUTES] 
WHERE hilltop.rain > 500 AND river.rain < hilltop.rain 

ACQUISITION RATE = EVERY 15 MINUTES 
MAX DELIVERY TIME = 24 HOURS
```

Our application requirements are inspired by an environmental monitoring system which may be used by hydrologists interested in assessing the hydro-dynamics of surface water drainage.

The goal of this query is to obtain, every 15 minutes, timestamped readings of the current rainfall and river depth, and the rainfall at the hilltop 15 minutes previously, in cases where the rain at the hilltop is above a certain threshold. We are only interested in cases where it is currently raining less at the river than it was at the hilltop 15 minutes ago (to reduce the likelihood that any increase in river depth was caused by rain on the river itself).

![http://dias-mc.cs.manchester.ac.uk/topology.jpg](http://dias-mc.cs.manchester.ac.uk/topology.jpg)

We pose the SNEEql query shown against the sensor network whose topology shown.
Note that Node 0 is the sink, nodes 5,6 7 and 9 have river sensors (and can measure rainfall and river depth), and node 4 is a hilltop sensor (and can measure rainfall).

### Single-site Phase ###

![http://dias-mc.cs.manchester.ac.uk/paf.jpg](http://dias-mc.cs.manchester.ac.uk/paf.jpg)

In the single-site phase, the declarative query text is parsed into an internal form and type-checked. The internal form is an abstract algebra, based on the relational algebra. The algebra is enriched with windows on streams, which impose a definite cardinality on the inputs. This allows blocking operations (such as joins) to be well-defined. The algorithm selection step gives rise to the physical-algebraic form shown.

### Routing ###

![http://dias-mc.cs.manchester.ac.uk/rt.jpg](http://dias-mc.cs.manchester.ac.uk/rt.jpg)

This step determines a routing tree for communication links that the data flows in the Physical Algebraic Form can then rely on. This is achieved by computing a steiner tree, i.e., a tree of minimal cost derived from the network topology graph with a required set of nodes, using any additional nodes which are necessary. The resulting RT is currently displayed; it consists of the source nodes for the river and hilltop extents, the sink node where the data is to be delivered, and nodes 2 and 3 used solely to relay results.

### Partitioning ###

![http://dias-mc.cs.manchester.ac.uk/faf.jpg](http://dias-mc.cs.manchester.ac.uk/faf.jpg)

Partitioning breaks up the physical-algebraic form into fragments by inserting exchange operators, using semantic criteria such as attribute sensitivity, and also identifying edges in the PAF with lower output sizes. As we can see, the PAF has been partitioned into four fragments, denoted F1...F4.

### Where-scheduling ###

![http://dias-mc.cs.manchester.ac.uk/daf.jpg](http://dias-mc.cs.manchester.ac.uk/daf.jpg)

Where-scheduling decides which query plan fragments are to run on which routing tree nodes. Fragments are placed with the aim to reduce the amount of data transmitted. This step results in the distributed-algebraic form (DAF) of the query, in which each fragment is allocated to a set of sites, displayed next to the fragment identifier.

### When-scheduling ###

![http://dias-mc.cs.manchester.ac.uk/agenda.jpg](http://dias-mc.cs.manchester.ac.uk/agenda.jpg)

When-scheduling stipulates execution times for each fragment. An agenda is generated based on the acquisition rate and delivery time specified by the user, aiming to buffer as many tuples as possible prior to transmission.

The agenda for the example query currently is displayed. In an agenda, there is a column for each site and a row for each time when a task is started. A task is either the evaluation of a fragment, or a communication event, denoted by tx n or rx

Note that in the example, leaf fragments F3 and F4 are repeated 36 times in each agenda evaluation, and that the agenda repeats every 9 hours. Such decisions are reached in light of the QoS specified by the user.

### Code generation ###
```
configuration QueryPlanNode15 { 
} 
implementation 
{ 
components F1n15C, F2n15C, GenericC 
F1n15C.TrayGet -> trayF3_F1n15_n15M 
F1n15C.TrayPutF1_F0n16_n15M -> tray 
F2n15C.TrayGet -> trayF3_F2n15_n15M 
F2n15C.TrayPutF3_F1n15_n15M -> tray 
Main.StdControl -> GenericComm.Con 
Main.StdControl -> QueryPlanM.StdC 
QueryPlanM.AgendaTimer -> TimerC.Ti 
QueryPlanM.DoTaskF1n15C -> F1n15C.D 
QueryPlanM.DoTaskF2n15C -> F2n15C.D 
```

Code Generation generates executable code for each site based on the distributed algebraic form, routing tree and agenda. The current implementation of SNEEql generates nesC code for execution in TinyOS, a component-based, event-driven runtime environment designed for wireless sensor networks.

### Example Query Results ###

```
4: hilltop.rain 600 acquired at time 0 
--- 
4: hilltop.rain 720 acquired at time 15 
6: river.rain 38 acquired at time 15 
6: river.depth 45 acquired at time 15 
9: river.rain 610 acquired at time 15 
9: river.depth 60 acquired at time 15 
--- 
6: river.rain 42 acquired at time 30 
6: river.depth 70 acquired at time 30 
--- 
0: result (time:15, rain:600, depth:45) 
0: result (time:30, rain:720, depth:70) 
```

The results for the example query are shown above.

### Benefits of our Approach ###

We have described the architecture of the SNEEql optimizer, a sensor network query optimizer based on the two-phase optimization architecture prevalent in DQP. This approach offers several benefits, such as
  * The ability to pose queries using a rich, expressive language based on classical stream query languages, and
  * The ability to schedule different workloads to different sites in the network, enabling more economical use of resources such as memory, and potentially support for heterogeneity in the SN; and
  * The ability to empower the user to trade-off conflicting qualities of service such as network longevity and delivery time.
We also compared well with TinyDB in an empirical evaluation.

These benefits suggest that, potentially, much can be learned from DQP optimizer architectures in the design of SN optimizer architectures.