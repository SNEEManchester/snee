# SNEE Architecture Overview #

The SNEEql (SNEE for Sensor NEtwork Engine) query optimizer is a novel query optimizer for sensor networks which combines a rich, expressive query language with a software architecture based traditional distributed query processing techniques.
The optimization steps cover all the query optimization phases that are required to map a declarative query to executable code.

![http://dias-mc.cs.manchester.ac.uk/query-stack-small.png](http://dias-mc.cs.manchester.ac.uk/query-stack-small.png)

The query stack we present is an extension of the classical two-phase optimization approach from distributed query processing. It is composed of a single-site phase (comprising the first 3 steps in gray boxes), and a subsequent multi-site phase (comprising the next 4 steps) in which a distributed query plan is generated.
The steps in the single-site phase are similar to those in distributed query processing, and are not the focus of our research. However, the multi-site phase has been adapted. We introduce the routing step, an important consideration in sensor networks as the paths by which tuples travel can have a large impact on the cost of a query plan. The timing of tasks is also important in a sensor network, as radio communications require co-ordination between nodes in the network, so we have introduced the when-scheduling step. The steps in the multi-site phase are further described ahead.

The final phase is code generation, which grounds the execution on the concrete software and hardware platforms available.

Note that optimizer decisions are informed by metadata, such as the network topology, resources on nodes such as memory available, and predictive cost models.