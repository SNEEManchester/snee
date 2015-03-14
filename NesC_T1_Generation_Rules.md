# Introduction #

This page describes the rules used by the NesC generator for TinyOS1.x.

### Naming of operators ###

An operator name takes the following format:

op{opID}n{siteID}F{fragID}M, where

  * opID 	= the unique identifier in the query plan operator tree,
  * siteID 	= the identifier of the sites on the sensor network where it is located
  * fragID 	= the identifier of the query plan fragment which the operator is in


### Naming of tuple types ###

  1. An operator’s default type name is based on the operator’s unique identifier, and takes the form TupleOp{xx}, where xx is the id of the operator in the query plan operator tree.
  1. The exception for (1) is when the operator is a deliver, an exchange producer, or the child of either a deliver or exchange producer.  In this case it takes the form TupleFrag{xx}, where xx is the id of the fragment that the operator belongs to.
  1. The exception for (2) is in the case of merge fragments (previously referred to as "recursive fragments"), which must have the same output tuple type as their input tuple type.  Therefore, the default type name for a merge fragment’s output type is its child fragment output type.


### Naming of trays ###

Trays are uniquely identified according to the input tuple default type name, the destination fragment id, the destination site id, and the current site id.

Trays with tuples of the same type which have the same destination fragment id and destination site id are merged into a single tray.  Note that theoretically it is possible for tuples of the type with the same destination fragment to have different destination sites (e.g., in the case of partitioned parallel join).


### RequestData interface ###

A request data interface name is based on the child operator’s output type name, explained in the rules for the systematic naming of tuple types.

### TrayPutTuples interfaces ###

A tray put tuples name is based on the child operator’s output type name, explained in the rules for the systematic naming of tuple types.