Problem description
===================


The goal of the project: to develop the simulation framework for study
processes in DDS and establish quantitative laws related to CAP-theorem.

DDS: distributed data stores


Entities
========

- Nodes are servers in DDS topology (1..n)
- Link is the binding for two nodes in the DDS topology (0..m)
- Dataunit is the node component, which is stored there
- Replica is the copy of dataunit that is broadcasted through the DDS to nodes that have this dataunit and need a new replica.


The minimal number of nodes is 1, when DDS consists of just one server.
In this case the number of links will be 0.


Needed services
===============

Services will provide all necessary behaviour for DDS projects


Graph service
-------------

This will be a set of modules that enhance the DDS simulation process.
It will help to model experiments on DDS to ensure the proper work of
broadcast algorithm and other communication processes in the datastore.
It will provide with animations, pictures of processes indicated above.

Allowed formats: gif, jpg, png, pdf.

Tools: networkx, matplotlib.


For now there should be created programs to animate given graph (
try on the simplest).
Later on the information about the graph should be read
from config (this will be section called something like ``topology`` that
will specify the DDS topology.
DDS topology formation may be discussed.
Possible current solutions:
- standard graph models (regular, random, small world etc.)
- third-party software to model topology (OPNet) and put to config.
It also has a possibility to create topologies following standard network
models.


Graph modeling should help to measure and realize the consistency
(later on availability and partition tolerance).


Experiment service
------------------

The main service that will start all other services.
Will collect needed data from Node service and send them to graph service
and metrics service appropriately, where these values can be used in the
graph representation and metrics calculation.


Node service
------------

In some definition DDS is a set of servers that are communicating
between each other through links (by read/write messages). Another their
purpose, not less important than, is listening to read/write requests from DDS
users.
Thus, the nodes need to be activated somehow.
But this is just a simulation, so the node does not need to be a thread, then
some system to start nodes need to be activated. This will be
powered by variety of algorithms for broadcasting in distributed system like
gossip etc.

Each node will be a fake server that listen to messages from other nodes.
(user requests is considered here as message also, because there is no
difference where the requests came from, either it would be the neighbor-node
or a DDS user itself). Actually, for now, for simplicity, some random node can
be a user-creator itself. So the server should process:


- POST request - attempt to create a new dataunit.
- PATCH request - attempt to modify a dataunit.
- DELETE request - attempt to delete a dataunit.

- GET request - attempt to get the info about a dataunit.


As you can see, in general, raising the level of abstraction, there are
actually two types of requests:
write (POST, PATCH, DELETE, also PUT and etc., but they are not considered for
now) and read (GET).


Write request will create or modify a dataunit and send it (may be in hash) to
other neighbors.

They will consider this message as a POST request.

This way this dataunit may be randomly distributed through DDS and
created on some nodes (here technology of replication need to be investigated
if it is random replica's distribution or how to choose what nodes own what
dataunits).

Read request is simple as that, it just returns the requested replica of
dataunit.

Here some epidemic algorithms may be used to distributed the replicas across
the DDS.

The node-link topology will be again taken from config files.

Now let us clearify what is a fake server (node).
When system (simulation) sends message to node, it should process it
and broadcast to others in parallel.
So the number of workers (for now they can be simple threads or processes,
later on if needed to avoid GIL in threading or performance overhead in
multiprocessing the technology called asyncio or greenlet can be used).


Calculation metrics service
---------------------------

Above all of this behaviour there should be the process that is calculating:

- the time till all of dataunits are consistent after change
- the number of nodes having up-to-date information after a time moment t (specified somehow)
- the distribution of broadcast time that is considered in each of experiments
- the distribution of node frequency (on how many nodes the same dataunit is stored)

Future possible metrics:

- the alive time of the node
- the alive time of the link
- occupance of the link (how often dataunits are transmitted through this link)
- occupance for all links (how many links in DDS ae busy at time moment t).


All calculated metrics may be saved to the database and later be shown at ssite.