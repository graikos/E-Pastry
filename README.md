# E-Pastry
Pastry DHT implementation in Python.

## Description
E-Pastry is a python implementation of the Pastry protocol, a peer-to-peer lookup service.

## Features
This is an implementation of the Pastry overlay network. Specifically, the following features have been implemented:

- **Seed Server**: The new node learns the identity of an existing E-Pastry node by contacting the [Seed Server](https://github.com/graikos/E-Pastry-Seed). The seed server returns the node which is closest to the new node, based on the proximity metric.
- **Proximity-based routing**: By using a proximity metric, the protocol is able to route to nodes that are close based on that metric. This would reduce latency in a real-world scenario. By default, random locations are chosen for each node and the distance between them is used as a metric, but this can be changed to use an existing proximity function.
- **Concurrent Joins**: Multiple nodes can join the network concurrently, and the join routine ensures they are initialized appropriately.
- **Handling Massive Node Failures**: Network consistency is preserved after multiple nodes fail simultaneously. During lookup, each node will replace failed nodes it is in contact with, whenever necessary.
- **Data Storage**: Each node can hold data in key-value pairs. Any node can be contacted to store such a pair, but the pair will be stored in a node determined by the network.
- **Load Balancing**: Data is shared between multiple nodes, such that no one node holds a much larger amount of keys than others. In this way, the load is balanced between the nodes.
- **Custom Parameters**: By editing the `config/params.json` file, various parameters regarding the network and scripts can be customized. It is required that all nodes run with the same parameters.

## Simulation Scripts
All simulation scripts assume nodes are running locally, on the same machine (`localhost`).

- **Mass Node Join**: Adds multiple nodes to the network.
- **Mass Node Leave**: Removes a percentage of the network's nodes. Before removal, each node will return all the keys it holds in storage. These will be stored in a file, `scripts/lost_keys.dat`. On lookup, the `mass_data` script will not lookup any of those keys.
- **Mass Data**: Adds or looks up data in the network. The input file name must be specified as the first parameter. The required format is JSON. The second parameter should either be `insert`/`i`, or `lookup`/`l`. The former will add the data to the network, whereas the latter will look up the data. Both scripts will display the failure percentage of their respective requests. 
 
## Potential Improvements
- **Data Replicas**: For the Pastry network to work consistently, it is necessary to store data replicas. Specifically, since the routing procedure performed by the network is equivalent to a localized search, depending on the starting node, it is possible that the closest node is unable to be located, thus either storing or looking up a key in a non-optimal node in ID-space. This results in more frequent lookup failures. According to [1], the pastry network is able to locate the closest node to a key in ID-space in over 75% of cases. According to experimental data, our implementation confirms this number, as lookup failures after insertion range from 10% to 28%. To further reduce this number, data replicas are necessary.
- **Data transfer**: As new nodes join the network, data should be appropriately transfered to them, as well as possibly be deleted from existing nodes. This has not been implemented, since it needs to work in conjunction with the replication procedure.

## References
[1] Rowstron, A., Druschel, P. (2001). Pastry: Scalable, Decentralized Object Location, and Routing for Large-Scale Peer-to-Peer Systems. In: Guerraoui, R. (eds) Middleware 2001. Middleware 2001. Lecture Notes in Computer Science, vol 2218. Springer, Berlin, Heidelberg. https://doi.org/10.1007/3-540-45518-3_18

## See Also
An equivalent implementation of the Chord protocol, which also served as a basis for this project, can be found [here](https://github.com/notTypecast/E-Chord).