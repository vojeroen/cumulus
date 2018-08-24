# Cumulus distributed file storage

This project is currently in early development stages. Not all features are already implemented, and there are still some serious constraints in the usage. The project, and its nimbus dependency, are also not yet properly packaged.

## Cluster topology
There are two different types of nodes: brokers and workers. The brokers are the connection points for new requests and then distribute incoming request over the workers. Multiple workers may be launched in parallel.

The project contains two different broker-worker pairs: proxy and storage. 

### Proxy broker
The proxy broker is the central connection point for all client requests.

### Proxy workers
The proxy workers handle client requests. Files are erasure-coded and distributed over the storage nodes. It uses mongodb for persistent storage of file metadata.

### Storage proxy
The central connection point for all storage requests by the proxy workers. All storage nodes connect to this storage proxy.

### Storage nodes
The nodes that contain the actual stored data. These nodes may be located anywhere, and may make their connection to the storage proxy over an unsecured network, such as the internet.

## Features
* File upload, listing and download.
* Support for erasure code.
* Prevent unwanted file modifications while stored in the cluster.
* Storage workers don't have to run in a secure network.
* All connections between the storage broker and storage workers are encrypted and authenticated. 

## (Current) limitations
* No automated tests.
* No documentation.
* No user-friendly client.
* No client authentication.
* No automated file checking and repair yet (although the file repair algorithm can be launched manually).
* The brokers are currently single points of failure.
* Files may not be too large, as they are handled in memory.

## Installation

The usage has only been tested on Ubuntu. The following packages are required:
* `libzmq5`
* `liberasurecode-dev`
* `mongodb`

A python 3 virtual environment must be created in the project root, in the directory `[project root]/venv`. The packages from `requirements.txt` must be installed in this virtual environment.

The nimbus project must be available in the same directory as the cumulus project: `[project root]/../nimbus`.   

## Initialize cluster
Run the following command in the project root to initialize and start a new cluster: `PYTHONPATH=.:../nimbus:$PYTHONPATH ./init-cluster.py`. This cluster is only intended for development purposes.

## Use cluster
Use the scripts `cluster/cluster-{start,stop,restart}.sh` to start, stop and restart your cluster.

The file `test/proxy-file-store.py` can be run to upload and download a file to the cluster. There are no additional helpers scripts yet.
