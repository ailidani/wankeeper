# WanKeeper

## Quick overview

WanKeeper is distributed coordination service to achieve scalability over WAN. WanKeeper uses a hierarchical token broker architecture for decentralized coordination tasks over multiple data-centers, and it is fully ZooKeeper API compatible.

## Configuration

The configuration file is a single properties file include both ZooKeeper and WanKeeper's parameters. Example config in `conf/wankeeper.properties`.


|      Name      | Default Value |                 Description                |
|:--------------:|:-------------:|:------------------------------------------:|
|       cid      |       1       | cluster id                                 |
|     master     |     false     | is master DC                               |
| master.address |   127.0.0.1   | master DC IPs                              |
|   master.port  |      1735     | master DC ports                            |
|    leasetype   |       2       | is n consecutive access, n=0 is percentage |
|     rwtoken    |      true     | using read/write token                     |



## Getting Started

To start WanKeeper server, run scripts in `bin` folder.
