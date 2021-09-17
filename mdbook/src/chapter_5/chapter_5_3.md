# Container types
Container types can be disassembled into a hollow container

Containers can be recreated from a hollow container, ready to accept data

Specialized containers:
* Push single elements -> Needs to know what's inside
* ExchangeContainers -> Need to know how to shard into multiple disjoint containers, while maintaining progress tracking properties



Though exercises

Sending a container over the network
* write container to [u8] buffer (abom/serialization)
* send buffer over the network
* dissasemble container -> requires functionality to clean container

Sending a container to muliple downstream operators
* Clone container for all but one receiver, send
* Send last copy to remaining receiver
* Here, we have n allocations, but what allocation do we want to retain?