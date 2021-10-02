# Containers

Dataflow edges in Timely can send arbitrary data. The most common encapsulation are vectors, which store elements
consecutively in memory.

Using vectors to transfer data is the appropriate choice for most use-cases, but there are specific applications that
can benefit from sending differently encapsulated data. For example, an application could choose to represent data in a
transposed or columnar format in memory, which cannot be represented by simple vectors.
For this reason Timely's network and core operator framework provides an abstraction over container types.

Containers in Timely serve several purposes.
Most importantly, they contain data passed on dataflow edges between operators.
On top of this, they wrap memory allocations for owned data, or serialized representations depending on the communication layer.
For progress tracking, they provide a means to count the number of elements stored in a container.
Channels to distribute data amongs workers require the ability to partition containers into disjoint parts.
Let's analyze the last three points separately.

## Managing memory allocations

Containers provide memory allocations to the data they contain.
Once an operator consumes a container's data or the communication layer serializes its contents, the allocation can be re-used.
For this purpose, containers can be transformed to a type capturing the allocation.
The allocation can be passed upstream and can be re-populated.

For this purpose, containers are defined as traits with an associated allocation type and a function `hollow` to transform it:

```rust,ignore
trait Container {
    type Allocation: IntoAllocated<Self>,
    fn hollow(self) -> Self::Allocation
}
```

The communication layer and core Timely operators try to pass allocations upstream, where reasonable.
The allocations can be reassembled based on provided data:

```rust,ignore
trait<T> IntoAllocated<T> {
    fn assemble(self, allocated: RefOrMut<T>) -> T;
}
```

## Progress tracking

To enable progress tracking, the container type describes the length of its contents:
```rust,ignore
trait Container {
    fn len() -> usize;
    fn is_empty() -> bool;
}
```

The length describes in how many elements a container could be sharded.
The `is_empty` function returns `true` if the length is zero.

For exchange channels, the length of the input buffer must be equal to the sum of the lengths of the output buffers.

## Data exchange

Data exchange is currently only provided for vector-based containers.
For other containers, a parallelization contract has to be specified.
