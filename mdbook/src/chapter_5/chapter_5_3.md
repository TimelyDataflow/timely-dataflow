# Containers

On a high level, Timely sends along dataflow edges. Underneath however, it needs to send and process batches of data to amortize processing overhead. In this section, we walk through the container abstraction, which is how Timely interfaces with batches of data.

A container represents a batch of data with an interface to enable progress tracking and extracting data. Progress tracking requires that containers have a length, which does not change independent of container modifications. Reading data out of a container is useful for several high-level operators and sessions, but not otherwise required.

Progress tracking requires that the length of a container stays the same between sending and receiving a container. The length must be additive, meaning that two containers combined must result in a container that has the sum of the lengths of the original containers. (Vectors satisfy the properties, but sets do not.)

The container trait defines two associated types, `Item` for moving data, and `ItemRef` for reading data. Both are lifetimed, meaning an implelemntation can select to return owned data or references, depending on the constraints imposed by its data representation. Related, the trait defines an `iter()` function that reads the contents, and a `drain()` that removes the contents.

In timely, we decouple write- and read-halves of a containers, as expressed by the `PushInto` trait. It describes how a target can absorb a datum as self, without requiring that the container knows about self. This allows a container to absorb an owned type and merely present dereferenced variants of the owned type when reading. For example, a container could accept `String` objects, but only permit reading `&str`.

## Capacity considerations

Timely wants to send containers of consistent size. In the past, this was a vector of 1024 elements, but this concept scales poorly to data that has a different layout.

What we want to achieve is:
* Timely makes progress when processing large amounts of data. An implementation that buffers all data and only releases it once time advances does not have this property.
* Containers have consistent size to minimize stress for the memory allocator.

## Core operators

In Timely, we provide a set of `core` operators that are generic on the container type they can handle.
In most cases, the `core` operators are an immediate generalization of their non-core variant, providing the semantically equivalent functionality.

## Limitations

* Explain why it's hard to build container-generic operators from smaller operators (unless we have higher-kinded types).

Each operator over arbitrary containers either returns an equal container, or needs to be parameterized to indicate the desired output type.
This is problematic when composing a high-level operator from smaller building blocks, such as the broadcast operator.
The broadcast operator for vectors maps each datum to a `(target, datum)` pair, where it repeats `datum` for each target.
Subsequent operators exchange the data and unwrap the `datum` from the tuple.
Defining this with specific containers in mind is simple, however, we do not have a mechanism to express this for arbitrary containers.
