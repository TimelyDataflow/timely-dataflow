# Containers

Dataflow edges in Timely can send arbitrary data.
The most common encapsulation are vectors, which store elements consecutively in memory.
However, the core Timely APIs support passing data that follows a simple `Container` pattern.

Using vectors to transfer data is the appropriate choice for most use-cases, but there are specific applications that can benefit from sending differently encapsulated data.
For example, an application could choose to represent data in a transposed or columnar format in memory, which cannot be represented by simple vectors.
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
For other containers, a custom parallelization contract has to be specified.

## Example: Vectors

Let's show how the `Container` trait can be implemented for `Vec<T>`.
The allocation type for a vector is its own type.
The `hollow` operation ensures that no elements remain and returns `self` as the allocation.

```rust,ignore
impl<T: Clone + 'static> Container for Vec<T> {
    // Recycling the allocation passes an empty vector
    type Allocation = Self;

    fn hollow(mut self) -> Self::Allocation {
        // clear contents if it wasn't done by the client
        self.clear();
        self
    }

    fn len(&self) -> usize {
        Vec::len(&self)
    }

    fn is_empty(&self) -> bool {
        Vec::is_empty(&self)
    }
}
```

Turning an allocated vector into a pupulated vector uses the following code path.
Depending on whether the provided data is a mutable or immutable reference, a different way of turning the data into owned data is selected.

```rust,ignore
impl<T: Clone> IntoAllocated<Vec<T>> for Vec<T> {
    #[inline(always)]
    fn assemble(mut self, ref_or_mut: RefOrMut<Vec<T>>) -> Vec<T> {
        match ref_or_mut {
            RefOrMut::Ref(t) => {
                // Clone for immutable references
                self.clone_from(t);
            }
            RefOrMut::Mut(t) => {
                // Pointer-swap for mutable references
                ::std::mem::swap(&mut self, t);
            },
        }
        self
    }

    #[inline(always)]
    fn assemble_new(ref_or_mut: RefOrMut<Vec<T>>) -> Vec<T> {
        // Construct a default-sized vector and use it to assmble
        Self::with_capacity(buffer::default_capacity::<T>()).assemble(ref_or_mut)
    }
}
```
