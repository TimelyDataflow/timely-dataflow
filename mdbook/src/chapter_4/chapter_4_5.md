# Custom Datatypes

Timely dataflow allows you to use a variety of Rust types, but you may also find that you need (or would prefer) your own `struct` and `enum` types.

Timely dataflow provides two traits, `Data` and `ExchangeData` for types that timely dataflow can transport within a worker thread and across threads.

## The `Data` trait

The `Data` trait is essentially a synonym for `Clone+'static`, meaning the type must be cloneable and cannot contain any references with other than a static lifetime. Most types implement these traits automatically, but if yours do not you should decorate your struct definition with a derivation of the `Clone` trait:

```rust
#[derive(Clone)]
struct YourStruct { .. }
```

## The `ExchangeData` trait

The `ExchangeData` trait is more complicated, and is established in the `communication/` module. There are two options for this trait, which are determined by whether you use the `--bincode` feature at compilation, or not.

*  If you use `--bincode` then the trait is a synonym for

    ```rust
    Send+Sync+Any+serde::Serialize+for<'a>serde::Deserialize<'a>+'static
    ```

    where `serde` is Rust's most popular serialization and deserialization crate. A great many types implement these traits. If your types does not, you should add these decorators to their definition:

    ```rust
    #[derive(Serialize, Deserialize)]
    ```

    You must include the `serde` crate, and if not on Rust 2018 the `serde_derive` crate.

    The downside to the `--bincode` flag is that deserialization will always involve a clone of the data, which has the potential to adversely impact performance. For example, if you have structures that contain lots of strings, timely dataflow will create allocations for each string even if you do not plan to use all of them.

*  If you do not use the `--bincode` feature, then the `Serialize` and `Deserialize` requirements are replaced by `Abomonation`, from the `abomonation` crate. This trait allows in-place deserialization, but is implemented for fewer types, and has the potential to be a bit scarier (due to in-place pointer correction).

    Your types likely do not implement `Abomonation` by default, but you can similarly use

    ```rust
    #[derive(Abomonation)]
    ```

    You must include the `abomonation` and `abomonation_derive` crate for this to work correctly.
