# Timely WASM example (wasm-bindgen)

A minimal example of running a vanilla timely dataflow program in a browser
using [`wasm-bindgen`]. It mirrors `timely/examples/threadless.rs`, with
`println!` replaced by `console::log_1` so output appears in the browser's
developer console.

## Why this exists

It documents the WASM-compatible entry point for timely: a single-threaded
worker constructed manually with `Worker::new(_, Thread::default(), None)`.
No `execute`, no `initialize`, no thread spawning, no networking, and no
`std::time::Instant` (the worker is constructed with `None` for the timer).

If you can build and run this example, you have a working timely-on-WASM
toolchain. Anything more elaborate (multiple workers, exchange channels, etc.)
is not currently supported on `wasm32-unknown-unknown`.

## Building

From this directory:

```sh
wasm-pack build --target web
```

The output `.wasm` file and JavaScript glue will land in `pkg/`.

If you don't have `wasm-pack`, install it with:

```sh
cargo install wasm-pack
```

You can also do a quick compile-only check (without the wasm-bindgen
post-processing) using cargo directly:

```sh
cargo check --target wasm32-unknown-unknown
```

This is what CI runs to verify the example continues to build.

## Running

You'll need an HTML page that imports the generated module:

```html
<!DOCTYPE html>
<html>
  <head>
    <title>Timely WASM example</title>
  </head>
  <body>
    <script type="module">
      import init from "./pkg/timely_wasm_bindgen_example.js";
      init();
    </script>
  </body>
</html>
```

Serve the directory with any static HTTP server (e.g., `python3 -m http.server`)
and open the page in a browser. Open the developer console; you should see
ten "timely says: ..." log lines.

## Why this crate is excluded from the workspace

This example has dependencies (`wasm-bindgen`, `web-sys`) that are only
meaningful when targeting `wasm32`. To avoid pulling them into the main
workspace's dependency tree, this crate is intentionally excluded from the
workspace via `[workspace.exclude]` in the root `Cargo.toml`. CI builds it
explicitly with `--manifest-path` (see `.github/workflows/test.yml`).

[`wasm-bindgen`]: https://rustwasm.github.io/wasm-bindgen/
