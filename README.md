# deckr-driver-mirabox-rust

Rust MiraBox bridge client for Deckr.

This repo keeps the existing `deckr-mirabox-manager` binary identity and embeds its own
layout assets from `layouts/built-in`, so it no longer depends on files living in the
Python MiraBox repo at build time.

## Compatibility

This repo stays a pure Cargo project. Controller compatibility is documented and tested
against the current Deckr hardware bridge contract rather than expressed as a Python
package dependency.

## Build

For normal local development:

```sh
cargo build
cargo test
```

If you use `just`, the same commands are available as:

```sh
just build
just test
```

## Cross-platform builds

The repo includes the cross toolchain setup that used to live in `sidepanel`:

- `Cross.toml`
- `docker/rust-cross/*.Dockerfile`
- `just cross-images`
- `just release`

Build the custom `cross` images first:

```sh
just cross-images
```

Then build release binaries for the supported Linux targets:

```sh
just release
```

This currently produces release binaries for:

- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`
- `arm-unknown-linux-gnueabihf`

The `arm-unknown-linux-gnueabihf` build keeps the existing ARMv6/Raspberry Pi compatible
flags from the old setup.
