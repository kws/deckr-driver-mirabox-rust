# deckr-driver-mirabox-rust

Standalone Rust MiraBox manager for Deckr.

This repo keeps the existing `deckr-mirabox-manager` binary identity and embeds its own
layout assets from `layouts/built-in`, so it no longer depends on files living in the
Python MiraBox repo at build time.

## Compatibility

This repo stays a pure Cargo project. Controller compatibility is documented and tested
against the Deckr controller `0.2.x` line rather than expressed as a Python package
dependency.
