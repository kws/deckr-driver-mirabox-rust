# deckr-driver-mirabox-rust

Rust MiraBox bridge client for Deckr.

This repo keeps the existing `deckr-mirabox-manager` binary identity and embeds its own
layout assets from `layouts/built-in`, so it no longer depends on files living in the
Python MiraBox repo at build time.

## Compatibility

This repo stays a pure Cargo project. Controller compatibility is documented and tested
against the current Deckr hardware bridge contract rather than expressed as a Python
package dependency.
