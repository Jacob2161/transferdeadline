# Changelog

This module follows semantic versioning. Before v1, a minor version may
change the API; each such change says what callers must do.

## v0.3.1 (2026-11-28)

- CI runs every step in bash, so the Windows runners read
  `-modfile=tools/go.mod` whole. No API change.
- The README says the module requires Go 1.27.

## v0.3.0 (2026-11-28)

- Requires Go 1.27, the newest Go release; Go 1.26 is no longer
  supported.
- CI runs staticcheck and govulncheck at the versions pinned in
  `tools/go.mod`, a module of its own that importers never see.

## v0.2.0 (2026-11-27)

This is a breaking change and ships as v0.2.0.

- `Middleware` takes `*MiddlewareArgs` instead of a `MinimumProgress`
  value. `MinimumProgress` and `DefaultMinimumProgress` are gone; the
  defaults are the constants `DefaultBodyWindow`, `DefaultBodyBytes`, and
  `DefaultWriteWindow`. To upgrade:
  - `Middleware(DefaultMinimumProgress())` becomes `Middleware(nil)`.
  - `Middleware(MinimumProgress{...})` becomes
    `Middleware(&MiddlewareArgs{...})` with the same field names.
  - A zero field now means its default. A window that was zero to turn a
    deadline off must now be negative.
- CI also checks formatting, `go mod tidy -diff`, and `govulncheck`.

## v0.1.1 (2026-11-27)

The same code as v0.1.0, tagged again because the module proxy failed to
fetch v0.1.0.

## v0.1.0 (2026-11-27)

First release.

- `Middleware` stops clients that stall a transfer: a request body that
  arrives slower than a minimum rate fails its read with `ErrBodyTooSlow`,
  and a response write the client stops reading fails after the write
  window.
- Deadlines are armed only while a read or write is blocked, so handler
  time never counts against the client, and they work per connection over
  HTTP/1.1 and per stream over HTTP/2 and h2c.
