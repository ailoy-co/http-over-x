//! One HTTP client for a server that is a child process or a URL.
//!
//! A [`Client`] sends the same requests over either transport, which only opening tells apart:
//! [`Client::spawn`] starts the server and talks to it in one HTTP/2 connection over its stdin
//! and stdout, and [`Client::connect`] reaches one at a base URL over TCP. (`Client::embed`
//! serves one in this process, as a test would.) A server answers
//! both with one router, a `tower` service: [`serve_stdio`] serves it over its own stdin and
//! stdout, and [`serve_tcp`], or any HTTP server, over a socket (`serve_tls` where its
//! clients are on other machines, and send no token in the clear).
//!
//! What the two ends share beyond the connection is the protocol this crate keeps, and a
//! server on it follows:
//!
//! - **Resources**: every body is JSON ([`Client::get`], [`Client::post`] and the like), and a
//!   refusal is its status with `{"error": {"code", "message"}}`, read as an
//!   [`Error::Status`].
//! - **Refused for now**: a `429` or a `503` is sent again after its `Retry-After`, for thirty
//!   seconds at most. A `429` whose `code` is `quota` with a `reason` other than `requests` is
//!   not, since waiting frees none of it; a `503` whose `code` is `capacity` says nothing of
//!   the request was done, and any other is sent again only where a second sending leaves what
//!   the first did ([`Client::send`]).
//! - **Authentication**: over TCP, as an [`Auth`] has it: `Authorization: Bearer <token>`, a
//!   command's token printed anew when it runs out or is refused; `Authorization: Basic`; or
//!   the client's certificate, in mutual TLS. Over stdio, none.
//! - **Events**: a job's log is a `text/event-stream` at `{job}/events`, each entry numbered by
//!   its `id` and the last one carrying an `end` member, read from after `Last-Event-ID`
//!   ([`sse`]). `GET {job}` answers whether it is `running` still.
//!
//! [`Auth`], [`Token`] and [`sse::LogEntry`] are always built; the client's end is the `client` feature,
//! on by default, the server's end the `server` feature, and its TLS the `tls` feature.

#[cfg(feature = "server")]
mod args;
mod auth;
#[cfg(feature = "client")]
mod client;
#[cfg(feature = "client")]
mod error;
#[cfg(feature = "server")]
mod server;
pub mod sse;
#[cfg(all(test, feature = "server"))]
mod tests;

/// The `http` crate, whose requests, responses and headers this crate's API is written in.
#[cfg(any(feature = "client", feature = "server"))]
pub use ::http;
#[cfg(feature = "server")]
pub use args::{described_type, from_body};
pub use auth::{Auth, Token};
#[cfg(feature = "client")]
pub use client::{Body, Client, TOKEN_REFUSED, full, json, resolved, segment, segments};
#[cfg(feature = "client")]
pub use error::{Error, Members, refused};
#[cfg(feature = "tls")]
pub use server::{Tls, serve_tls};
#[cfg(feature = "server")]
pub use server::{serve_stdio, serve_tcp};
