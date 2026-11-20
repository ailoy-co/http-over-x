# http-over-x

One HTTP client for a server that is a child process or a URL.

A program that talks to a server often needs it in two places: on the same machine, started when the program starts, and somewhere else, reached over the network. `http-over-x` gives both one client. The server speaks HTTP either way: over its own stdin and stdout when it is spawned, over TCP when it is at a URL. Only the line that opens the client differs.

```rust
let client = Client::spawn(Command::new("my-server"))?;                    // a child process
let client = Client::connect("https://example.com/api", auth, headers)?;   // a URL
```

Both ends are here: the client, and what a server needs to answer it over either.

## Install

The crate is not on crates.io yet. Depend on it by git:

```toml
[dependencies]
http-over-x = { git = "https://github.com/ailoy-co/http-over-x" }
```

A server adds the `server` feature, and `tls` to serve over TLS itself. The client's end is the `client` feature, on by default.

## The client

A request is a path under the server's base, with JSON in and JSON out: `client.get`, `post`, `put`, `patch` and `delete`. A job's log is read as a stream of numbered entries.

```rust
use futures_util::StreamExt;
use http_over_x::{Client, sse};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct Start {
    cmd: Vec<String>,
}

#[derive(Deserialize)]
struct Job {
    id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Line {
    Stdout(String),
    End { code: i32 },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::spawn(std::process::Command::new("my-server"))?;

    let start = Start { cmd: vec!["ls".into()] };
    let job: Job = client.post("/jobs", &start).await?;

    let path = format!("/jobs/{}/events", job.id);
    {
        let mut log = std::pin::pin!(sse::log::<Line>(&client, path, None));
        while let Some(entry) = log.next().await {
            println!("{:?}", entry?.entry);
        }
    }
    client.close().await;
    Ok(())
}
```

What the client does for every request:

- **Retries.** A `429` or a `503` is sent again after its `Retry-After`, for thirty seconds at most, and only where a second sending is safe.
- **Credentials.** Over TCP it sends what an `Auth` says: a bearer token, given or printed by a command that is run again when the token runs out or the server refuses it; a username and password, as `Authorization: Basic`; or a client certificate, in mutual TLS. A token or a password is never sent in the clear to another machine.
- **Refusals.** A status outside `2xx` is an `Error::Status` with the server's `code` and `message`.
- **Logs that survive a break.** Over TCP, a log whose connection breaks is read on from after the last entry it got.

`Client::over` opens a client on a stream the program already holds, with no process started and no credentials sent, as over stdio. A test reaches a server in its own process this way, over an in-memory pipe that `serve_stdio` answers at the other end:

```rust
let (pipe, server) = tokio::io::duplex(64 * 1024);
let (input, output) = tokio::io::split(server);
tokio::spawn(serve_stdio(router, input, output));
let client = Client::over(pipe);
```

## The server

`serve_stdio` answers one HTTP/2 connection over the server's own stdin and stdout with a service, and `serve_tcp` every connection a TCP listener takes with the same one: a router, declared once. `from_body` reads a request's JSON body as its args, and says which member was wrong in plain words.

```rust
use axum::{Json, Router, body::Bytes, http::StatusCode, routing::post};
use http_over_x::{from_body, serve_stdio, serve_tcp};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Start {
    cmd: Vec<String>,
}

async fn start(body: Bytes) -> (StatusCode, Json<Value>) {
    match from_body::<Start>(&body) {
        Ok(start) => (StatusCode::OK, Json(json!({"id": "j1", "cmd": start.cmd}))),
        // `cmd: a list, not "ls"`
        Err(why) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": {"code": "invalid", "message": why}})),
        ),
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let router = Router::new().route("/jobs", post(start));
    match std::env::args().nth(1) {
        // At an address: over TCP.
        Some(address) => {
            let listener = tokio::net::TcpListener::bind(address).await?;
            serve_tcp(router, listener).await
        }
        // Spawned: over its own stdin and stdout.
        None => serve_stdio(router, tokio::io::stdin(), tokio::io::stdout()).await,
    }
}
```

The service is any `tower` one from a request to a response, and axum's router is only the one shown: a plain function serves through `tower::service_fn`. Over stdio the server's stdout is the connection, so what it logs goes to stderr. The requests, responses and headers are the `http` crate's, which this crate re-exports as `http_over_x::http`. `sse::Log` keeps a job's entries and sends them as an event stream to any number of readers, each from where it left off.

Over TCP, `serve_tcp`'s connections are in the clear, which a client sends its token or its password over only to a server on its own machine. For clients elsewhere, `serve_tls` (the `tls` feature) serves the same service over TLS, with the server's certificate and its key in PEM files:

```rust
use http_over_x::{Tls, serve_tls};

// Any client:
serve_tls(router, listener, &Tls::new("server.pem", "server.key")).await
// Only those showing a certificate this authority signed, in mutual TLS:
serve_tls(router, listener, &Tls::new("server.pem", "server.key").client_ca("clients-ca.pem")).await
```

Neither checks a token or a password: that is the service's, as a layer of its router.

## The protocol

The two ends share a few conventions, and a server on this crate follows them:

| | |
| --- | --- |
| Bodies | JSON, in and out. |
| Refusals | The status, with `{"error": {"code": "...", "message": "..."}}`. Other members of `error` reach the client as they are. |
| Refused for now | `429` and `503`, with `Retry-After` in seconds. A `429` whose `code` is `quota` with a `reason` other than `requests` is not sent again. A `503` whose `code` is `capacity` says nothing of the request was done. |
| Authentication | Over TCP, `Authorization: Bearer <token>`, `Authorization: Basic <credentials>`, or the client's certificate in mutual TLS. Over stdio, none: the client started the server. |
| Logs | A `text/event-stream` at `{job}/events`. Each entry has an `id`, counted from `1`, and the last has an `end` member. A reader sends `Last-Event-ID` to read on. `GET {job}` answers whether the job is `running`. |

## Status

The crate is new, and its API may change.

## License

Apache-2.0. See [LICENSE](LICENSE).
