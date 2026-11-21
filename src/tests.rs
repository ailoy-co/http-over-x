//! Both ends together: what a client and a server share, each test run once over TCP and once
//! over an in-memory HTTP/2 connection as stdio serves one. What only one transport has, such
//! as the credentials sent over TCP, is tested where it is written.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU32, Ordering::SeqCst},
};

use ::http::{
    HeaderMap, HeaderName, HeaderValue, Method, StatusCode,
    header::{CONTENT_TYPE, RETRY_AFTER},
};
use axum::{
    Json, Router,
    extract::{Path, State},
    response::IntoResponse as _,
    routing::{any, get, post, put},
};
use futures_util::StreamExt as _;
use rstest::rstest;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{Auth, Client, Error, full, segments, serve_tcp, sse};

/// A transport a test's router is served over, and its client asks over.
#[derive(Clone, Copy)]
enum Over {
    Tcp,
    Stdio,
}

use Over::{Stdio, Tcp};

impl Over {
    /// A client of `router`, served over this transport: on this machine's loopback, or over
    /// an in-memory pipe as a spawned server's stdin and stdout are.
    async fn client(self, router: Router) -> Client {
        match self {
            Tcp => {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let url = format!("http://{}/", listener.local_addr().unwrap());
                tokio::spawn(serve_tcp(router, listener));
                Client::connect(&url, Auth::None, HeaderMap::new()).unwrap()
            }
            Stdio => Client::embed(router),
        }
    }
}

/// An entry of the tests' logs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
enum Line {
    Stdout(String),
    End { code: i32 },
}
/// GET, POST, PUT, PATCH and DELETE carry JSON there and back.
#[rstest]
#[tokio::test]
async fn json_methods(#[values(Tcp, Stdio)] over: Over) {
    let echo = |method: Method, body: axum::body::Bytes| async move {
        let sent = serde_json::from_slice::<Value>(&body).ok();
        Json(json!({"method": method.as_str(), "sent": sent}))
    };
    let client = over.client(Router::new().route("/echo", any(echo))).await;
    let sent = json!({"cmd": ["ls", "-l"]});
    assert_eq!(
        client.get::<Value>("/echo").await.unwrap(),
        json!({"method": "GET", "sent": null})
    );
    assert_eq!(
        client.post::<Value>("/echo", &sent).await.unwrap(),
        json!({"method": "POST", "sent": sent})
    );
    assert_eq!(
        client.put::<Value>("/echo", &sent).await.unwrap(),
        json!({"method": "PUT", "sent": sent})
    );
    assert_eq!(
        client.patch::<Value>("/echo", &sent).await.unwrap(),
        json!({"method": "PATCH", "sent": sent})
    );
    client.post_empty("/echo", &sent).await.unwrap();
    client.delete("/echo").await.unwrap();
    client.close().await;
}
/// A non-2xx answer becomes `Error::Status` with the server's `code` and `message`.
#[rstest]
#[tokio::test]
async fn error_response(#[values(Tcp, Stdio)] over: Over) {
    let router = Router::new().route(
        "/volumes/{name}",
        get(|| async {
            let body = json!({"error": {"code": "not_found", "message": "none"}});
            (StatusCode::NOT_FOUND, Json(body))
        }),
    );
    let client = over.client(router).await;
    let error = client.get::<String>("/volumes/nothing").await.unwrap_err();
    let Error::Status(404, message, _) = &error else {
        panic!("not a 404: {error:?}");
    };
    assert_eq!(message, "not_found: none");
    assert_eq!(error.code(), Some("not_found"));
}
/// A header set on the client is sent with every request until it is unset.
#[rstest]
#[tokio::test]
async fn set_header(#[values(Tcp, Stdio)] over: Over) {
    let project = HeaderName::from_static("x-project");
    let named = project.clone();
    let router = Router::new().route(
        "/project",
        get(move |headers: HeaderMap| async move {
            let named = headers.get(named).map(|v| v.to_str().unwrap().to_owned());
            Json(named)
        }),
    );
    let client = over.client(router).await;
    let asked = || client.get::<Option<String>>("/project");
    assert_eq!(asked().await.unwrap(), None);
    client.set_header(project.clone(), Some(HeaderValue::from_static("demo")));
    for _ in 0..2 {
        assert_eq!(asked().await.unwrap().as_deref(), Some("demo"));
    }
    client.set_header(project, None);
    assert_eq!(asked().await.unwrap(), None);
}
/// A path's segments are percent-encoded on the way and arrive decoded, with the query.
#[rstest]
#[tokio::test]
async fn path_and_query(#[values(Tcp, Stdio)] over: Over) {
    let echo = |Path(path): Path<String>, uri: ::http::Uri| async move {
        Json(json!({"path": path, "query": uri.query()}))
    };
    let client = over
        .client(Router::new().route("/files/{*path}", get(echo)))
        .await;
    let path = format!("/files/{}?depth=1", segments("dir/a b#한글.txt"));
    assert_eq!(
        client.get::<Value>(&path).await.unwrap(),
        json!({"path": "dir/a b#한글.txt", "query": "depth=1"})
    );
}

/// A body of a few megabytes, far past any buffer on the way, arrives whole in both
/// directions.
#[rstest]
#[tokio::test]
async fn large_body(#[values(Tcp, Stdio)] over: Over) {
    let echo = |body: axum::body::Bytes| async move { body };
    let router = Router::new()
        .route("/echo", post(echo))
        .layer(axum::extract::DefaultBodyLimit::disable());
    let client = over.client(router).await;
    let sent = "0123456789abcdef".repeat(256 * 1024);
    let echoed = client.post::<String>("/echo", &sent).await.unwrap();
    assert!(echoed == sent, "{} bytes of {}", echoed.len(), sent.len());
}

/// Requests sent at once are in flight together, not one after another, and each gets its
/// own answer.
#[rstest]
#[tokio::test]
async fn concurrent_requests(#[values(Tcp, Stdio)] over: Over) {
    let in_flight = Arc::new((AtomicU32::new(0), AtomicU32::new(0)));
    let slow = |State(in_flight): State<Arc<(AtomicU32, AtomicU32)>>, Path(n): Path<u32>| async move {
        let (now, most) = &*in_flight;
        most.fetch_max(now.fetch_add(1, SeqCst) + 1, SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        now.fetch_sub(1, SeqCst);
        Json(n)
    };
    let router = Router::new()
        .route("/slow/{n}", get(slow))
        .with_state(in_flight.clone());
    let client = &over.client(router).await;
    let answers = futures_util::future::join_all(
        (0..16u32).map(|n| async move { client.get::<u32>(&format!("/slow/{n}")).await }),
    )
    .await;
    for (n, answer) in (0..16u32).zip(answers) {
        assert_eq!(answer.unwrap(), n);
    }
    assert!(in_flight.1.load(SeqCst) > 1, "one at a time");
}

/// A 401 or a 403 is an error with its status, from `send` too.
#[rstest]
#[tokio::test]
async fn unauthorized(#[values(Tcp, Stdio)] over: Over) {
    let router = Router::new()
        .route("/401", get(|| async { StatusCode::UNAUTHORIZED }))
        .route("/403", get(|| async { StatusCode::FORBIDDEN }));
    let client = over.client(router).await;
    for status in [401, 403] {
        let path = format!("/{status}");
        let error = client.get::<Value>(&path).await.unwrap_err();
        assert_eq!(error.status(), Some(status), "{error:?}");
        let request = ::http::Request::get(&path).body(full("")).unwrap();
        let error = client.send(request).await.unwrap_err();
        assert_eq!(error.status(), Some(status), "{error:?}");
    }
}

/// A job's log kept by `sse::Log` is read by `sse::log` entry by entry as the job writes
/// it, to its end; and from after a given entry.
#[rstest]
#[tokio::test]
async fn log_stream(#[values(Tcp, Stdio)] over: Over) {
    let log = sse::Log::new(100, Line::Stdout("...".into()));
    let events = |State(log): State<Arc<sse::Log<Line>>>, headers: HeaderMap| async move {
        (
            [(CONTENT_TYPE, "text/event-stream")],
            axum::body::Body::from_stream(log.events(&headers)),
        )
    };
    let router = Router::new()
        .route("/jobs/j/events", get(events))
        .with_state(log.clone());
    let client = over.client(router).await;

    log.push(Line::Stdout("a".into()));
    let mut entries = std::pin::pin!(sse::log::<Line>(&client, "/jobs/j/events".into(), None));
    let first = entries.next().await.unwrap().unwrap();
    assert_eq!((first.id, first.entry), (1, Line::Stdout("a".into())));
    // Written while the stream is open.
    log.push(Line::Stdout("b".into()));
    let second = entries.next().await.unwrap().unwrap();
    assert_eq!((second.id, second.entry), (2, Line::Stdout("b".into())));
    log.end(Line::End { code: 0 }, || ());
    let last = entries.next().await.unwrap().unwrap();
    assert_eq!((last.id, last.entry), (3, Line::End { code: 0 }));
    assert!(entries.next().await.is_none());

    let ids: Vec<_> = sse::log::<Line>(&client, "/jobs/j/events".into(), Some(1))
        .map(|entry| entry.unwrap().id)
        .collect()
        .await;
    assert_eq!(ids, [2, 3]);
}

/// A 503 or a 429 is retried, after `Retry-After` or a backoff, until the request succeeds.
#[rstest]
#[tokio::test]
async fn retry_until_ok(#[values(Tcp, Stdio)] over: Over) {
    let asked = Arc::new(AtomicU32::new(0));
    let read = |State(asked): State<Arc<AtomicU32>>| async move {
        match asked.fetch_add(1, SeqCst) {
            0 => (
                StatusCode::SERVICE_UNAVAILABLE,
                [(CONTENT_TYPE, "text/html")],
                "<html>503</html>",
            )
                .into_response(),
            1 => (
                StatusCode::TOO_MANY_REQUESTS,
                [(RETRY_AFTER, "1")],
                Json(json!({"error": {
                    "code": "quota", "message": "slow down", "reason": "requests",
                }})),
            )
                .into_response(),
            _ => Json("read").into_response(),
        }
    };
    let written = |State(asked): State<Arc<AtomicU32>>| async move {
        match asked.fetch_add(1, SeqCst) {
            0 => (
                StatusCode::SERVICE_UNAVAILABLE,
                [(RETRY_AFTER, "1")],
                Json(json!({"error": {"code": "unavailable", "message": "not now"}})),
            )
                .into_response(),
            _ => Json("written").into_response(),
        }
    };
    let router = Router::new()
        .route("/read", get(read))
        .route("/written", put(written))
        .with_state(asked.clone());
    let client = over.client(router).await;
    assert_eq!(client.get::<String>("/read").await.unwrap(), "read");
    assert_eq!(asked.swap(0, SeqCst), 3);
    // A `PUT` makes nothing a second sending makes again.
    let written = client.put::<String>("/written", &json!({})).await;
    assert_eq!(written.unwrap(), "written");
    assert_eq!(asked.swap(0, SeqCst), 2);
}
/// A 429 for a quota that waiting does not free (not the request rate) is returned at once.
#[rstest]
#[tokio::test]
async fn no_retry_on_quota(#[values(Tcp, Stdio)] over: Over) {
    let asked = Arc::new(AtomicU32::new(0));
    let quota = |State(asked): State<Arc<AtomicU32>>| async move {
        asked.fetch_add(1, SeqCst);
        (
            StatusCode::TOO_MANY_REQUESTS,
            [(RETRY_AFTER, "30")],
            Json(json!({"error": {
                "code": "quota", "message": "3 sandboxes open", "reason": "sandboxes",
            }})),
        )
    };
    let router = Router::new()
        .route("/quota", get(quota))
        .with_state(asked.clone());
    let client = over.client(router).await;
    let error = client.get::<String>("/quota").await.unwrap_err();
    assert!(matches!(error, Error::Status(429, _, _)), "{error:?}");
    assert_eq!(error.code(), Some("quota"));
    assert_eq!(asked.load(SeqCst), 1);
}
/// A POST answered 503 is retried only when the code is `capacity` (nothing was done);
/// any other 503 may have come after the work started.
#[rstest]
#[tokio::test]
async fn post_retried_only_on_capacity(#[values(Tcp, Stdio)] over: Over) {
    let asked = Arc::new(AtomicU32::new(0));
    let made = |State(asked): State<Arc<AtomicU32>>| async move {
        asked.fetch_add(1, SeqCst);
        (
            StatusCode::SERVICE_UNAVAILABLE,
            [(RETRY_AFTER, "1")],
            Json(json!({"error": {"code": "unavailable", "message": "its machine went away"}})),
        )
    };
    let room = |State(asked): State<Arc<AtomicU32>>| async move {
        match asked.fetch_add(1, SeqCst) {
            0 => (
                StatusCode::SERVICE_UNAVAILABLE,
                [(RETRY_AFTER, "1")],
                Json(json!({"error": {"code": "capacity", "message": "no room yet"}})),
            )
                .into_response(),
            _ => Json("made").into_response(),
        }
    };
    let router = Router::new()
        .route("/made", post(made))
        .route("/room", post(room))
        .with_state(asked.clone());
    let client = over.client(router).await;
    let error = client
        .post::<String>("/made", &json!({}))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Status(503, _, _)), "{error:?}");
    assert_eq!(asked.swap(0, SeqCst), 1, "made once at most");
    let made = client.post::<String>("/room", &json!({})).await;
    assert_eq!(made.unwrap(), "made");
    assert_eq!(asked.swap(0, SeqCst), 2, "nothing made: sent again");
}
/// After a 503 that says nothing of what was done, only repeatable requests are retried:
/// not a MOVE, not a conditional PUT; a DELETE is, and a 404 on its retry counts as done.
/// `capacity` retries any method.
#[rstest]
#[tokio::test]
async fn retry_only_if_repeatable(#[values(Tcp, Stdio)] over: Over) {
    let asked = Arc::new(AtomicU32::new(0));
    // A proxy's page, which says nothing of what was done.
    let page = (
        StatusCode::SERVICE_UNAVAILABLE,
        [("content-type", "text/html")],
        "<html>503</html>",
    );
    let always = move |State(asked): State<Arc<AtomicU32>>| async move {
        asked.fetch_add(1, SeqCst);
        page
    };
    let gone = move |State(asked): State<Arc<AtomicU32>>| async move {
        match asked.fetch_add(1, SeqCst) {
            0 => page.into_response(),
            _ => StatusCode::NOT_FOUND.into_response(),
        }
    };
    let room = |State(asked): State<Arc<AtomicU32>>| async move {
        match asked.fetch_add(1, SeqCst) {
            0 => (
                StatusCode::SERVICE_UNAVAILABLE,
                [(RETRY_AFTER, "1")],
                Json(json!({"error": {"code": "capacity", "message": "no room yet"}})),
            )
                .into_response(),
            _ => StatusCode::CREATED.into_response(),
        }
    };
    let router = Router::new()
        .route("/always", any(always))
        .route("/gone", any(gone))
        .route("/room", any(room))
        .with_state(asked.clone());
    let client = over.client(router).await;

    let moved = ::http::Request::builder()
        .method("MOVE")
        .uri("/always")
        .header("Destination", "/to");
    let answered = client.send(moved.body(full("")).unwrap()).await.unwrap();
    assert_eq!(answered.status(), 503);
    assert_eq!(asked.swap(0, SeqCst), 1, "a MOVE goes once");

    let conditional = ::http::Request::put("/always").header("If-None-Match", "*");
    let answered = client
        .send(conditional.body(full("")).unwrap())
        .await
        .unwrap();
    assert_eq!(answered.status(), 503);
    assert_eq!(asked.swap(0, SeqCst), 1, "a PUT with a condition goes once");

    client.delete("/gone").await.unwrap();
    assert_eq!(asked.swap(0, SeqCst), 2, "a DELETE goes again");

    let moved = ::http::Request::builder()
        .method("MOVE")
        .uri("/room")
        .header("Destination", "/to");
    let answered = client.send(moved.body(full("")).unwrap()).await.unwrap();
    assert_eq!(answered.status(), 201);
    assert_eq!(asked.swap(0, SeqCst), 2, "nothing done: sent again");
}
/// A log stream cut mid-way: over TCP the client reconnects with `Last-Event-ID`, past a
/// proxy's 502, and reads to the end; over stdio the cut is an error at once.
#[rstest]
#[tokio::test]
async fn log_resumes_after_disconnect(#[values(Tcp, Stdio)] over: Over) {
    // The `Last-Event-ID` of each request.
    let asked = Arc::new(Mutex::new(Vec::new()));
    let log = |State(asked): State<Arc<Mutex<Vec<Option<String>>>>>, headers: HeaderMap| async move {
        let after = headers
            .get("last-event-id")
            .map(|v| v.to_str().unwrap().to_owned());
        let opened = {
            let mut asked = asked.lock().unwrap();
            asked.push(after);
            asked.len()
        };
        let sse = [(CONTENT_TYPE, "text/event-stream")];
        match opened {
            1 => {
                let sent = "id: 1\ndata: {\"stdout\":\"a\"}\n\nid: 2\ndata: {\"stdout\":\"b\"}\n\n";
                // Cut a moment after what was sent, once that has reached the client.
                let cut = futures_util::stream::once(async {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    Err(std::io::Error::other("cut"))
                });
                let body = futures_util::stream::iter([Ok(sent)]).chain(cut);
                (sse, axum::body::Body::from_stream(body)).into_response()
            }
            2 => (
                StatusCode::BAD_GATEWAY,
                [(CONTENT_TYPE, "text/html")],
                "<html><body><h1>502 Bad Gateway</h1></body></html>",
            )
                .into_response(),
            _ => (
                sse,
                "id: 3\ndata: {\"stdout\":\"c\"}\n\nid: 4\ndata: {\"end\":{\"code\":0}}\n\n",
            )
                .into_response(),
        }
    };
    let router = Router::new()
        .route("/log", get(log))
        .with_state(asked.clone());
    let client = over.client(router).await;
    let read: Vec<_> = sse::log::<Line>(&client, "/log".into(), None)
        .map(|entry| entry.map(|entry| entry.id))
        .collect()
        .await;
    let again = Some("2".to_owned());
    match over {
        Tcp => {
            assert!(matches!(read[..], [Ok(1), Ok(2), Ok(3), Ok(4)]), "{read:?}");
            assert_eq!(*asked.lock().unwrap(), [None, again.clone(), again]);
        }
        Stdio => {
            assert!(
                matches!(read[..], [Ok(1), Ok(2), Err(Error::Transport(_))]),
                "{read:?}"
            );
            assert_eq!(*asked.lock().unwrap(), [None]);
        }
    }
}
/// A log stream the server closes before the `end` entry is reopened after the last entry
/// read: it reads on, ends with the job's 404 if the job was removed, waits while the job
/// runs, and stops at once if the log had already ended.
#[rstest]
#[tokio::test]
async fn log_resumes_after_early_end(#[values(Tcp, Stdio)] over: Over) {
    // Each request for a log: its job, and the `Last-Event-ID` it came with.
    let asked = Arc::new(Mutex::new(Vec::new()));
    let events = |State(asked): State<Arc<Mutex<Vec<String>>>>,
                  Path(job): Path<String>,
                  headers: HeaderMap| async move {
        let after = headers
            .get("last-event-id")
            .map_or("-", |v| v.to_str().unwrap());
        let opened = {
            let mut asked = asked.lock().unwrap();
            asked.push(format!("{job} after {after}"));
            asked.iter().filter(|a| a.starts_with(&job)).count()
        };
        let sse = [(CONTENT_TYPE, "text/event-stream")];
        let a = "id: 1\ndata: {\"stdout\":\"a\"}\n\n";
        let b = "id: 2\ndata: {\"stdout\":\"b\"}\n\n";
        let end = "id: 3\ndata: {\"end\":{\"code\":0}}\n\n";
        match (job.as_str(), opened) {
            ("cut", 1) => (sse, [a, b].concat()).into_response(),
            ("cut", _) => (sse, end).into_response(),
            ("removed", 1) => (sse, a).into_response(),
            ("waiting", 1) | ("ended", _) => (sse, "").into_response(),
            ("waiting", _) => (sse, [a, b, end].concat()).into_response(),
            _ => StatusCode::NOT_FOUND.into_response(),
        }
    };
    let job = |Path(job): Path<String>| async move {
        match job.as_str() {
            "removed" => StatusCode::NOT_FOUND.into_response(),
            _ => Json(json!({"id": job, "running": job == "waiting"})).into_response(),
        }
    };
    let router = Router::new()
        .route("/jobs/{job}", get(job))
        .route("/jobs/{job}/events", get(events))
        .with_state(asked.clone());
    let client = over.client(router).await;

    // Each job's log, read from after an entry or from its start: the ids it gave, or the
    // status it ended with.
    for (job, after, read) in [
        ("cut", None, vec![Ok(1), Ok(2), Ok(3)]),
        ("removed", None, vec![Ok(1), Err(Some(404))]),
        ("waiting", None, vec![Ok(1), Ok(2), Ok(3)]),
        ("ended", Some(3), vec![]),
    ] {
        let entries: Vec<_> = sse::log::<Line>(&client, format!("/jobs/{job}/events"), after)
            .map(|entry| entry.map(|entry| entry.id).map_err(|e| e.status()))
            .collect()
            .await;
        assert_eq!(entries, read, "{job}");
    }
    assert_eq!(
        *asked.lock().unwrap(),
        [
            "cut after -",
            "cut after 2",
            "removed after -",
            "removed after 1",
            "waiting after -",
            "waiting after -",
            "ended after 3",
        ]
    );
}
