//! Why a client's request failed, whichever endpoint it went to.

use std::fmt;

use http::header;
use http_body_util::BodyExt;
use serde::Deserialize;

use crate::Body;

/// Why a call failed, and by the variant where: on the way, at the server, or on this side.
#[derive(Debug)]
pub enum Error {
    /// The request did not reach the server, or its response did not come back: the
    /// connection failed or the session ended.
    Transport(String),
    /// The server refused the request with an HTTP status: the status, and why, for a person,
    /// as `code: message` where the server answered its error as the protocol's Resources do;
    /// and the members its error had beside those. A `401` or a `403` from any endpoint says the headers
    /// are not what it wants: a client may set new ones and send the request again.
    Status(u16, String, Members),
    /// An HTTP response that is not the protocol's error: its status, what it was rather than
    /// its body for a person, and its raw body if it had one.
    Refusal {
        status: u16,
        description: String,
        body: Option<String>,
    },
    /// This side failed: an argument that is not what the call takes, a file on this machine
    /// that could not be read or written, or an answer it could not read.
    Client(String),
}

/// What a refusal answered as the protocol's Resources do says beside its `code` and
/// `message`: the other members of its `error`, as the server sent them.
pub type Members = serde_json::Map<String, serde_json::Value>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Transport(reason) | Error::Client(reason) => f.write_str(reason),
            Error::Status(status, body, _) => {
                let reason = http::StatusCode::from_u16(*status)
                    .ok()
                    .and_then(|status| status.canonical_reason())
                    .unwrap_or("");
                write!(f, "the server answered {status} {reason}")?;
                if !body.is_empty() {
                    write!(f, ": {body}")?;
                }
                Ok(())
            }
            Error::Refusal {
                status,
                description,
                ..
            } => {
                let reason = http::StatusCode::from_u16(*status)
                    .ok()
                    .and_then(|status| status.canonical_reason())
                    .unwrap_or("");
                write!(f, "the server answered {status} {reason}")?;
                if !description.is_empty() {
                    write!(f, "; {description}")?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    /// The HTTP status the server refused the request with, if it did.
    pub fn status(&self) -> Option<u16> {
        match self {
            Error::Status(status, _, _) | Error::Refusal { status, .. } => Some(*status),
            _ => None,
        }
    }

    /// The members a refusal answered as the protocol's Resources do had beside its `code` and
    /// `message`: none, for any other error.
    pub fn members(&self) -> Option<&Members> {
        match self {
            Error::Status(_, _, members) => Some(members),
            _ => None,
        }
    }

    /// The code of a refusal the server answered as the protocol's Resources do
    /// (`{"error": {"code", "message"}}`): `not_found`, `in_use`, `quota` and the like. What a
    /// client branches on, rather than the message.
    pub fn code(&self) -> Option<&str> {
        let Error::Status(_, said, _) = self else {
            return None;
        };
        let (code, _) = said.split_once(": ")?;
        let protocol = !code.is_empty()
            && code
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
        protocol.then_some(code)
    }
}

/// `response`, a refusal, as the [`Error::Status`] or [`Error::Refusal`] it is: its body read
/// for a person -- the `{"error": {"code", "message"}}` a resource answers as `code: message`,
/// with the [`Members`] beside them, plain text as it is -- or what any other refusal was
/// rather than its body.
pub async fn refused(response: http::Response<Body>) -> Error {
    #[derive(Deserialize)]
    struct Refusal {
        error: Reason,
    }
    #[derive(Deserialize)]
    struct Reason {
        code: String,
        message: String,
        #[serde(flatten)]
        members: Members,
    }
    let (parts, body) = response.into_parts();
    let status = parts.status.as_u16();
    let content_type = parts
        .headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .unwrap_or("");
    let body = body
        .collect()
        .await
        .map(|body| body.to_bytes())
        .unwrap_or_default();
    if body.is_empty() {
        return Error::Status(status, String::new(), Members::default());
    }
    match serde_json::from_slice::<Refusal>(&body) {
        Ok(Refusal { error }) => Error::Status(
            status,
            format!("{}: {}", error.code, error.message),
            error.members,
        ),
        // Words, as a server or a proxy in front of it may answer: said as they are. A page,
        // such as a proxy's HTML, is not words to a person.
        Err(_) if content_type.eq_ignore_ascii_case("text/plain") => {
            match std::str::from_utf8(&body) {
                Ok(said) => Error::Status(
                    status,
                    said.trim().chars().take(TEXT_MOST).collect(),
                    Members::default(),
                ),
                Err(_) => Error::Refusal {
                    status,
                    description: format!(
                        "not the server's answer ({content_type}, {} bytes)",
                        body.len()
                    ),
                    body: Some(String::from_utf8_lossy(&body).into_owned()),
                },
            }
        }
        Err(_) => {
            let desc = if content_type.is_empty() {
                format!("{} bytes", body.len())
            } else {
                format!("{content_type}, {} bytes", body.len())
            };
            let mut description = format!("not the server's answer ({desc})");
            if matches!(status, 502..=504) {
                description.push_str(": the service is unreachable or restarting");
            }
            let raw_body = String::from_utf8_lossy(&body).into_owned();
            Error::Refusal {
                status,
                description,
                body: Some(raw_body),
            }
        }
    }
}

/// The most of a plain-text refusal said as its message, in characters.
const TEXT_MOST: usize = 1000;

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::full;

    #[test]
    fn a_refusal_reads_as_its_status_and_why() {
        let refused = Error::Status(409, "in_use: held".into(), Members::default());
        assert_eq!(
            refused.to_string(),
            "the server answered 409 Conflict: in_use: held"
        );
        assert_eq!(
            Error::Status(404, String::new(), Members::default()).to_string(),
            "the server answered 404 Not Found"
        );
        // A status HTTP gives no reason has none.
        assert_eq!(
            Error::Status(599, "odd".into(), Members::default()).to_string(),
            "the server answered 599 : odd"
        );
        assert_eq!(Error::Transport("gone".into()).to_string(), "gone");
    }

    #[test]
    fn a_refusal_gives_its_status() {
        assert_eq!(
            Error::Status(404, "not found".into(), Members::default()).status(),
            Some(404)
        );
        assert_eq!(
            Error::Refusal {
                status: 503,
                description: "not the server's answer".into(),
                body: None,
            }
            .status(),
            Some(503)
        );
        assert_eq!(Error::Transport("connection reset".into()).status(), None);
        assert_eq!(Error::Client("bad json".into()).status(), None);
    }

    #[test]
    fn a_refusal_in_the_protocol_s_shape_gives_its_code() {
        assert_eq!(
            Error::Status(
                409,
                "in_use: volume `dev` is mounted".into(),
                Members::default()
            )
            .code(),
            Some("in_use")
        );
        assert_eq!(
            Error::Status(404, "not_found: no sandbox s1".into(), Members::default()).code(),
            Some("not_found")
        );
        // A refusal not in the protocol's shape, or none: no code.
        assert_eq!(
            Error::Status(500, "Internal Server Error".into(), Members::default()).code(),
            None
        );
        assert_eq!(
            Error::Status(
                503,
                "no room for it yet: try again".into(),
                Members::default()
            )
            .code(),
            None
        );
        assert_eq!(
            Error::Status(404, String::new(), Members::default()).code(),
            None
        );
        assert_eq!(
            Error::Status(404, "/work/a.txt".into(), Members::default()).code(),
            None
        );
        assert_eq!(Error::Transport("refused: x".into()).code(), None);
    }

    #[tokio::test]
    async fn a_non_json_refusal_says_what_it_was_without_quoting_it() {
        let response = http::Response::builder()
            .status(503)
            .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
            .body(full(Bytes::from(
                "<html><title>503 Service Unavailable</title></html>",
            )))
            .unwrap();
        let error = refused(response).await;
        assert_eq!(
            error.to_string(),
            "the server answered 503 Service Unavailable; not the server's answer (text/html, 51 bytes): the service is unreachable or restarting"
        );
        let Error::Refusal { status, body, .. } = &error else {
            panic!("expected Error::Refusal, got {error:?}");
        };
        assert_eq!(*status, 503);
        assert_eq!(
            body.as_deref(),
            Some("<html><title>503 Service Unavailable</title></html>")
        );
    }

    #[tokio::test]
    async fn a_non_json_refusal_without_content_type() {
        let response = http::Response::builder()
            .status(502)
            .body(full(Bytes::from("bad gateway")))
            .unwrap();
        let error = refused(response).await;
        assert_eq!(
            error.to_string(),
            "the server answered 502 Bad Gateway; not the server's answer (11 bytes): the service is unreachable or restarting"
        );
    }

    /// Plain text is said as it is, a server's and a proxy's alike, as words to a person -- not
    /// as "not the server's answer" of a server that answered.
    #[tokio::test]
    async fn a_plain_text_refusal_is_said_as_it_is() {
        for (status, text, said) in [
            (
                500,
                "internal crash\n",
                "the server answered 500 Internal Server Error: internal crash",
            ),
            (
                503,
                "no room for the sandbox yet: try again",
                "the server answered 503 Service Unavailable: no room for the sandbox yet: try again",
            ),
        ] {
            let response = http::Response::builder()
                .status(status)
                .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
                .body(full(Bytes::from(text)))
                .unwrap();
            let error = refused(response).await;
            assert_eq!(error.to_string(), said);
            assert!(
                matches!(error, Error::Status(s, _, _) if s == status),
                "{error:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_protocol_refusal_keeps_its_code_and_message() {
        let response = http::Response::builder()
            .status(422)
            .header(header::CONTENT_TYPE, "application/json")
            .body(full(Bytes::from(
                r#"{"error": {"code": "not_started", "message": "no such binary"}}"#,
            )))
            .unwrap();
        let error = refused(response).await;
        assert_eq!(
            error.to_string(),
            "the server answered 422 Unprocessable Entity: not_started: no such binary"
        );
        assert_eq!(error.members(), Some(&Members::new()));
    }

    #[tokio::test]
    async fn a_refusal_keeps_the_members_beside_its_code_and_message() {
        let response = http::Response::builder()
            .status(409)
            .header(header::CONTENT_TYPE, "application/json")
            .body(full(Bytes::from(
                r#"{"error": {"code": "ended", "message": "cut", "written": 65536, "x": 1}}"#,
            )))
            .unwrap();
        let error = refused(response).await;
        assert_eq!(error.code(), Some("ended"));
        let members = error.members().unwrap();
        assert_eq!(members["written"], 65536);
        assert_eq!(members["x"], 1);
    }

    #[tokio::test]
    async fn an_empty_refusal_says_only_the_status() {
        let response = http::Response::builder()
            .status(401)
            .body(full(Bytes::new()))
            .unwrap();
        let error = refused(response).await;
        assert_eq!(error.to_string(), "the server answered 401 Unauthorized");
    }
}
