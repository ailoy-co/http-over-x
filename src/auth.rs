//! The credentials a client sends with every request to a server over HTTP, as the Authentication
//! section of the protocol has a server want them.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// How a client proves who it is with every request: not at all, by a token or a password in
/// its `Authorization` header, or by a certificate as the connection opens. `Debug` leaves
/// the token and the password out.
///
/// As JSON, `{"type": "none"}`, `{"type": "bearer", "token": ...}` (a [`Token`]),
/// `{"type": "basic", "username": "...", "password": "..."}` or `{"type": "mtls", "cert":
/// "...", "key": "...", "ca": "..."}`, the `ca` there or not.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Auth {
    #[default]
    None,
    /// `Authorization: Bearer <token>`.
    Bearer { token: Token },
    /// `Authorization: Basic <username:password, in base64>`.
    Basic { username: String, password: String },
    /// Mutual TLS: the client's certificate, shown to the server as each connection opens, and
    /// nothing in a header. `cert` and `key` are the paths of PEM files, the certificate (with
    /// the chain that leads to it, where the server wants one) and its private key, read when
    /// the auth is given; `key` may be the file `cert` is, holding both. Over `https` only.
    ///
    /// `ca`, where given, is the path of a PEM file of the certificate authorities that sign
    /// the server's own certificate, trusted along with the system's: a server that wants a
    /// certificate is often one whose own no public authority signed.
    Mtls {
        cert: PathBuf,
        key: PathBuf,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ca: Option<PathBuf>,
    },
}

/// Where a bearer token comes from: given, or printed by a command.
///
/// As JSON, the token itself, `"..."`, or the command, `{"program": "...", "args": [...]}`.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum Token {
    /// The token itself, the caller's to replace ([`Client::set_auth`](crate::Client::set_auth))
    /// when the server refuses it.
    Given(String),
    /// The token `program` with `args` prints: a token alone, or `{"token": "...",
    /// "expires_in": <seconds>}`. It is run for the first request, and again when its token
    /// is a minute from running out or the server refuses it -- then with `TOKEN_REFUSED` set
    /// (the client's [`TOKEN_REFUSED`](crate::TOKEN_REFUSED)), for one that keeps its tokens to
    /// give a new one: a person's short-lived token.
    Command {
        program: String,
        #[serde(default)]
        args: Vec<String>,
    },
}

impl Auth {
    /// A bearer `token`, given.
    pub fn bearer(token: impl Into<String>) -> Self {
        Auth::Bearer {
            token: Token::Given(token.into()),
        }
    }

    /// The bearer token `program` with `args` prints ([`Token::Command`]).
    pub fn command<A: Into<String>>(
        program: impl Into<String>,
        args: impl IntoIterator<Item = A>,
    ) -> Self {
        Auth::Bearer {
            token: Token::Command {
                program: program.into(),
                args: args.into_iter().map(Into::into).collect(),
            },
        }
    }

    /// A `username` and its `password`.
    pub fn basic(username: impl Into<String>, password: impl Into<String>) -> Self {
        Auth::Basic {
            username: username.into(),
            password: password.into(),
        }
    }

    /// The certificate in the PEM file at `cert`, whose private key is in the one at `key`,
    /// shown to a server whose own the system trusts: no `ca`.
    pub fn mtls(cert: impl Into<PathBuf>, key: impl Into<PathBuf>) -> Self {
        Auth::Mtls {
            cert: cert.into(),
            key: key.into(),
            ca: None,
        }
    }
}

impl std::fmt::Debug for Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            Self::Bearer { token } => f.debug_struct("Bearer").field("token", token).finish(),
            Self::Basic { username, .. } => f
                .debug_struct("Basic")
                .field("username", username)
                .finish_non_exhaustive(),
            Self::Mtls { cert, key, ca } => f
                .debug_struct("Mtls")
                .field("cert", cert)
                .field("key", key)
                .field("ca", ca)
                .finish(),
        }
    }
}

impl std::fmt::Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Given(_) => f.debug_tuple("Given").finish_non_exhaustive(),
            Self::Command { program, args } => f
                .debug_struct("Command")
                .field("program", program)
                .field("args", args)
                .finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn debug_leaves_the_secrets_out() {
        let bearer = Auth::bearer("secret");
        assert!(!format!("{bearer:?}").contains("secret"));
        let basic = Auth::basic("alice", "secret");
        let shown = format!("{basic:?}");
        assert!(
            shown.contains("alice") && !shown.contains("secret"),
            "{shown}"
        );
    }

    #[test]
    fn each_auth_is_its_json() {
        for (auth, json) in [
            (Auth::None, json!({"type": "none"})),
            (
                Auth::bearer("secret"),
                json!({"type": "bearer", "token": "secret"}),
            ),
            (
                Auth::command("gcloud", ["auth", "print-access-token"]),
                json!({"type": "bearer", "token": {
                    "program": "gcloud", "args": ["auth", "print-access-token"],
                }}),
            ),
            (
                Auth::basic("alice", "secret"),
                json!({"type": "basic", "username": "alice", "password": "secret"}),
            ),
            (
                Auth::mtls("client.pem", "client.key"),
                json!({"type": "mtls", "cert": "client.pem", "key": "client.key"}),
            ),
            (
                Auth::Mtls {
                    cert: "client.pem".into(),
                    key: "client.key".into(),
                    ca: Some("ca.pem".into()),
                },
                json!({"type": "mtls", "cert": "client.pem", "key": "client.key", "ca": "ca.pem"}),
            ),
        ] {
            assert_eq!(serde_json::to_value(&auth).unwrap(), json);
            assert_eq!(serde_json::from_value::<Auth>(json).unwrap(), auth);
        }
    }

    #[test]
    fn a_command_reads_without_its_args() {
        let command: Auth =
            serde_json::from_value(json!({"type": "bearer", "token": {"program": "token"}}))
                .unwrap();
        assert_eq!(command, Auth::command("token", [] as [&str; 0]));
    }

    #[test]
    fn a_member_unknown_is_refused() {
        for json in [
            json!({"type": "bearer", "token": "t", "program": "p"}),
            json!({"type": "bearer", "token": {"program": "p", "arg": []}}),
            json!({"type": "command", "program": "p"}),
        ] {
            assert!(
                serde_json::from_value::<Auth>(json.clone()).is_err(),
                "{json}"
            );
        }
    }
}
