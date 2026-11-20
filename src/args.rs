//! A request's JSON body read as the args it carries, and what a server says of one that does not
//! read as them: the member it failed at, what that member takes and what was sent, as
//! `cmd: a list, not "ls"` -- in its own words, not serde's.

use serde::de::DeserializeOwned;
use serde_json::Value;
use serde_path_to_error::Segment;

/// The most of a value sent that a refusal shows, in characters.
const SHOWN: usize = 60;

/// `body` read as a `T`; or why it does not read as one, naming the member it failed at.
pub fn from_body<T: DeserializeOwned>(body: &[u8]) -> Result<T, String> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Err("the body: a JSON object, and none was sent".to_owned());
    }
    // Where it stops reading, and none of what it holds, which may be a credential.
    let not_json = |e: serde_json::Error| {
        format!(
            "the body: JSON, which it stops being at line {}, column {}",
            e.line(),
            e.column()
        )
    };
    let mut json = serde_json::Deserializer::from_slice(body);
    let failed = match serde_path_to_error::deserialize(&mut json) {
        Ok(args) => return json.end().map(|()| args).map_err(not_json),
        Err(failed) => failed,
    };
    let sent = serde_json::from_slice::<Value>(body).map_err(not_json)?;

    let path: Vec<&Segment> = failed.path().iter().collect();
    let name = named(&path);
    let found = path.iter().try_fold(&sent, |value, segment| match segment {
        Segment::Seq { index } => value.get(index),
        Segment::Map { key } => value.get(key),
        Segment::Enum { variant } => value.get(variant),
        Segment::Unknown => None,
    });
    let not = found
        .map(|value| format!(", not {}", shown_value(value)))
        .unwrap_or_default();

    // serde's words, without where in the text it failed: what the type it read said.
    let said = failed.inner().to_string();
    let said = match said.rfind(" at line ") {
        Some(at) => &said[..at],
        None => &said,
    };
    // serde reads an internally tagged member, such as `image`, whole before what is in it, so
    // for a failure inside it the path stops at the member: told there in serde's words, which
    // name what was sent inside, with none of the member shown.
    let inside = |unexpected: &str| {
        let sent_as = match found {
            Some(Value::Object(_)) => "map",
            Some(Value::Array(_)) => "sequence",
            _ => return false,
        };
        !unexpected.starts_with(sent_as)
    };
    let member = |field: &str| match name.as_str() {
        "the body" => field.to_owned(),
        parent => format!("{parent}.{field}"),
    };
    Err(if let Some(field) = quoted(said, "missing field ") {
        format!("{}: required, and not sent", member(field))
    } else if let Some(field) = quoted(said, "duplicate field ") {
        format!("{}: sent twice", member(field))
    } else if let Some(field) = quoted(said, "unknown field ") {
        let members = said
            .split_once(", expected one of ")
            .or_else(|| said.split_once(", expected "))
            .map_or("none", |(_, members)| members);
        let members = members.replace("` or `", "`, `");
        // Its path is the member's own, read to tell it from the members -- but for one inside
        // an internally tagged member, which stops at that member.
        let name = match path.last() {
            Some(Segment::Map { key }) if key == field => name,
            _ => member(field),
        };
        format!("{name}: not a member; the members are {members}")
    } else if let Some((_, kinds)) = said
        .strip_prefix("unknown variant ")
        .and_then(|rest| rest.split_once(", expected "))
    {
        let kinds = kinds.strip_prefix("one of ").unwrap_or(kinds);
        format!("{name}: one of {kinds}{not}")
    } else if let Some((unexpected, expected)) = ["invalid type: ", "invalid value: "]
        .iter()
        .find_map(|prefix| said.strip_prefix(prefix))
        .and_then(|rest| rest.split_once(", expected "))
    {
        match (inside(unexpected), unexpected) {
            (true, _) => format!("{name}: {said}"),
            (false, "unit variant") => {
                format!("{name}: {}, not its name alone", described_type(expected))
            }
            (false, _) => format!("{name}: {}{not}", described_type(expected)),
        }
    } else if let Some((_, expected)) = said
        .strip_prefix("invalid length ")
        .and_then(|rest| rest.split_once(", expected "))
    {
        format!("{name}: {}{not}", described_type(expected))
    } else if said == "expected value" {
        // serde_json on an enum sent neither a variant's name nor an object of one member.
        match found {
            Some(Value::Object(sent)) => {
                format!("{name}: an object of one member, not of {}", sent.len())
            }
            _ => format!("{name}: a string or an object{not}"),
        }
    } else if found.is_some_and(|value| said.contains(&shown_value(value))) {
        // A message of the type's own that already says what was sent.
        format!("{name}: {said}")
    } else {
        format!("{name}: {said}{not}")
    })
}

/// What a value of the Rust type `type_name` is, or one serde says it expected, as a client is
/// told it: `u16` is "a whole number from 0 to 65535".
pub fn described_type(type_name: &str) -> String {
    let bare = type_name.rsplit("::").next().unwrap_or(type_name);
    match bare {
        "u8" => "a whole number from 0 to 255".to_owned(),
        "u16" => "a whole number from 0 to 65535".to_owned(),
        "u32" => "a whole number from 0 to 4294967295".to_owned(),
        "u64" | "usize" => "a whole number from 0".to_owned(),
        "i8" | "i16" | "i32" | "i64" | "isize" => "a whole number".to_owned(),
        "f32" | "f64" => "a number".to_owned(),
        "bool" | "a boolean" => "true or false".to_owned(),
        "String" | "str" | "a string" | "a borrowed string" | "a character" => {
            "a string".to_owned()
        }
        "Uuid" => "a UUID".to_owned(),
        "newtype variant" | "tuple variant" | "struct variant" => {
            "an object of its name and its value".to_owned()
        }
        _ if bare.starts_with("a sequence") || bare.starts_with("tuple") => "a list".to_owned(),
        _ if [
            "a map",
            "struct ",
            "internally tagged enum ",
            "adjacently tagged enum ",
        ]
        .iter()
        .any(|object| bare.starts_with(object)) =>
        {
            "an object".to_owned()
        }
        _ => type_name.to_owned(),
    }
}

/// The member `path` leads to, as a client spells it: `volumes[0].at`, or "the body".
fn named(path: &[&Segment]) -> String {
    let mut name = String::new();
    for segment in path {
        match segment {
            Segment::Seq { index } => name.push_str(&format!("[{index}]")),
            Segment::Map { key } if name.is_empty() => name.push_str(key),
            Segment::Map { key } => name.push_str(&format!(".{key}")),
            Segment::Enum { variant } => name.push_str(&format!(".{variant}")),
            Segment::Unknown => {}
        }
    }
    match name.is_empty() {
        true => "the body".to_owned(),
        false => name,
    }
}

/// The name in backquotes that `said` starts with after `prefix`.
fn quoted<'a>(said: &'a str, prefix: &str) -> Option<&'a str> {
    let rest = said.strip_prefix(prefix)?.strip_prefix('`')?;
    rest.split_once('`').map(|(name, _)| name)
}

/// `value` as JSON, cut at [`SHOWN`] characters; an object or a list by its kind alone, as one
/// may carry what is not to be shown back, such as a credential.
fn shown_value(value: &Value) -> String {
    match value {
        Value::Object(_) => return "an object".to_owned(),
        Value::Array(_) => return "a list".to_owned(),
        _ => {}
    }
    let json = value.to_string();
    match json.chars().count() > SHOWN {
        true => format!("{}...", json.chars().take(SHOWN).collect::<String>()),
        false => json,
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    /// Args with a list, and a number that may be left out.
    #[derive(Debug, PartialEq, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Exec {
        cmd: Vec<String>,
        #[serde(default)]
        timeout_ms: Option<u64>,
    }

    /// Args with an internally tagged member.
    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Create {
        #[allow(dead_code)]
        image: Image,
    }

    #[derive(Debug, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
    #[allow(dead_code)]
    enum Image {
        Tag { tag: String },
        Digest { digest: String },
    }

    fn refused<T: DeserializeOwned + std::fmt::Debug>(body: &str) -> String {
        from_body::<T>(body.as_bytes()).expect_err(body)
    }

    #[test]
    fn a_body_that_reads_as_its_args_is_them() {
        let read: Exec = from_body(br#"{"cmd": ["ls", "-l"]}"#).unwrap();
        let args = Exec {
            cmd: vec!["ls".into(), "-l".into()],
            timeout_ms: None,
        };
        assert_eq!(read, args);
    }

    #[test]
    fn a_body_that_is_not_json_is_refused_saying_so() {
        assert_eq!(
            refused::<Exec>(""),
            "the body: a JSON object, and none was sent"
        );
        assert_eq!(
            refused::<Exec>("not json"),
            "the body: JSON, which it stops being at line 1, column 2"
        );
        assert_eq!(
            refused::<Exec>(r#"{"cmd": ["ls"]} {}"#),
            "the body: JSON, which it stops being at line 1, column 17"
        );
        // None of it is shown: a body missing its last `}` may hold a credential.
        let image = r#"{"image": {"type": "tag", "tag": "secret_abcdef"}"#;
        assert_eq!(
            refused::<Create>(image),
            "the body: JSON, which it stops being at line 1, column 49"
        );
    }

    #[test]
    fn a_member_that_does_not_read_is_named_with_what_it_takes_and_what_was_sent() {
        assert_eq!(refused::<Exec>("[]"), "the body: an object, not a list");
        assert_eq!(
            refused::<Exec>(r#"{"cmd": "ls"}"#),
            r#"cmd: a list, not "ls""#
        );
        assert_eq!(
            refused::<Exec>(r#"{"cmd": ["ls", 1]}"#),
            "cmd[1]: a string, not 1"
        );
        assert_eq!(
            refused::<Exec>(r#"{"cmd": ["ls"], "timeout_ms": -1}"#),
            "timeout_ms: a whole number from 0, not -1"
        );
        assert_eq!(
            refused::<Create>(r#"{"image": "alpine"}"#),
            r#"image: an object, not "alpine""#
        );
    }

    /// serde reads an internally tagged member whole before what is in it, so what failed in
    /// it is told at the member, in serde's words, and no object sent is shown back: it may
    /// carry a credential.
    #[test]
    fn a_member_inside_an_internally_tagged_one_is_told_at_it_showing_no_object() {
        assert_eq!(
            refused::<Create>(r#"{"image": {"type": "tag", "tag": 1}}"#),
            "image: invalid type: integer `1`, expected a string"
        );
        assert_eq!(
            refused::<Create>(r#"{"image": {"type": "tag", "tag": "a", "x": 1}}"#),
            "image.x: not a member; the members are `tag`"
        );
        assert_eq!(
            refused::<Create>(r#"{"image": {"type": "tag", "tag": ["secret_abcdef"]}}"#),
            "image: invalid type: sequence, expected a string"
        );
    }

    #[test]
    fn a_member_missing_or_unknown_is_named() {
        assert_eq!(refused::<Exec>("{}"), "cmd: required, and not sent");
        assert_eq!(
            refused::<Exec>(r#"{"cmd": ["ls"], "x": 1}"#),
            "x: not a member; the members are `cmd`, `timeout_ms`"
        );
        assert_eq!(
            refused::<Exec>(r#"{"cmd": ["ls"], "cmd": ["ls"]}"#),
            "cmd: sent twice"
        );
        assert_eq!(
            refused::<Create>(r#"{"image": {"tag": "alpine"}}"#),
            "image.type: required, and not sent"
        );
        assert_eq!(
            refused::<Create>(r#"{"image": {"type": "name", "tag": "alpine"}}"#),
            r#"image.type: one of `tag` or `digest`, not "name""#
        );
    }

    #[test]
    fn a_type_is_told_in_words() {
        assert_eq!(described_type("u16"), "a whole number from 0 to 65535");
        assert_eq!(described_type("u64"), "a whole number from 0");
        assert_eq!(described_type("i64"), "a whole number");
        assert_eq!(described_type("uuid::Uuid"), "a UUID");
        assert_eq!(described_type("alloc::string::String"), "a string");
        assert_eq!(described_type("struct Exec with 2 elements"), "an object");
    }
}
