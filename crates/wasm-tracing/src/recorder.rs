use core::fmt::{self, Write};
use std::collections::HashMap;

use tracing::field::{Field, Visit};

#[derive(Default)]
pub struct StringRecorder {
    pub display: String,
    pub is_following_args: bool,
    pub fields: HashMap<String, String>,
    pub message: Option<String>,
}
impl StringRecorder {
    fn record_field(&mut self, field: &Field, value: &dyn fmt::Debug, raw: String) {
        if self.is_following_args {
            writeln!(self.display).unwrap();
        } else {
            write!(self.display, " ").unwrap();
            self.is_following_args = true;
        }
        write!(self.display, "{} = {:?};", field.name(), value).unwrap();
        self.fields.insert(field.name().to_owned(), raw);
    }
}

impl Visit for StringRecorder {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.record_debug(field, &value);
        } else {
            self.record_field(field, &value, value.to_owned());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            let message = format!("{value:?}");
            self.message = Some(trim_debug_message(&message).to_string());
            if !self.display.is_empty() {
                self.display = format!("{value:?}\n{}", self.display)
            } else {
                self.display = format!("{value:?}")
            }
        } else {
            self.record_field(field, value, format!("{value:?}"));
        }
    }
}

impl core::fmt::Display for StringRecorder {
    fn fmt(&self, mut f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        if !self.display.is_empty() {
            write!(&mut f, " {}", self.display)
        } else {
            Ok(())
        }
    }
}

fn trim_debug_message(message: &str) -> &str {
    message
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(message)
}
