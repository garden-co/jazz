//! Names back ends derive from the authored ones: PascalCase singular type
//! names for tables, and the clash checks every back end runs.

/// PascalCase of an authored name: `chat_members` and `chatMembers` both
/// give `ChatMembers`.
pub fn pascal(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for part in name.split('_').filter(|p| !p.is_empty()) {
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            out.extend(first.to_uppercase());
            out.push_str(chars.as_str());
        }
    }
    out
}

/// Words that end in `s` in the singular, so `<word>es` is their plural.
const S_SINGULARS: &[&str] = &[
    "alias", "atlas", "bias", "bonus", "bus", "campus", "canvas", "census", "focus", "gas", "lens",
    "radius", "status", "virus",
];

/// Plurals that don't follow the rules.
const IRREGULAR: &[(&str, &str)] = &[
    ("people", "person"),
    ("children", "child"),
    ("men", "man"),
    ("women", "woman"),
    ("mice", "mouse"),
    ("geese", "goose"),
    ("feet", "foot"),
    ("teeth", "tooth"),
];

/// The singular of the last word of a (plural) table name: `chatMembers` →
/// `chatMember`, `categories` → `category`, `canvases` → `canvas`. A name
/// that isn't a plural stays as it is.
pub fn singular(name: &str) -> String {
    // The last word: after the last `_` or before the last capital.
    let split = name
        .char_indices()
        .rev()
        .find(|(i, c)| *i > 0 && (c.is_ascii_uppercase() || *c == '_'))
        .map_or(0, |(i, c)| if c == '_' { i + 1 } else { i });
    let (head, word) = name.split_at(split);
    let lower = word.to_ascii_lowercase();
    let keep_case = |singular: &str| -> String {
        // Keep the word's first letter's case.
        let mut chars = singular.chars();
        match (word.chars().next(), chars.next()) {
            (Some(first), Some(_)) if first.is_ascii_uppercase() => {
                let mut out = first.to_string();
                out.push_str(chars.as_str());
                out
            }
            _ => singular.to_string(),
        }
    };
    let singular_word = if let Some((_, one)) = IRREGULAR.iter().find(|(many, _)| *many == lower) {
        keep_case(one)
    } else if lower.len() > 3 && lower.ends_with("ies") {
        format!("{}y", &word[..word.len() - 3])
    } else if (lower.ends_with("ses") && S_SINGULARS.contains(&&lower[..lower.len() - 2]))
        || ["sses", "ches", "shes", "xes", "zes"]
            .iter()
            .any(|end| lower.ends_with(end))
    {
        word[..word.len() - 2].to_string()
    } else if lower.len() > 1
        && lower.ends_with('s')
        && !lower.ends_with("ss")
        && !lower.ends_with("us")
        && !lower.ends_with("is")
    {
        word[..word.len() - 1].to_string()
    } else {
        word.to_string()
    };
    format!("{head}{singular_word}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_names_become_singular_pascal_case() {
        for (table, name) in [
            ("chats", "Chat"),
            ("chatMembers", "ChatMember"),
            ("chat_members", "ChatMember"),
            ("canvases", "Canvas"),
            ("categories", "Category"),
            ("statuses", "Status"),
            ("boxes", "Box"),
            ("matches", "Match"),
            ("addresses", "Address"),
            ("people", "Person"),
            ("teamPeople", "TeamPerson"),
            ("cases", "Case"),
            ("todo", "Todo"),
            ("news", "New"),
            ("status", "Status"),
            ("analysis", "Analysis"),
            ("glass", "Glass"),
            ("series", "Sery"),
            ("profiles", "Profile"),
            ("strokes", "Stroke"),
        ] {
            assert_eq!(pascal(&singular(table)), name, "{table}");
        }
    }
}
