//! The Snowball English ("Porter 2") stemmer, ported from
//! `algorithms/english.sbl` of Snowball v3.2.0
//! (<https://github.com/snowballstem/snowball>, BSD-3-Clause) and checked
//! against that release's vocabulary (`english/voc.txt`, `output.txt` of
//! snowball-data).
//!
//! A self-contained port instead of the `rust-stemmers` crate: that crate
//! carries an older revision of the algorithm (before Snowball 3's changes to
//! `-ing`, `-eed` and the R1 exceptions) and every language's tables, and has
//! had no release since 2019.
//!
//! Words are handled as `char`s, like Snowball's UTF-8 mode: only the ASCII
//! letters below are vowels or suffix letters, so other characters are
//! consonants and are never removed.

/// Stems one lowercase word.
pub(super) fn stem(word: &str) -> String {
    if let Some(stem) = exception1(word) {
        return stem.to_owned();
    }
    let mut w: Vec<char> = word.chars().collect();
    if w.len() < 3 {
        return word.to_owned();
    }
    let y_found = prelude(&mut w);
    let (p1, p2) = mark_regions(&w);
    let mut s = Stemmer { w, p1, p2 };
    s.step_1a();
    s.step_1b();
    s.step_1c();
    s.step_2();
    s.step_3();
    s.step_4();
    s.step_5();
    let mut w = s.w;
    if y_found {
        for c in &mut w {
            if *c == 'Y' {
                *c = 'y';
            }
        }
    }
    w.into_iter().collect()
}

fn vowel(c: char) -> bool {
    matches!(c, 'a' | 'e' | 'i' | 'o' | 'u' | 'y')
}

fn exception1(word: &str) -> Option<&'static str> {
    Some(match word {
        "skis" => "ski",
        "skies" => "sky",
        "idly" => "idl",
        "gently" => "gentl",
        "ugly" => "ugli",
        "early" => "earli",
        "only" => "onli",
        "singly" => "singl",
        "sky" => "sky",
        "news" => "news",
        "howe" => "howe",
        "atlas" => "atlas",
        "cosmos" => "cosmos",
        "bias" => "bias",
        "andes" => "andes",
        _ => return None,
    })
}

/// Drops a leading apostrophe and marks consonant `y`s as `Y`. Returns
/// whether any `Y` was set.
fn prelude(w: &mut Vec<char>) -> bool {
    if w.first() == Some(&'\'') {
        w.remove(0);
    }
    let mut found = false;
    if w.first() == Some(&'y') {
        w[0] = 'Y';
        found = true;
    }
    for i in 1..w.len() {
        if w[i] == 'y' && vowel(w[i - 1]) {
            w[i] = 'Y';
            found = true;
        }
    }
    found
}

/// R1 and R2 as char positions (`len` when empty).
fn mark_regions(w: &[char]) -> (usize, usize) {
    const R1_PREFIXES: &[&str] = &[
        "gener", "commun", "arsen", "past", "univers", "later", "emerg", "organ", "inter",
    ];
    let len = w.len();
    // After the first non-vowel that follows a vowel, from `from`.
    let region = |from: usize| -> usize {
        let Some(v) = (from..len).find(|&i| vowel(w[i])) else {
            return len;
        };
        (v + 1..len).find(|&i| !vowel(w[i])).map_or(len, |i| i + 1)
    };
    let p1 = R1_PREFIXES
        .iter()
        .filter(|p| starts_with(w, p))
        .map(|p| p.len())
        .max()
        .unwrap_or_else(|| region(0));
    let p2 = if p1 < len { region(p1) } else { len };
    (p1, p2)
}

fn starts_with(w: &[char], p: &str) -> bool {
    w.len() >= p.len() && p.chars().zip(w).all(|(a, &b)| a == b)
}

struct Stemmer {
    w: Vec<char>,
    p1: usize,
    p2: usize,
}

impl Stemmer {
    fn ends_at(&self, end: usize, s: &str) -> bool {
        let n = s.chars().count();
        end >= n && s.chars().zip(&self.w[end - n..end]).all(|(a, &b)| a == b)
    }

    fn ends(&self, s: &str) -> bool {
        self.ends_at(self.w.len(), s)
    }

    /// The longest of `suffixes` the word ends with, and where it starts.
    fn longest<'a>(&self, suffixes: &[&'a str]) -> Option<(&'a str, usize)> {
        suffixes
            .iter()
            .filter(|s| self.ends(s))
            .max_by_key(|s| s.len())
            .map(|s| (*s, self.w.len() - s.chars().count()))
    }

    fn replace_from(&mut self, at: usize, with: &str) {
        self.w.truncate(at);
        self.w.extend(with.chars());
    }

    /// Snowball's `shortv` at the end of `self.w[..end]`.
    fn short_v(&self, end: usize) -> bool {
        let w = &self.w[..end];
        let n = w.len();
        let non_v_wxy = |c: char| !vowel(c) && !matches!(c, 'w' | 'x' | 'Y');
        // Backward mode reads the pattern from the end: the last letter is
        // the one that must not be w, x or Y.
        (n >= 3 && !vowel(w[n - 3]) && vowel(w[n - 2]) && non_v_wxy(w[n - 1]))
            || (n == 2 && vowel(w[0]) && !vowel(w[1]))
            || self.ends_at(end, "past")
    }

    fn step_1a(&mut self) {
        if let Some((_, at)) = self.longest(&["'", "'s", "'s'"]) {
            self.w.truncate(at);
        }
        let Some((suffix, at)) = self.longest(&["sses", "ied", "ies", "s", "us", "ss"]) else {
            return;
        };
        match suffix {
            "sses" => self.replace_from(at, "ss"),
            "ied" | "ies" => self.replace_from(at, if at >= 2 { "i" } else { "ie" }),
            "s" => {
                // A vowel before the letter preceding the `s`.
                if at >= 1 && self.w[..at - 1].iter().any(|&c| vowel(c)) {
                    self.w.truncate(at);
                }
            }
            _ => {}
        }
    }

    fn step_1b(&mut self) {
        let suffix = self.longest(&["eed", "eedly", "ed", "edly", "ingly", "ing"]);
        let Some((suffix, at)) = suffix else { return };
        match suffix {
            "eed" | "eedly" => {
                if at >= self.p1 {
                    let stem = &self.w[..at];
                    let exception = ["proc", "exc", "succ"]
                        .iter()
                        .any(|e| stem.len() == e.len() && starts_with(stem, e));
                    if !exception {
                        self.replace_from(at, "ee");
                    }
                }
                return;
            }
            "ing" => {
                let stem = &self.w[..at];
                // dying -> die, lying -> lie, tying -> tie, vying -> vie.
                if stem.len() == 2 && stem[1] == 'y' && !vowel(stem[0]) {
                    self.replace_from(at - 1, "ie");
                    return;
                }
                if ["inn", "out", "cann", "herr", "earr", "even"]
                    .iter()
                    .any(|e| stem.len() == e.len() && starts_with(stem, e))
                {
                    return;
                }
            }
            _ => {}
        }
        // `ed`, `edly`, `ing`, `ingly`: delete if the stem has a vowel, then
        // tidy the stem's end.
        if !self.w[..at].iter().any(|&c| vowel(c)) {
            return;
        }
        self.w.truncate(at);
        let n = self.w.len();
        if self.ends("at") || self.ends("bl") || self.ends("iz") {
            self.w.push('e');
        } else if n >= 2
            && self.w[n - 1] == self.w[n - 2]
            && matches!(
                self.w[n - 1],
                'b' | 'd' | 'f' | 'g' | 'm' | 'n' | 'p' | 'r' | 't'
            )
        {
            // Not after a lone leading `a`, `e` or `o` (add, ebb, odd).
            if !(n == 3 && matches!(self.w[0], 'a' | 'e' | 'o')) {
                self.w.pop();
            }
        } else if n == self.p1 && self.short_v(n) {
            self.w.push('e');
        }
    }

    fn step_1c(&mut self) {
        let n = self.w.len();
        if n >= 3 && matches!(self.w[n - 1], 'y' | 'Y') && !vowel(self.w[n - 2]) {
            self.w[n - 1] = 'i';
        }
    }

    fn step_2(&mut self) {
        const SUFFIXES: &[(&str, &str)] = &[
            ("tional", "tion"),
            ("enci", "ence"),
            ("anci", "ance"),
            ("abli", "able"),
            ("entli", "ent"),
            ("izer", "ize"),
            ("ization", "ize"),
            ("ational", "ate"),
            ("ation", "ate"),
            ("ator", "ate"),
            ("alism", "al"),
            ("aliti", "al"),
            ("alli", "al"),
            ("fulness", "ful"),
            ("ousli", "ous"),
            ("ousness", "ous"),
            ("iveness", "ive"),
            ("iviti", "ive"),
            ("biliti", "ble"),
            ("bli", "ble"),
            ("ogist", "og"),
            ("ogi", "og"),
            ("fulli", "ful"),
            ("lessli", "less"),
            ("li", ""),
        ];
        let keys: Vec<&str> = SUFFIXES.iter().map(|(s, _)| *s).collect();
        let Some((suffix, at)) = self.longest(&keys) else {
            return;
        };
        if at < self.p1 {
            return;
        }
        let before = at.checked_sub(1).map(|i| self.w[i]);
        match suffix {
            "ogi" if before != Some('l') => {}
            "li" if !matches!(
                before,
                Some('c' | 'd' | 'e' | 'g' | 'h' | 'k' | 'm' | 'n' | 'r' | 't')
            ) => {}
            _ => {
                let with = SUFFIXES
                    .iter()
                    .find(|(s, _)| *s == suffix)
                    .expect("listed")
                    .1;
                self.replace_from(at, with);
            }
        }
    }

    fn step_3(&mut self) {
        const SUFFIXES: &[&str] = &[
            "tional", "ational", "alize", "icate", "iciti", "ical", "ful", "ness", "ative",
        ];
        let Some((suffix, at)) = self.longest(SUFFIXES) else {
            return;
        };
        if at < self.p1 {
            return;
        }
        match suffix {
            "tional" => self.replace_from(at, "tion"),
            "ational" => self.replace_from(at, "ate"),
            "alize" => self.replace_from(at, "al"),
            "icate" | "iciti" | "ical" => self.replace_from(at, "ic"),
            "ful" | "ness" => self.w.truncate(at),
            "ative" if at >= self.p2 => self.w.truncate(at),
            _ => {}
        }
    }

    fn step_4(&mut self) {
        const SUFFIXES: &[&str] = &[
            "al", "ance", "ence", "er", "ic", "able", "ible", "ant", "ement", "ment", "ent", "ism",
            "ate", "iti", "ous", "ive", "ize", "ion",
        ];
        let Some((suffix, at)) = self.longest(SUFFIXES) else {
            return;
        };
        if at < self.p2 {
            return;
        }
        if suffix == "ion" {
            if at >= 1 && matches!(self.w[at - 1], 's' | 't') {
                self.w.truncate(at);
            }
        } else {
            self.w.truncate(at);
        }
    }

    fn step_5(&mut self) {
        let n = self.w.len();
        match self.w.last() {
            Some('e') => {
                let at = n - 1;
                if at >= self.p2 || (at >= self.p1 && !self.short_v(at)) {
                    self.w.truncate(at);
                }
            }
            Some('l') => {
                let at = n - 1;
                if at >= self.p2 && at >= 1 && self.w[at - 1] == 'l' {
                    self.w.truncate(at);
                }
            }
            _ => {}
        }
    }
}

/// The port is checked word by word against Snowball's own vocabulary, so
/// this tests the private stemmer directly: the analyzer never hands it the
/// apostrophe-only words that vocabulary includes.
#[cfg(test)]
mod tests {
    use super::stem;

    fn check(pairs: &str) -> usize {
        let mut n = 0;
        for line in pairs.lines() {
            let (word, expected) = line.split_once(' ').expect("`word stem` lines");
            assert_eq!(stem(word), expected, "stem({word:?})");
            n += 1;
        }
        n
    }

    /// Every tenth word of Snowball v3.2.0's English vocabulary, plus every
    /// exception and every word with an apostrophe, stems as Snowball does.
    #[test]
    fn matches_snowball_vocabulary_sample() {
        let n = check(include_str!("testdata/snowball-english-sample.txt"));
        assert!(n > 4000);
    }

    /// The whole vocabulary (42 649 words), when `SNOWBALL_ENGLISH_VOC`
    /// names a directory holding snowball-data's `english/voc.txt` and
    /// `output.txt`.
    #[test]
    fn matches_full_snowball_vocabulary_when_given() {
        let Ok(dir) = std::env::var("SNOWBALL_ENGLISH_VOC") else {
            return;
        };
        let read = |f: &str| std::fs::read_to_string(format!("{dir}/{f}")).expect("readable");
        let (voc, out) = (read("voc.txt"), read("output.txt"));
        let pairs: Vec<String> = voc
            .lines()
            .zip(out.lines())
            .map(|(w, s)| format!("{w} {s}"))
            .collect();
        assert_eq!(check(&pairs.join("\n")), 42_649);
    }
}
