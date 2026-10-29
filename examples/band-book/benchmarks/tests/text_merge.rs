//! Receipts for the block text merge benchmarks: each case ends where its
//! name says, on every replica.

use jazz_example_band_book_benchmark::text_merge::{
    self, LONG_NOTE_EDITS, PARAGRAPH, Reconnect, note, scattered,
};

fn converged(reconnect: Reconnect) -> String {
    let (band, block) = reconnect.run();
    band.converged_text(block)
}

#[test]
fn the_fast_path_applies_the_lone_edit() {
    assert_eq!(
        converged(text_merge::fast_path()),
        format!("{PARAGRAPH} Count in on four.")
    );
}

#[test]
fn a_concurrent_paragraph_keeps_both_edits() {
    assert_eq!(
        converged(text_merge::concurrent_paragraph()),
        format!("Intro: four bars. {PARAGRAPH} Tuning: drop D.")
    );
}

#[test]
fn an_offline_chain_keeps_every_write_of_both_devices() {
    let writes = 20;
    let text = converged(text_merge::offline_chain(writes));
    let takes: String = (0..writes.div_ceil(4))
        .rev()
        .map(|i| format!("Take {i}. "))
        .collect();
    let bars: String = (0..writes).map(|i| format!(" Bar {i}.")).collect();
    assert_eq!(text, format!("{takes}{PARAGRAPH}{bars}"));
}

#[test]
fn a_long_note_keeps_both_sides_scattered_edits() {
    let bytes = 100_000;
    let text = converged(text_merge::long_note(bytes));
    assert_eq!(text.matches("Ada ").count(), LONG_NOTE_EDITS);
    assert_eq!(text.matches("Bo ").count(), LONG_NOTE_EDITS);
    // Taking both sides' insertions out gives back the note they started from.
    assert_eq!(text.replace("Ada ", "").replace("Bo ", ""), note(bytes, 1));
}

#[test]
fn past_the_work_budget_the_later_write_wins() {
    let (reconnect, later) = text_merge::over_budget(1_000_000);
    assert_eq!(converged(reconnect), later);
}

#[test]
fn scattered_edits_insert_whole_words() {
    let base = note(10_000, 3);
    let edited = scattered(&base, 50, 0.0, "X");
    assert_eq!(
        edited.matches(" X ").count() + edited.matches("\nX ").count(),
        50
    );
    assert_eq!(edited.replace("X ", ""), base);
}
