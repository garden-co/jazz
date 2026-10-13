//! Acknowledged external perf control for a selected benchmark phase.
pub(super) struct PerfControl {
    control: std::fs::File,
    acknowledgements: std::io::BufReader<std::fs::File>,
}

impl PerfControl {
    pub(super) fn selected(backend: &str, phase: &str) -> Option<Self> {
        if std::env::var("JAZZ_PERF_PHASE").ok().as_deref() != Some(phase)
            || std::env::var("JAZZ_PERF_BACKEND").ok().as_deref() != Some(backend)
        {
            return None;
        }
        let open = |name| {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(std::env::var_os(name).expect("perf control requires control and ack FIFOs"))
                .expect("open perf control FIFO")
        };
        let mut control = Self {
            control: open("JAZZ_PERF_CONTROL_FIFO"),
            acknowledgements: std::io::BufReader::new(open("JAZZ_PERF_ACK_FIFO")),
        };
        control.command("enable");
        Some(control)
    }

    fn command(&mut self, command: &str) {
        use std::io::{BufRead, Write};
        writeln!(self.control, "{command}").expect("write perf command");
        self.control.flush().expect("flush perf command");
        let mut acknowledgement = String::new();
        self.acknowledgements
            .read_line(&mut acknowledgement)
            .expect("read perf acknowledgement");
        assert_eq!(
            acknowledgement.trim_matches(|ch: char| ch == '\0' || ch.is_ascii_whitespace()),
            "ack",
            "unexpected perf acknowledgement"
        );
    }
}

impl Drop for PerfControl {
    fn drop(&mut self) {
        self.command("disable");
    }
}
