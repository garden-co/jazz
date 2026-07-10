// PROTOTYPE — interceptor spike, throwaway. Not production code.
// Minimal std-only loopback file server modeling the RN interceptor:
//   - binds 127.0.0.1 on a random port
//   - requires a per-boot secret path segment: GET /<secret>/files/<name>
//   - serves bytes from a backing dir with Range/206 support
//   - sends X-Content-Type-Options: nosniff
// Run: rustc -O loopback.rs -o /tmp/spike-loopback && /tmp/spike-loopback <dir>
// It prints "READY http://127.0.0.1:<port>/<secret>" then serves forever.

use std::io::{Read, Seek, SeekFrom, Write};
use std::net::TcpListener;

fn rand_hex(n: usize) -> String {
    // std-only randomness: hash of time + pid, good enough for a spike
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut s = String::new();
    let mut seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        ^ (std::process::id() as u128);
    while s.len() < n {
        let mut h = DefaultHasher::new();
        seed.hash(&mut h);
        seed = seed.wrapping_add(0x9e3779b97f4a7c15);
        s.push_str(&format!("{:016x}", h.finish()));
    }
    s.truncate(n);
    s
}

fn main() {
    let dir = std::env::args().nth(1).expect("usage: loopback <dir>");
    let secret = rand_hex(32);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    println!("READY http://127.0.0.1:{}/{}", port, secret);

    for stream in listener.incoming() {
        let mut stream = match stream {
            Ok(s) => s,
            Err(_) => continue,
        };
        let mut buf = [0u8; 4096];
        let n = match stream.read(&mut buf) {
            Ok(n) => n,
            Err(_) => continue,
        };
        let req = String::from_utf8_lossy(&buf[..n]).to_string();
        let mut lines = req.lines();
        let first = lines.next().unwrap_or("");
        let path = first.split_whitespace().nth(1).unwrap_or("/");
        let range = lines
            .find(|l| l.to_ascii_lowercase().starts_with("range:"))
            .map(|l| l.splitn(2, ':').nth(1).unwrap().trim().to_string());

        let expected_prefix = format!("/{}/files/", secret);
        if !path.starts_with(&expected_prefix) {
            let _ = stream.write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n");
            continue;
        }
        let name = &path[expected_prefix.len()..];
        let fpath = format!("{}/{}", dir, name);
        let mut file = match std::fs::File::open(&fpath) {
            Ok(f) => f,
            Err(_) => {
                let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n");
                continue;
            }
        };
        let size = file.metadata().unwrap().len();

        let (status, start, end) = match range.as_deref() {
            Some(r) if r.starts_with("bytes=") => {
                let spec = &r[6..];
                let mut parts = spec.splitn(2, '-');
                let a = parts.next().unwrap_or("");
                let b = parts.next().unwrap_or("");
                if a.is_empty() {
                    let sfx: u64 = b.parse().unwrap_or(0);
                    ("206", size.saturating_sub(sfx), size - 1)
                } else {
                    let s: u64 = a.parse().unwrap_or(0);
                    let e: u64 = if b.is_empty() { size - 1 } else { b.parse().unwrap_or(size - 1).min(size - 1) };
                    ("206", s, e)
                }
            }
            _ => ("200", 0, size - 1),
        };
        let len = end - start + 1;
        file.seek(SeekFrom::Start(start)).unwrap();
        let mut body = vec![0u8; len as usize];
        file.read_exact(&mut body).unwrap();

        let mut head = format!(
            "HTTP/1.1 {} X\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nX-Content-Type-Options: nosniff\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n",
            if status == "206" { "206 Partial Content" } else { "200 OK" },
            len
        );
        if status == "206" {
            head.push_str(&format!("Content-Range: bytes {}-{}/{}\r\n", start, end, size));
        }
        head.push_str("\r\n");
        let _ = stream.write_all(head.as_bytes());
        let _ = stream.write_all(&body);
    }
}
