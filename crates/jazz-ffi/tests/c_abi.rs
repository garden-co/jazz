//! The C ABI Go calls, exercised from Rust.

mod common;

use std::ffi::{CStr, CString, c_char, c_void};
use std::ptr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use common::*;
use jazz_ffi::c_abi::*;
use serde_json::{Value, json};

fn c(text: &str) -> CString {
    CString::new(text).unwrap()
}

/// Takes ownership of a returned string.
fn owned(text: *mut c_char) -> String {
    assert!(!text.is_null());
    let copy = unsafe { CStr::from_ptr(text) }.to_str().unwrap().to_owned();
    unsafe { jazz_string_free(text) };
    copy
}

fn open(config: &str) -> Result<*mut JazzHandle, Value> {
    let mut err = ptr::null_mut();
    let handle = unsafe { jazz_open(c(config).as_ptr(), &mut err) };
    if handle.is_null() {
        Err(parse(&owned(err)))
    } else {
        assert!(err.is_null());
        Ok(handle)
    }
}

fn call(handle: *mut JazzHandle, method: &str, args: Value) -> Result<Value, Value> {
    let mut err = ptr::null_mut();
    let out = unsafe {
        jazz_call(
            handle,
            c(method).as_ptr(),
            c(&args.to_string()).as_ptr(),
            &mut err,
        )
    };
    if out.is_null() {
        Err(parse(&owned(err)))
    } else {
        assert!(err.is_null());
        Ok(parse(&owned(out)))
    }
}

#[derive(Default)]
struct Events(Mutex<Vec<(String, Value)>>);

unsafe extern "C" fn record(ctx: *mut c_void, kind: *const c_char, json: *const c_char) {
    let events = unsafe { &*(ctx as *const Events) };
    let kind = unsafe { CStr::from_ptr(kind) }.to_str().unwrap().to_owned();
    let json = parse(unsafe { CStr::from_ptr(json) }.to_str().unwrap());
    events.0.lock().unwrap().push((kind, json));
}

fn wait_for(events: &Events, count: usize) -> Vec<(String, Value)> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let seen = events.0.lock().unwrap().clone();
        if seen.len() >= count || Instant::now() > deadline {
            return seen;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn a_client_through_the_c_abi() {
    let h = open(&client_config()).unwrap();
    assert_eq!(call(h, "account", json!({})).unwrap(), Value::Null);
    let show = call(
        h,
        "insert",
        json!({ "table": "shows", "row": parse(&show(nil_uuid())) }),
    )
    .unwrap();
    let show_id = show["id"].as_str().unwrap().to_owned();

    let events = Box::new(Events::default());
    let mut err = ptr::null_mut();
    let sub = unsafe {
        jazz_subscribe(
            h,
            c(&tasks_of(&show_id)).as_ptr(),
            Some(record),
            &*events as *const Events as *mut c_void,
            &mut err,
        )
    };
    assert!(!sub.is_null() && err.is_null());
    let reset = wait_for(&events, 1);
    assert_eq!(reset[0].0, "delta");
    assert_eq!(reset[0].1["reset"], true);
    assert_eq!(reset[0].1["added"], json!([]));

    let rig = call(
        h,
        "insert",
        json!({ "table": "tasks", "row": parse(&task(&show_id, "Rig", 1.0)) }),
    )
    .unwrap();
    let id = rig["id"].as_str().unwrap().to_owned();
    assert_eq!(
        call(
            h,
            "update",
            json!({ "table": "tasks", "id": id, "patch": { "status": "doing" } })
        )
        .unwrap(),
        Value::Null
    );
    let one = call(h, "one", json!({ "query": { "table": "tasks", "where": [{ "column": "id", "op": "eq", "value": id }] } })).unwrap();
    assert_eq!(one["status"], "doing");
    assert_eq!(
        titles(&call(h, "all", json!({ "query": parse(&tasks_of(&show_id)) })).unwrap()),
        ["Rig"]
    );
    assert_eq!(
        call(h, "delete", json!({ "table": "tasks", "id": id })).unwrap(),
        Value::Null
    );
    assert_eq!(
        call(h, "one", json!({ "query": { "table": "tasks" } })).unwrap(),
        Value::Null
    );

    let seen = wait_for(&events, 4);
    let kinds: Vec<_> = seen
        .iter()
        .map(|(kind, delta)| (kind.as_str(), delta["reset"].as_bool()))
        .collect();
    assert_eq!(
        kinds[..4],
        [
            ("delta", Some(true)),
            ("delta", Some(false)),
            ("delta", Some(false)),
            ("delta", Some(false))
        ]
    );
    assert_eq!(seen[3].1["removed"], json!([id]));

    unsafe { jazz_unsubscribe(sub) };
    let before = events.0.lock().unwrap().len();
    call(
        h,
        "insert",
        json!({ "table": "tasks", "row": parse(&task(&show_id, "unseen", 2.0)) }),
    )
    .unwrap();
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(events.0.lock().unwrap().len(), before);

    unsafe { jazz_close(h) };
}

#[test]
fn errors_come_back_as_json_and_null_is_refused() {
    assert_eq!(open("{").unwrap_err()["kind"], "refused");
    let h = open(&client_config()).unwrap();
    let error = call(
        h,
        "all",
        json!({ "query": { "table": "shows", "include": {} } }),
    )
    .unwrap_err();
    assert_eq!(error["kind"], "refused");
    assert!(
        error["message"]
            .as_str()
            .unwrap()
            .starts_with("not supported yet")
    );
    assert_eq!(call(h, "nope", json!({})).unwrap_err()["kind"], "refused");
    assert_eq!(
        call(h, "insert", json!({ "table": "tasks" })).unwrap_err()["kind"],
        "refused"
    );

    // NULL arguments are refused, not dereferenced; NULL err is allowed.
    let mut err = ptr::null_mut();
    assert!(
        unsafe { jazz_call(ptr::null_mut(), c"all".as_ptr(), c"{}".as_ptr(), &mut err) }.is_null()
    );
    assert_eq!(parse(&owned(err))["message"], "the handle is NULL");
    assert!(unsafe { jazz_call(h, ptr::null(), c"{}".as_ptr(), ptr::null_mut()) }.is_null());
    let mut err = ptr::null_mut();
    assert!(
        unsafe {
            jazz_subscribe(
                h,
                c"{\"table\":\"tasks\"}".as_ptr(),
                None,
                ptr::null_mut(),
                &mut err,
            )
        }
        .is_null()
    );
    assert_eq!(parse(&owned(err))["kind"], "refused");
    unsafe {
        jazz_unsubscribe(ptr::null_mut());
        jazz_string_free(ptr::null_mut());
        jazz_close(h);
        jazz_close(ptr::null_mut());
    }
}

#[test]
fn returned_strings_are_the_callers_to_free() {
    let h = open(&client_config()).unwrap();
    // Many calls, each result freed once by `owned`: under a leak or
    // double-free checker (Miri, ASan) this is where ownership shows.
    for i in 0..50 {
        let row = call(
            h,
            "insert",
            json!({ "table": "shows", "row": parse(&show(nil_uuid())) }),
        )
        .unwrap();
        assert!(row["id"].is_string(), "{i}");
        call(h, "all", json!({ "query": { "table": "shows" } })).unwrap();
    }
    let mut err = ptr::null_mut();
    let out = unsafe {
        jazz_call(
            h,
            c"all".as_ptr(),
            c"{\"query\":{\"table\":\"shows\"}}".as_ptr(),
            &mut err,
        )
    };
    assert!(err.is_null());
    let text = unsafe { CStr::from_ptr(out) }.to_bytes().len();
    assert!(text > 0);
    unsafe { jazz_string_free(out) };
    unsafe { jazz_close(h) };
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backend_through_the_c_abi() {
    let app_id = jazz_app::AppId::random();
    let server = jazz_server::JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .unwrap();
    let config = backend_config(&server, &app_id.to_string(), "carol");
    let result = tokio::task::spawn_blocking(move || {
        let h = open(&config).unwrap();
        let account = call(h, "account", json!({})).unwrap();
        let account = account.as_str().expect("carol's account").to_owned();
        let show = call(
            h,
            "insert",
            json!({ "table": "shows", "row": parse(&show(&account)) }),
        )
        .unwrap();
        let show_id = show["id"].as_str().unwrap();
        call(
            h,
            "insert",
            json!({ "table": "tasks", "row": parse(&task(show_id, "Doors", 1.0)) }),
        )
        .unwrap();
        assert_eq!(
            titles(&call(h, "all", json!({ "query": parse(&tasks_of(show_id)) })).unwrap()),
            ["Doors"]
        );
        unsafe { jazz_close(h) };
    })
    .await;
    server.shutdown().await;
    result.unwrap();
}
