use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::PathBuf;

use wasmparser::{KnownCustom, Name, Operator, Parser, Payload, TypeRef};

#[derive(Clone, Debug)]
struct FunctionCount {
    function_index: u32,
    name: String,
    call_indirect_sites: u32,
    return_call_indirect_sites: u32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let wasm_path = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("crates/jazz-wasm/pkg/jazz_wasm_bg.wasm"));
    let bytes = fs::read(&wasm_path)?;
    let mut imported_functions = 0_u32;
    let mut function_count = 0_u32;
    let mut next_body_index = 0_u32;
    let mut names = BTreeMap::<u32, String>::new();
    let mut rows = Vec::<FunctionCount>::new();

    for payload in Parser::new(0).parse_all(&bytes) {
        match payload? {
            Payload::ImportSection(imports) => {
                for import in imports.into_imports() {
                    if matches!(import?.ty, TypeRef::Func(_) | TypeRef::FuncExact(_)) {
                        imported_functions += 1;
                    }
                }
            }
            Payload::FunctionSection(functions) => {
                function_count = functions.count();
            }
            Payload::CustomSection(section) if section.name() == "name" => {
                if let KnownCustom::Name(reader) = section.as_known() {
                    for subsection in reader {
                        if let Name::Function(function_names) = subsection? {
                            for naming in function_names {
                                let naming = naming?;
                                names.insert(naming.index, naming.name.to_owned());
                            }
                        }
                    }
                }
            }
            Payload::CodeSectionEntry(body) => {
                let function_index = imported_functions + next_body_index;
                next_body_index += 1;
                let mut call_indirect_sites = 0_u32;
                let mut return_call_indirect_sites = 0_u32;
                for op in body.get_operators_reader()? {
                    match op? {
                        Operator::CallIndirect { .. } => call_indirect_sites += 1,
                        Operator::ReturnCallIndirect { .. } => return_call_indirect_sites += 1,
                        _ => {}
                    }
                }
                if call_indirect_sites > 0 || return_call_indirect_sites > 0 {
                    rows.push(FunctionCount {
                        function_index,
                        name: String::new(),
                        call_indirect_sites,
                        return_call_indirect_sites,
                    });
                }
            }
            _ => {}
        }
    }

    for row in &mut rows {
        row.name = names
            .get(&row.function_index)
            .cloned()
            .unwrap_or_else(|| format!("<function {}>", row.function_index));
    }

    rows.sort_by(|left, right| {
        right
            .call_indirect_sites
            .cmp(&left.call_indirect_sites)
            .then_with(|| right.return_call_indirect_sites.cmp(&left.return_call_indirect_sites))
            .then_with(|| left.name.cmp(&right.name))
    });

    let total_call_indirect: u32 = rows.iter().map(|row| row.call_indirect_sites).sum();
    let total_return_call_indirect: u32 = rows.iter().map(|row| row.return_call_indirect_sites).sum();
    let groove_call_indirect: u32 = rows
        .iter()
        .filter(|row| row.name.contains("groove"))
        .map(|row| row.call_indirect_sites)
        .sum();
    let groove_return_call_indirect: u32 = rows
        .iter()
        .filter(|row| row.name.contains("groove"))
        .map(|row| row.return_call_indirect_sites)
        .sum();
    let top = env::var("WASM_CALL_INDIRECT_TOP")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(40);

    println!("{{");
    println!("  \"wasmPath\": \"{}\",", json_string(&wasm_path.display().to_string()));
    println!("  \"importedFunctions\": {imported_functions},");
    println!("  \"functionCount\": {function_count},");
    println!("  \"totalCallIndirectSites\": {total_call_indirect},");
    println!("  \"totalReturnCallIndirectSites\": {total_return_call_indirect},");
    println!("  \"functionsWithIndirectCalls\": {},", rows.len());
    println!("  \"grooveCallIndirectSites\": {groove_call_indirect},");
    println!("  \"grooveReturnCallIndirectSites\": {groove_return_call_indirect},");
    println!(
        "  \"grooveFunctionsWithIndirectCalls\": {},",
        rows.iter().filter(|row| row.name.contains("groove")).count()
    );
    println!("  \"top\": [");
    for (index, row) in rows.iter().take(top).enumerate() {
        println!(
            "    {{\"functionIndex\":{},\"name\":\"{}\",\"callIndirectSites\":{},\"returnCallIndirectSites\":{}}}{}",
            row.function_index,
            json_string(&row.name),
            row.call_indirect_sites,
            row.return_call_indirect_sites,
            if index + 1 == rows.iter().take(top).count() {
                ""
            } else {
                ","
            }
        );
    }
    println!("  ]");
    println!("}}");

    Ok(())
}

fn json_string(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}
