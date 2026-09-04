#!/usr/bin/env bash
# Keep the Jazz and Groove invariant registries mechanically honest.
# `now` + `untested` is intentionally visible documented debt, not a failure.

# Do not inherit errexit: expected missing citations are accumulated below and
# every external command's status is read explicitly.
set +e
set -u -o pipefail

readonly JAZZ_REGISTRY="${JAZZ_INVARIANT_REGISTRY:-crates/jazz/SPEC/invariants}"
readonly GROOVE_REGISTRY="${GROOVE_INVARIANT_REGISTRY:-crates/groove/SPEC/invariants}"
readonly JAZZ_PARITY_RECEIPT="${JAZZ_INVARIANT_PARITY_RECEIPT:-crates/jazz/SPEC/invariant-registry-parity.tsv}"
readonly GROOVE_PARITY_RECEIPT="${GROOVE_INVARIANT_PARITY_RECEIPT:-crates/groove/SPEC/invariant-registry-parity.tsv}"

failures=0
rows=0
missing_tests=0
uncited_covered=0
now_untested=0
declare -A known_test_sources=()

fail() {
    printf 'invariant-registry: ERROR: %s\n' "$*" >&2
    failures=$((failures + 1))
}

require_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        fail "required command not found: $1"
        return 1
    fi
    return 0
}

# Emits six unit-separator-delimited fields for each structurally valid
# record. One file per invariant is the authoritative merge surface; its file
# name and heading are both the stable id. Paragraph text is normalized only
# for this gate's line-oriented citation checks, not as an encoding contract.
parse_registry() {
    local registry=$1 parsed_status
    PARSED_ROWS="$(perl - "$registry" <<'PERL'
use strict;
use warnings;
use File::Basename qw(basename);

my ($path) = @ARGV;
opendir my $dir, $path or die "cannot read $path: $!\n";
my @all_files = sort grep { !/^\./ && -f "$path/$_" } readdir $dir;
closedir $dir;
my $bad = 0;
my @files;
for my $file (@all_files) {
    if ($file !~ /^((?:G-)?INV-[A-Za-z0-9-]+)\.md\z/) {
        warn "$path/$file: invariant record files must be named INV-*.md\n";
        $bad++;
        next;
    }
    push @files, $file;
}
for my $file (@files) {
    my $full_path = "$path/$file";
    open my $fh, '<', $full_path or die "cannot read $full_path: $!\n";
    local $/;
    my $text = <$fh>;
    close $fh;
    my ($id, $status, $coverage, $invariant, $tests, $impl) = $text =~
        /\A# ((?:G-)?INV-[A-Za-z0-9-]+)\n\n- Status: ([^\n]+)\n- Coverage: ([^\n]+)\n\n## Invariant\n\n(.*?)\n\n## Enforced by \(tests\)\n\n(.*?)\n\n## Implementation(?:\n\n(.*?))?\n?\z/s;
    if (!defined $id) {
        warn "$full_path: malformed invariant record\n";
        $bad++;
        next;
    }
    my $file_id = basename($file, '.md');
    if ($id ne $file_id) {
        warn "$full_path: heading id $id does not match file name $file_id\n";
        $bad++;
        next;
    }
    $impl //= '';
    for ($invariant, $tests, $impl) { s/\s+/ /g; s/^\s+|\s+$//g; }
    print join("\x1f", $id, $invariant, $tests, $impl, $status, $coverage), "\n";
}
exit($bad ? 1 : 0);
PERL
)"
    parsed_status=$?
    if (( parsed_status != 0 )); then
        fail "$registry: parser failed (exit $parsed_status)"
    fi
}

index_test_functions() {
    local output status function source_path
    output="$(perl - <<'PERL'
use strict;
use warnings;
for my $file (grep { chomp; /\.rs\z/ } `git ls-files -- crates`) {
    open my $fh, '<', $file or die "cannot read $file: $!\n";
    my @lines = <$fh>;
    close $fh;
    for my $index (0 .. $#lines) {
        my $line = $lines[$index];
        next unless $line =~ /^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(/;
        my $name = $1;
        my $attrs = join '', @lines[($index > 4 ? $index - 4 : 0) .. $index - 1];
        next unless $attrs =~ /#\[\s*(?:[A-Za-z_][A-Za-z0-9_]*::)*test(?:\s*\([^]]*\))?\s*\]/;
        print "$name\x1f$file\n";
    }
}
PERL
)"
    status=$?
    if (( status != 0 )); then
        fail "could not build the Rust test-item index (exit $status)"
        return
    fi
    while IFS=$'\x1f' read -r function source_path; do
        [[ -n $function && -n $source_path ]] || continue
        known_test_sources[$function]+=" $source_path"
    done <<< "$output"
}

citation_matches_test_source() {
    local citation=$1 source_path=$2 crate leaf citation_parts modules source_component position=0
    citation_parts=${citation//::/ }
    read -r -a modules <<< "$citation_parts"
    crate=${modules[0]}
    leaf=${modules[${#modules[@]} - 1]}
    case $crate in
        jazz) [[ $source_path == crates/jazz/* ]] || return 1 ;;
        groove) [[ $source_path == crates/groove/* ]] || return 1 ;;
        jazz_tools) [[ $source_path == crates/jazz-testkit/* ]] || return 1 ;;
        jazz_server) [[ $source_path == crates/jazz-server/* ]] || return 1 ;;
    esac
    if [[ $crate != jazz && $crate != groove && $crate != jazz_tools && $crate != jazz_server ]]; then
        [[ $source_path == "crates/"*"/tests/$crate.rs" ]] && return 0
        return 1
    fi
    source_path=${source_path%.rs}
    source_path=${source_path//\//::}
    source_path="::${source_path}::"
    for ((i = 1; i < ${#modules[@]} - 1; i++)); do
        # `harness` is the registry's stable logical mount for node test helpers;
        # physical test files remain free to move beneath `node/tests/`.
        [[ ${modules[i]} == tests || ${modules[i]} == harness || ${modules[i]} == *_tests ]] && continue
        source_component="::${modules[i]}::"
        position=${source_path#*"$source_component"}
        [[ $position != "$source_path" ]] || return 1
        source_path="::$position"
    done
    return 0
}

check_test_citations() {
    local registry=$1 id=$2 text=$3 remaining citation prefix brace_names brace_name test_path function source_path matched
    remaining=$text
    while [[ $remaining =~ ([a-z][a-z0-9_]*(::[A-Za-z0-9_]+)+)::\{([^}]*)\} ]]; do
        citation=${BASH_REMATCH[0]}
        prefix=${BASH_REMATCH[1]}
        brace_names=${BASH_REMATCH[3]}
        remaining=${remaining#*"$citation"}
        while IFS= read -r brace_name; do
            brace_name=${brace_name//[[:space:]]/}
            [[ -n $brace_name ]] || continue
            check_test_citations "$registry" "$id" "$prefix::$brace_name"
        done <<< "${brace_names//,/$'\n'}"
    done
    while [[ $remaining =~ [a-z][a-z0-9_]*(::[A-Za-z0-9_*]+)+ ]]; do
        citation=${BASH_REMATCH[0]}
        remaining=${remaining#*"$citation"}
        [[ $citation == rs::* ]] && continue
        function=${citation##*::}
        matched=0
        for source_path in ${known_test_sources[$function]-}; do
            if citation_matches_test_source "$citation" "$source_path"; then
                matched=1
                break
            fi
        done
        if (( ! matched )); then
            printf 'invariant-registry: missing test: %s:%s: %s\n' "$registry" "$id" "$citation" >&2
            missing_tests=$((missing_tests + 1))
            failures=$((failures + 1))
        fi
    done
    remaining=$text
    while [[ $remaining =~ (packages/[A-Za-z0-9_./-]+\.test\.(ts|tsx)) ]]; do
        test_path=${BASH_REMATCH[1]}
        remaining=${remaining#*"$test_path"}
        if [[ $test_path == *'..'* || $test_path != packages/* || -z $(git ls-files -- "$test_path") ]]; then
            printf 'invariant-registry: missing test file: %s:%s: %s\n' "$registry" "$id" "$test_path" >&2
            missing_tests=$((missing_tests + 1))
            failures=$((failures + 1))
        fi
    done
}

# The receipt is deliberately one-way: every legacy-table record must remain
# byte-identical after canonical whitespace normalization, while new records
# may be added without editing a shared inventory/count file.
check_migration_parity() {
    local registry=$1 receipt=$2 row id expected actual_hash
    local -A actual=()
    if [[ ! -f $receipt ]]; then
        fail "$registry: missing migration parity receipt $receipt"
        return
    fi
    while IFS= read -r row; do
        [[ -n $row ]] || continue
        id=${row%%$'\x1f'*}
        actual[$id]="$(printf '%s' "$row" | sha256sum | awk '{print $1}')"
    done <<< "$PARSED_ROWS"
    while IFS=$'\t' read -r id expected; do
        [[ -z $id || $id == \#* ]] && continue
        if [[ ! $id =~ ^(G-)?INV-[A-Za-z0-9-]+$ || ! $expected =~ ^[0-9a-f]{64}$ ]]; then
            fail "$receipt: malformed parity row '$id'"
            continue
        fi
        actual_hash=${actual[$id]-}
        if [[ -z $actual_hash ]]; then
            fail "$registry: migrated invariant $id is missing"
        elif [[ $actual_hash != "$expected" ]]; then
            fail "$registry: migrated invariant $id changed from its canonical legacy fields"
        fi
    done < "$receipt"
}

check_registry() {
    local registry=$1 receipt=$2 record id invariant tests impl status coverage registry_rows=0
    local -A seen_ids=()
    parse_registry "$registry"
    while IFS=$'\x1f' read -r id invariant tests impl status coverage; do
        [[ -n $id ]] || continue
        if [[ ! $id =~ ^(G-)?INV-[A-Za-z0-9-]+$ ]]; then
            fail "$registry: invalid invariant id '$id'"
            continue
        fi
        if [[ -n ${seen_ids[$id]+present} ]]; then
            fail "$registry: duplicate invariant id '$id'"
        else
            seen_ids[$id]=1
        fi
        if [[ -z $status || -z $coverage ]]; then
            fail "$registry:$id: status and coverage must not be empty"
        fi
        case $status in
            now|target|next|planned|prov|open) ;;
            *) fail "$registry:$id: unknown status '$status'" ;;
        esac
        if [[ $coverage == '✓' \
            && ! $tests =~ [a-z][a-z0-9_]*(::[A-Za-z0-9_*]+)+ \
            && ! $tests =~ packages/[A-Za-z0-9_./-]+\.test\.(ts|tsx) ]]; then
            printf 'invariant-registry: covered without test: %s:%s\n' "$registry" "$id" >&2
            uncited_covered=$((uncited_covered + 1))
            failures=$((failures + 1))
        fi
        if [[ $status == 'now' && $coverage == 'untested' ]]; then
            now_untested=$((now_untested + 1))
            printf 'invariant-registry: documented debt (not failing): %s:%s is now + untested\n' "$registry" "$id" >&2
        fi
        check_test_citations "$registry" "$id" "$tests"
        registry_rows=$((registry_rows + 1))
        rows=$((rows + 1))
    done <<< "$PARSED_ROWS"
    check_migration_parity "$registry" "$receipt"
    printf 'invariant-registry: checked %s (%d rows)\n' "$registry" "$registry_rows"
}

require_command git || exit 1
require_command perl || exit 1
require_command sha256sum || exit 1
index_test_functions
check_registry "$JAZZ_REGISTRY" "$JAZZ_PARITY_RECEIPT"
check_registry "$GROOVE_REGISTRY" "$GROOVE_PARITY_RECEIPT"
printf 'invariant-registry: summary: %d rows, %d missing test citations, %d covered rows without a test, %d now + untested (not failing)\n' \
    "$rows" "$missing_tests" "$uncited_covered" "$now_untested"

(( failures == 0 ))
