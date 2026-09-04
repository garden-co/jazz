#!/usr/bin/env bash
# Keep the Jazz and Groove invariant registries mechanically honest.
# `now` + `untested` is intentionally visible documented debt, not a failure.

# Do not inherit errexit: expected missing citations are accumulated below and
# every external command's status is read explicitly.
set +e
set -u -o pipefail

readonly REGISTRY="${INVARIANT_REGISTRY:-crates/invariant-registry.jsonl}"
readonly PARITY_RECEIPT="${INVARIANT_PARITY_RECEIPT:-crates/invariant-registry-parity.tsv}"
# This is a one-time migration commitment, not a registry. It pins every
# historical ID and field hash while later JSONL records remain appendable
# without touching the frozen receipt.
readonly LEGACY_PARITY_SHA256="12e257ba861b9613c8061a5b2f81fbe9e336b3f6e170bdd7d069ea70d216bdd8"
readonly LEGACY_JAZZ_RECORDS=334
readonly LEGACY_GROOVE_RECORDS=143

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

# Emits seven unit-separator-delimited fields for every canonical JSONL
# record. The single repository-wide registry is deliberately one physical
# record per line: unrelated changes conflict only when they edit one ID.
parse_registry() {
    local registry=$1 parsed_status
    PARSED_ROWS="$(perl - "$registry" <<'PERL'
use strict;
use warnings;
use Digest::SHA qw(sha256_hex);
use Encode qw(encode_utf8);
use JSON::PP ();
use open qw(:std :encoding(UTF-8));

my ($path) = @ARGV;
open my $fh, '<', $path or die "cannot read $path: $!\n";
my $json = JSON::PP->new->utf8(0);
my @keys = qw(domain id invariant tests implementation status coverage);
my $bad = 0;
my $previous = '';
my $line_number = 0;
while (my $line = <$fh>) {
    $line_number++;
    chomp $line;
    if ($line eq '') {
        warn "$path:$line_number: blank lines are not records\n";
        $bad++;
        next;
    }
    my $record = eval { $json->decode($line) };
    if (!$record || ref($record) ne 'HASH') {
        warn "$path:$line_number: malformed JSON object\n";
        $bad++;
        next;
    }
    my @actual_keys = sort keys %$record;
    if (join(',', @actual_keys) ne join(',', sort @keys)
        || grep { !defined($record->{$_}) || ref($record->{$_}) } @keys) {
        warn "$path:$line_number: record must contain exactly the seven scalar fields\n";
        $bad++;
        next;
    }
    my $canonical = '{' . join(',', map { $json->encode($_) . ':' . $json->encode($record->{$_}) } @keys) . '}';
    if ($line ne $canonical) {
        warn "$path:$line_number: non-canonical JSONL spelling or field order\n";
        $bad++;
        next;
    }
    my $sort_key = "$record->{id}\0$record->{domain}";
    if ($previous ne '' && $sort_key le $previous) {
        warn "$path:$line_number: records must be strictly sorted by id then domain\n";
        $bad++;
        next;
    }
    $previous = $sort_key;
    my @fields = @{$record}{@keys};
    print join("\x1f", @fields, sha256_hex(encode_utf8(join("\x1f", @fields)))), "\n";
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
    local registry=$1 receipt=$2 row domain id expected actual_hash receipt_hash jazz_records=0 groove_records=0
    local -A actual=()
    if [[ ! -f $receipt ]]; then
        fail "$registry: missing migration parity receipt $receipt"
        return
    fi
    while IFS= read -r row; do
        [[ -n $row ]] || continue
        domain=${row%%$'\x1f'*}
        id=${row#*$'\x1f'}; id=${id%%$'\x1f'*}
        actual["$domain:$id"]=${row##*$'\x1f'}
    done <<< "$PARSED_ROWS"
    receipt_hash="$(perl -MDigest::SHA=sha256_hex -e 'open my $fh, "<:raw", shift or die $!; local $/; print sha256_hex(<$fh>)' "$receipt")"
    if [[ $receipt_hash != "$LEGACY_PARITY_SHA256" ]]; then
        fail "$receipt: frozen legacy parity receipt digest differs"
    fi
    while IFS=$'\t' read -r domain id expected; do
        [[ -z $domain || $domain == \#* ]] && continue
        if [[ ! $domain =~ ^(jazz|groove)$ || ! $id =~ ^(G-)?INV-[A-Za-z0-9-]+$ || ! $expected =~ ^[0-9a-f]{64}$ ]]; then
            fail "$receipt: malformed parity row '$domain:$id'"
            continue
        fi
        actual_hash=${actual["$domain:$id"]-}
        if [[ -z $actual_hash ]]; then
            fail "$registry: migrated invariant $domain:$id is missing"
        elif [[ $actual_hash != "$expected" ]]; then
            fail "$registry: migrated invariant $domain:$id changed from its canonical legacy fields"
        fi
        case $domain in
            jazz) jazz_records=$((jazz_records + 1)) ;;
            groove) groove_records=$((groove_records + 1)) ;;
        esac
    done < "$receipt"
    if (( jazz_records != LEGACY_JAZZ_RECORDS || groove_records != LEGACY_GROOVE_RECORDS )); then
        fail "$receipt: frozen legacy inventory count differs (expected jazz=$LEGACY_JAZZ_RECORDS groove=$LEGACY_GROOVE_RECORDS; got jazz=$jazz_records groove=$groove_records)"
    fi
}

check_registry() {
    local registry=$1 receipt=$2 record domain id invariant tests impl status coverage record_hash registry_rows=0
    local -A seen_ids=()
    parse_registry "$registry"
    while IFS=$'\x1f' read -r domain id invariant tests impl status coverage record_hash; do
        [[ -n $id ]] || continue
        if [[ ! $domain =~ ^(jazz|groove)$ ]]; then
            fail "$registry: invalid invariant domain '$domain'"
            continue
        fi
        if [[ ! $id =~ ^(G-)?INV-[A-Za-z0-9-]+$ ]]; then
            fail "$registry:$domain: invalid invariant id '$id'"
            continue
        fi
        if [[ -n ${seen_ids["$domain:$id"]+present} ]]; then
            fail "$registry: duplicate invariant id '$domain:$id'"
        else
            seen_ids["$domain:$id"]=1
        fi
        if [[ -z $status || -z $coverage ]]; then
            fail "$registry:$domain:$id: status and coverage must not be empty"
        fi
        case $status in
            now|target|next|planned|prov|open) ;;
            *) fail "$registry:$domain:$id: unknown status '$status'" ;;
        esac
        if [[ $coverage == '✓' \
            && ! $tests =~ [a-z][a-z0-9_]*(::[A-Za-z0-9_*]+)+ \
            && ! $tests =~ packages/[A-Za-z0-9_./-]+\.test\.(ts|tsx) ]]; then
            printf 'invariant-registry: covered without test: %s:%s:%s\n' "$registry" "$domain" "$id" >&2
            uncited_covered=$((uncited_covered + 1))
            failures=$((failures + 1))
        fi
        if [[ $status == 'now' && $coverage == 'untested' ]]; then
            now_untested=$((now_untested + 1))
            printf 'invariant-registry: documented debt (not failing): %s:%s:%s is now + untested\n' "$registry" "$domain" "$id" >&2
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
index_test_functions
check_registry "$REGISTRY" "$PARITY_RECEIPT"
printf 'invariant-registry: summary: %d rows, %d missing test citations, %d covered rows without a test, %d now + untested (not failing)\n' \
    "$rows" "$missing_tests" "$uncited_covered" "$now_untested"

(( failures == 0 ))
