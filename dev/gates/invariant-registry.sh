#!/usr/bin/env bash
# Keep the Jazz and Groove invariant registries mechanically honest.
# `now` + `untested` is intentionally visible documented debt, not a failure.

# Do not inherit errexit: expected missing citations are accumulated below and
# every external command's status is read explicitly.
set +e
set -u -o pipefail

readonly JAZZ_REGISTRY="${JAZZ_INVARIANT_REGISTRY:-crates/jazz/SPEC/INVARIANTS.md}"
readonly GROOVE_REGISTRY="${GROOVE_INVARIANT_REGISTRY:-crates/groove/SPEC/INVARIANTS.md}"

failures=0
rows=0
missing_tests=0
uncited_covered=0
now_untested=0
declare -A known_functions=()

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

# Emits six unit-separator-delimited fields for each structurally valid registry
# row. A literal Markdown pipe must be written as \|; the parser retains it in
# the field rather than treating it as a table boundary.
parse_registry() {
    local registry=$1 parsed_status
    PARSED_ROWS="$(perl - "$registry" <<'PERL'
use strict;
use warnings;

my ($path) = @ARGV;
open my $fh, '<', $path or die "cannot read $path: $!\n";
my ($line_no, $state, $bad) = (0, 'before-header', 0);

sub trim {
    my ($value) = @_;
    $value =~ s/^\s+|\s+$//g;
    return $value;
}

sub split_row {
    my ($line) = @_;
    return unless $line =~ /^\|.*\|$/;
    my ($cell, @fields) = ('');
    my $inside = substr($line, 1, -1);
    for (my $i = 0; $i < length $inside; $i++) {
        my $char = substr($inside, $i, 1);
        if ($char eq '\\' && $i + 1 < length($inside) && substr($inside, $i + 1, 1) eq '|') {
            $cell .= '\\|';
            $i++;
        } elsif ($char eq '|') {
            push @fields, trim($cell);
            $cell = '';
        } else {
            $cell .= $char;
        }
    }
    push @fields, trim($cell);
    return @fields;
}

while (my $line = <$fh>) {
    chomp $line;
    $line_no++;
    if ($state eq 'before-header') {
        if ($line =~ /^\|\s*id\s*\|/) {
            my @fields = split_row($line);
            if (@fields != 6 || $fields[0] ne 'id' || $fields[1] ne 'invariant' || $fields[2] ne 'enforced by (test)' || $fields[3] ne 'impl' || $fields[4] ne 'status' || $fields[5] ne 'coverage') {
                warn "$path:$line_no: malformed invariant-registry header\n";
                $bad++;
                last;
            }
            $state = 'separator';
        } elsif ($line =~ /^\|/) {
            warn "$path:$line_no: table row before invariant-registry header\n";
            $bad++;
        }
    } elsif ($state eq 'separator') {
        my @fields = split_row($line);
        if (@fields != 6 || grep { !/^:?-{3,}:?$/ } @fields) {
            warn "$path:$line_no: malformed table separator\n";
            $bad++;
        }
        $state = 'rows';
    } elsif ($state eq 'rows') {
        if ($line !~ /^\|/) {
            $state = 'after-table';
            next;
        }
        my @fields = split_row($line);
        if (@fields != 6) {
            warn "$path:$line_no: malformed row (expected six columns; escape literal pipes as \\|)\n";
            $bad++;
            next;
        }
        print join("\x1f", @fields), "\n";
    } elsif ($state eq 'after-table' && $line =~ /^\|/) {
        warn "$path:$line_no: unexpected table row after invariant registry\n";
        $bad++;
    }
}

if ($state eq 'before-header' || $state eq 'separator') {
    warn "$path: invariant registry table is missing or incomplete\n";
    $bad++;
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
    local output status source_line function
    output="$(git grep -h -E 'fn[[:space:]]+[A-Za-z_][A-Za-z0-9_]*[[:space:]]*\(' -- crates)"
    status=$?
    if (( status != 0 )); then
        fail "git grep could not build the Rust test-function index (exit $status)"
        return
    fi
    while IFS= read -r source_line; do
        if [[ $source_line =~ fn[[:space:]]+([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*\( ]]; then
            function=${BASH_REMATCH[1]}
            known_functions[$function]=1
        fi
    done <<< "$output"
}

check_test_citations() {
    local registry=$1 id=$2 text=$3 remaining citation function prefix brace_names brace_name test_path
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
        function=${citation##*::}
        if [[ ! $function =~ ^[A-Za-z_][A-Za-z0-9_]*$ || -z ${known_functions[$function]+present} ]]; then
            printf 'invariant-registry: missing test: %s:%s: %s\n' "$registry" "$id" "$citation" >&2
            missing_tests=$((missing_tests + 1))
            failures=$((failures + 1))
        fi
    done
    remaining=$text
    while [[ $remaining =~ (packages/[A-Za-z0-9_./-]+\.test\.(ts|tsx)) ]]; do
        test_path=${BASH_REMATCH[1]}
        remaining=${remaining#*"$test_path"}
        if [[ ! -f $test_path ]]; then
            printf 'invariant-registry: missing test file: %s:%s: %s\n' "$registry" "$id" "$test_path" >&2
            missing_tests=$((missing_tests + 1))
            failures=$((failures + 1))
        fi
    done
}

check_registry() {
    local registry=$1 record id invariant tests impl status coverage registry_rows=0
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
    printf 'invariant-registry: checked %s (%d rows)\n' "$registry" "$registry_rows"
}

require_command git || exit 1
require_command perl || exit 1
index_test_functions
check_registry "$JAZZ_REGISTRY"
check_registry "$GROOVE_REGISTRY"
printf 'invariant-registry: summary: %d rows, %d missing test citations, %d covered rows without a test, %d now + untested (not failing)\n' \
    "$rows" "$missing_tests" "$uncited_covered" "$now_untested"

(( failures == 0 ))
