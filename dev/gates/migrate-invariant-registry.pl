#!/usr/bin/env perl
# One-time/maintenance conversion of the former per-domain Markdown tables to
# the one-line-per-record JSONL registry. JSON is emitted in one fixed field
# order, so a semantic edit changes only that record's physical line.
use strict;
use warnings;
use Digest::SHA qw(sha256_hex);
use Encode qw(encode_utf8);
use JSON::PP ();
use open qw(:std :encoding(UTF-8));

my $json = JSON::PP->new->utf8(0);

my ($jazz_source, $groove_source, $registry_path, $receipt_flag, $receipt_path) = @ARGV;
die "usage: $0 JAZZ-INVARIANTS.md GROOVE-INVARIANTS.md REGISTRY.jsonl --receipt PARITY.tsv\n"
    unless $jazz_source && $groove_source && $registry_path
        && $receipt_flag eq '--receipt' && $receipt_path;

sub trim { my ($value) = @_; $value =~ s/^\s+|\s+$//g; return $value; }
sub split_row {
    my ($line) = @_;
    return unless $line =~ /^\|.*\|$/;
    my ($cell, @fields) = ('');
    my $inside = substr($line, 1, -1);
    for (my $i = 0; $i < length $inside; $i++) {
        my $char = substr($inside, $i, 1);
        if ($char eq '\\' && $i + 1 < length($inside) && substr($inside, $i + 1, 1) eq '|') {
            $cell .= '|'; $i++;
        } elsif ($char eq '|') { push @fields, trim($cell); $cell = ''; }
        else { $cell .= $char; }
    }
    push @fields, trim($cell);
    return @fields;
}
sub parse_table {
    my ($domain, $source) = @_;
    open my $fh, '<', $source or die "cannot read $source: $!\n";
    my ($in_rows, @records) = (0);
    while (my $line = <$fh>) {
        chomp $line;
        if (!$in_rows) { $in_rows = 1 if $line =~ /^\|\s*id\s*\|/; next; }
        next if $line =~ /^\|\s*:?-{3,}/;
        last if $line !~ /^\|/;
        my @fields = split_row($line);
        die "$source: malformed row\n" unless @fields == 6;
        my ($id, $invariant, $tests, $implementation, $status, $coverage) = @fields;
        die "$source: invalid id $id\n" unless $id =~ /^(?:G-)?INV-[A-Za-z0-9-]+$/;
        for ($invariant, $tests, $implementation) { s/\s+/ /g; s/^\s+|\s+$//g; }
        push @records, {
            domain => $domain, id => $id, invariant => $invariant, tests => $tests,
            implementation => $implementation, status => $status, coverage => $coverage,
        };
    }
    die "$source: no legacy invariant table found; this historical converter only accepts the pre-JSONL table format\n"
        unless $in_rows;
    return @records;
}
sub canonical_line {
    my ($record) = @_;
    return '{' . join(',', map { $json->encode($_->[0]) . ':' . $json->encode($_->[1]) }
        ['domain', $record->{domain}], ['id', $record->{id}],
        ['invariant', $record->{invariant}], ['tests', $record->{tests}],
        ['implementation', $record->{implementation}], ['status', $record->{status}],
        ['coverage', $record->{coverage}]) . '}';
}

my @records = (parse_table('jazz', $jazz_source), parse_table('groove', $groove_source));
@records = sort { $a->{id} cmp $b->{id} || $a->{domain} cmp $b->{domain} } @records;
open my $registry, '>', $registry_path or die "cannot write $registry_path: $!\n";
open my $receipt, '>', $receipt_path or die "cannot write $receipt_path: $!\n";
print {$receipt} "# Canonical normalized legacy-table migration parity receipt.\n";
print {$receipt} "# domain, id, and every invariant field are covered by each SHA-256.\n";
for my $record (@records) {
    print {$registry} canonical_line($record), "\n";
    my $digest = sha256_hex(encode_utf8(join("\x1f", @{$record}{qw(domain id invariant tests implementation status coverage)})));
    print {$receipt} join("\t", $record->{domain}, $record->{id}, $digest), "\n";
}
close $registry or die "cannot close $registry_path: $!\n";
close $receipt or die "cannot close $receipt_path: $!\n";
print "wrote ", scalar(@records), " JSONL invariant records\n";
