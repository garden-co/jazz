#!/usr/bin/env perl
# One-time/maintenance converter from the former Markdown table to the
# record-per-invariant registry. Kept so future imports can be mechanically
# checked rather than retyped.
use strict;
use warnings;
use File::Path qw(make_path);
use Digest::SHA qw(sha256_hex);

my ($source, $destination, $receipt_flag, $receipt_path) = @ARGV;
die "usage: $0 OLD-INVARIANTS.md DESTINATION-DIR [--receipt PARITY.tsv]\n"
    unless $source && $destination && (!$receipt_flag || ($receipt_flag eq '--receipt' && $receipt_path));
open my $fh, '<', $source or die "cannot read $source: $!\n";

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

my $in_rows = 0;
my $count = 0;
my %parity;
while (my $line = <$fh>) {
    chomp $line;
    if (!$in_rows) {
        $in_rows = 1 if $line =~ /^\|\s*id\s*\|/;
        next;
    }
    next if $line =~ /^\|\s*:?-{3,}/;
    last if $line !~ /^\|/;
    my @fields = split_row($line);
    die "$source: malformed row\n" unless @fields == 6;
    my ($id, $invariant, $tests, $impl, $status, $coverage) = @fields;
    die "$source: invalid id $id\n" unless $id =~ /^(?:G-)?INV-[A-Za-z0-9-]+$/;
    for ($invariant, $tests, $impl) { s/\s+/ /g; s/^\s+|\s+$//g; }
    make_path($destination);
    my $path = "$destination/$id.md";
    die "$path already exists\n" if -e $path;
    open my $out, '>', $path or die "cannot write $path: $!\n";
    print {$out} "# $id\n\n";
    print {$out} "- Status: $status\n";
    print {$out} "- Coverage: $coverage\n\n";
    print {$out} "## Invariant\n\n$invariant\n\n";
    print {$out} "## Enforced by (tests)\n\n$tests\n\n";
    print {$out} "## Implementation";
    print {$out} length($impl) ? "\n\n$impl\n" : "\n";
    close $out or die "cannot close $path: $!\n";
    $parity{$id} = sha256_hex(join("\x1f", $id, $invariant, $tests, $impl, $status, $coverage));
    $count++;
}
if ($receipt_path) {
    open my $receipt, '>', $receipt_path or die "cannot write $receipt_path: $!\n";
    print {$receipt} "# Canonical normalized field hashes for the legacy-table migration.\n";
    print {$receipt} "# Each hash covers id, invariant, tests, implementation, status, coverage.\n";
    for my $id (sort keys %parity) { print {$receipt} "$id\t$parity{$id}\n"; }
    close $receipt or die "cannot close $receipt_path: $!\n";
}
print "$source: wrote $count records to $destination\n";
