#!/usr/bin/env perl
# Index test declarations from tracked Rust paths supplied on stdin.
# local_tokio_test! is a known test-producing wrapper; other macros are not.
use strict;
use warnings;

my $comment = qr{//[^\n]*(?:\n|\z)|/\*.*?\*/}s;
my $attribute = qr{\#\[[^\]]*\]}s;
my $metadata = qr{(?:(?:$comment|$attribute)\s*)*};
while (my $file = <STDIN>) {
    chomp $file;
    next unless $file =~ /\.rs\z/;
    open my $fh, '<', $file or die "cannot read $file: $!\n";
    my $source = do { local $/; <$fh> };
    close $fh;
    while ($source =~ /^\h*(local_tokio_test!\s*\{\s*)?($metadata)(?:pub(?:\([^)]*\))?\s+)?(async\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(/mg) {
        my ($wrapper, $attrs, $async, $name) = ($1, $2, $3, $4);
        # Documentation mentioning #[test] does not make a helper a test.
        $attrs =~ s/$comment//g;
        next unless ($wrapper && $async)
            || $attrs =~ /\#\[\s*(?:[A-Za-z_][A-Za-z0-9_]*::)*test(?:\s*\([^]]*\))?\s*\]/;
        print "$name\x1f$file\n";
    }
}
