package Data::Dedup::Files::CLI::_guts;
use 5.016;
use strict;
use warnings;
use mop 0.02;
use signatures 0.07;

use CLI::Startup 0.08;
use Data::Dedup::Files;


=head1 NAME

Data::Dedup::Files::CLI - A command-line interface to deduplicate files.

=cut


my %options = (
    'dir|d=s@' => 'directory under which to scan',
    'quiet|q' => 'suppress all messages',
    'verbose|v+' => 'display extra messages',
    'debug' => 'include information only interesting to developers',
    'format|f=s' => 'specify format of output: robot, text',
);


class Data::Dedup::Files::CLI {
    has $!CLI;
    has $!dedup = Data::Dedup::Files->new;

    has $!stdout is rw = *STDOUT;
    has $!stderr is rw = *STDERR;

    method BUILD($args) {
        $!CLI = CLI::Startup->new({
            usage => '--dir /path/to/scan [options...]',
            options => \%options,
        });
    }

    # Removes " at FILE line ##" and everything after it.
    # This message may appear on the same line as a warning message,
    # or it may appear on a line by itself (e.g., with Carp).
    sub _remove_source_loc {
        my ($msg) = @_;
        return @_ if ref $msg;
        my @lines;
        for my $line ($msg =~ m/^.*$/gm) {
            if ($line =~ s/ at .+? line \d+.*$//) {
                push @lines, $line if $line;
                last;
            } else {
                push @lines, $line;
            }
        }
        return map "$_\n", @lines;
    }

    method run {
        $!CLI->init;
        my $opts = $!CLI->get_options;

        my $syswarn = $SIG{__WARN__} || sub { warn @_ };
        local $SIG{__WARN__}
            = $opts->{quiet} ? sub { } # quiet = suppress all
            : $opts->{debug} ? sub { $syswarn->(@_) } # debug = let all through
            : sub { $syswarn->(_remove_source_loc @_) };

        $!dedup->scan( dir => $_ ) for @{$opts->{dir}};

        my $file_list = $!dedup->duplicates(
            resolve_hardlinks => sub { ( sort { $a cmp $b } @{$_[0]} )[0] },
        );

        print $!stdout
            sort { $a cmp $b }
            map {
                (join "\t", sort { $a cmp $b } @$_) . "\n"
            } grep { @$_ > 1 } @$file_list;
    }
}


=head1 AUTHOR

J. Timothy King (www.JTimothyKing.com, github:JTimothyKing)

=head1 LICENSE

This software is copyright 2014 J. Timothy King.

This is free software. You may modify it and/or redistribute it under the terms of
The Apache License 2.0. (See the LICENSE file for details.)

=cut

1;
