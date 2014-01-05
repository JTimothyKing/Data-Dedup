package Data::Dedup::Files::CLI::_guts; ## no critic (RequireFilenameMatchesPackage)
use 5.016;
use strict;
use warnings;
use mop;
use signatures;

## no critic (ProhibitSubroutinePrototypes)
#   ...because of signatures

use CLI::Startup;
use Data::Dedup::Files;


my %options = (
    'dir|d=s@' => 'directory under which to scan',
    'quiet|q' => 'suppress all messages',
    'format|f=s' => 'specify format of output: robot, text',
);


class Data::Dedup::Files::CLI {
    has $!CLI;
    has $!dedup = Data::Dedup::Files->new;

    has $!stdout is rw = \*STDOUT;
    has $!stderr is rw = \*STDERR;

    method BUILD($args) {
        $!CLI = CLI::Startup->new({
            options => \%options,
        });
    }

    method run {
        $!CLI->init;
        my $opts = $!CLI->get_options;

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
