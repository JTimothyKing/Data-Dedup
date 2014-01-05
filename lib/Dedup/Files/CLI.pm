package Dedup::Files::CLI::_guts; ## no critic (RequireFilenameMatchesPackage)
use 5.016;
use strict;
use warnings;
use mop;
use signatures;

## no critic (ProhibitSubroutinePrototypes)
#   ...because of signatures

use CLI::Startup;
use Dedup::Files;


my %options = (
    'dir|d=s@' => 'directory under which to scan',
    'quiet|q' => 'suppress all messages',
    'format|f=s' => 'specify format of output: robot, text',
);


class Dedup::Files::CLI {
    has $!CLI;
    has $!dedup = Dedup::Files->new;

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


1;
