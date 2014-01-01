package Dedup::Files::_guts;
use 5.016;
use strict;
use warnings;
use mop;
use signatures;

use Dedup::Engine;

use Digest::SHA;

# core modules
use File::Find ();
use File::stat ();
use List::Util 'min', 'max';

=head1 NAME

Dedup::Files - Detect duplicate files using Dedup::Engine

=head1 SYNOPSIS

    my $dedup = Dedup::Files->new(
        dir => '/path/to/directory/structure/to/dedup',
    );

    $dedup->scan();
    $dedup->compare();

    my $file_list = $dedup->duplicates;
    for my $files (@$file_list) {
        print @$files > 1 ? 'duplicates' : 'unique', "\n",
              (map "  $_\n", @$files);
    }

=head1 DESCRIPTION

=head1 POSSIBLE FUTURE FEATURES

  * Select different digest algorithms to use during scan.

  * Multiple directories, and specify explicit list of files.

=cut

class Dedup::Files {
    has $!dir is rw;
    has $!progress is rw;

    has $!engine;

    method BUILD {
        $!engine = Dedup::Engine->new(
            blocking => [
                sub { -s $_[0] },   # first blocking key: filesize

                sub {               # second blocking key: data sample from first cluster
                    my $file = shift;
                    my $st = File::stat::lstat $file;
                    my $cluster_size = min $st->size, ($st->blksize || 4096);
                    my $offset = max 0, ($cluster_size/2 - 128);
                    open my $fd, '<', $file or die "cannot read from $file";
                    my $data;
                    read $fd, $data, 128, $offset;
                    close $fd;
                    return $data;
                },

                sub {               # third blocking key: SHA-1
                    Digest::SHA->new(1)
                        ->addfile($_[0])
                        ->digest;
                },
            ],
        );
    }

    has $!inodes_seen = {};

    method scan(%args) {
        my $dir = $args{dir} // $!dir;
        my $progress = $args{progress} // $!progress;

        File::Find::find({
            no_chdir => 1,
            wanted => sub {
                return unless -f && !-l && -s > 0;
                return if $!inodes_seen->{ File::stat::lstat($_)->ino }++;

                my $filesize = -s;  # while it's fresh in memory

                $!engine->add( $_ );

                $progress->($filesize) if $progress;
            },
        }, $dir);
    }


    method duplicates {
        [ map { $_->{objects} } @{$!engine->blocks} ];
    }
}

1;
