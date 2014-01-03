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
    has $!ignore_empty is rw;
    has $!progress is rw;

    has $!engine;

    method BUILD {
        $!engine = Dedup::Engine->new(
            blocking => [
                sub { -s $_[0] },   # first blocking key: filesize

                sub {               # second blocking key: data sample from first cluster
                    my $file = shift;
                    my ($size,$blksize) = (lstat $file)[7,11];
                    my $cluster_size = min $size, ($blksize || 4096);
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
        my $ignore_empty = $args{ignore_empty} // $!ignore_empty;

        File::Find::find({
            no_chdir => 1,
            wanted => sub {
                return unless -f && !-l && (!$ignore_empty || -s > 0);

                warn("cannot read file $_\n"), return unless -r;

                return if 1 < push @{ $!inodes_seen->{ (lstat)[1] } }, $_;

                my $filesize = -s;  # while it's fresh in memory

                $!engine->add( $_ );

                $progress->($filesize) if $progress;
            },
        }, $dir);
    }


    method duplicates(%args) {
        my $resolve_hardlinks = $args{resolve_hardlinks};

        my @file_list = map { $_->objects } @{$!engine->blocks};

        if ($resolve_hardlinks) {
            my %hardlinks = map {
                my $files = $_;
                @$files > 1 ? map { $_ => $files } @$files : ();
            } @{ $self->hardlinks };

            for my $files (@file_list) {
                for my $file (@$files) {
                    # !!! permanently changes the $file stored in $!engine->blocks
                    $file = $resolve_hardlinks->($hardlinks{$file})
                        if exists $hardlinks{$file};
                }
            }
        }

        return \@file_list;
    }


    method hardlinks {
        [ values %{$!inodes_seen} ];
    }
}

1;
