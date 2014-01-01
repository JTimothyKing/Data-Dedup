package Dedup::Files::_guts;
use strict;
use warnings;
use feature 'state';
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
    has $!verbose is rw;

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


    sub print_files_count($numfiles) {
        state $files_count = 0;
        state $next_min_files_to_print = 0;
        $files_count += $numfiles;
        if ($files_count >= $next_min_files_to_print) {
            print STDERR "\rscanned $files_count files";
            $next_min_files_to_print = (int($files_count / 100) + 1) * 100;
        }
    }

    method scan {
        my %seen; # inode numbers that we've seen before
        File::Find::find({
            no_chdir => 1,
            wanted => sub {
                return unless -f && !-l && -s > 0;
                return if $seen{ File::stat::lstat($_)->ino }++;
                $!engine->add( $_ );
                print_files_count 1 if $!verbose;
            },
        }, $!dir);
        print STDERR "\r" if $!verbose;
    }


    method duplicates {
        [ map { $_->{objects} } @{$!engine->blocks} ];
    }
}

1;
