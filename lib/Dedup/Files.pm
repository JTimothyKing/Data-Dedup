package Dedup::Files::_guts; ## no critic (RequireFilenameMatchesPackage)
use 5.016;
use strict;
use warnings;
use mop;
use signatures;

## no critic (ProhibitSubroutinePrototypes)
#   ...because of signatures

use Dedup::Engine;
use Dedup::Files::DigestFactory;

# core modules
use File::Find ();
use Scalar::Util 'blessed';

=head1 NAME

Dedup::Files - Detect duplicate files using Dedup::Engine

=head1 SYNOPSIS

    my $dedup = Dedup::Files->new(
        dir => '/path/to/directory/structure/to/dedup',
    );

    $dedup->scan();

    my $file_list = $dedup->duplicates;
    for my $files (@$file_list) {
        print @$files > 1 ? 'duplicates' : 'unique', "\n",
              (map "  $_\n", @$files);
    }

Or...

    my $dedup = Dedup::Files->new;

    $dedup->scan( dir => '/a/path/to/dedup' );
    $dedup->scan( dir => '/another/path' );
    $dedup->scan( dir => '/yet/another/path' );

    my $file_list = $dedup->duplicates;

=head1 DESCRIPTION

This module 

=cut


class Dedup::Files {
    has $!blocking is ro;

    has $!dir is rw;
    has $!ignore_empty is rw;
    has $!progress is rw;

    has $!engine;

    method BUILD {
        $!blocking //= Dedup::Files::DigestFactory->new;
        $!engine = Dedup::Engine->new( blocking => $!blocking );
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


=head1 FEATURES THAT WOULD BE NICE TO ADD

=over

=item *

Select different digest algorithms to use during scan.

=item *

Multiple directories, and specify explicit list of files.

=item *

An option to report hardlinks as dups (maybe via resolve_hardlinks).

=back


=head1 AUTHOR

J. Timothy King (www.JTimothyKing.com, github:JTimothyKing)

=head1 LICENSE

This software is copyright 2014 J. Timothy King.

This is free software. You may modify it and/or redistribute it under the terms of
The Apache License 2.0. (See the LICENSE file for details.)

=cut

1;
