package Data::Dedup::Files::DigestFactory::_guts;
use 5.016;
use strict;
use warnings;
use mop 0.03;
use signatures 0.07;

use Digest::SHA 5.82;

use Data::Dedup::Engine::BlockingFactory;

# core modules
use List::Util 'min', 'max';

=head1 NAME

Data::Dedup::Files::DigestFactory - Generate file-digest blocking functions for Data::Dedup::Files

=head1 SYNOPSIS

=head1 DESCRIPTION

=cut


class Data::Dedup::Files::DigestFactory with Data::Dedup::Engine::BlockingFactory {
    method all_functions {
        return [
            $self->from_filesize,   # first blocking key: filesize
            $self->from_sample,     # second blocking key: data sample from first cluster
            $self->from_sha,        # third blocking key: SHA-1
        ];
    }

    method from_filesize {
        return sub ($file) { -s $file };
    }

    method from_sample {
        return sub ($file) {
            my ($size,$blksize) = (lstat $file)[7,11];
            my $cluster_size = min $size, ($blksize || 4096);
            my $offset = max 0, ($cluster_size/2 - 128);
            open my $fd, '<', $file or die "cannot read from $file";
            my $data;
            read $fd, $data, 128, $offset;
            close $fd;
            return $data;
        };
    }

    method from_sha {
        return sub ($file) {
            Digest::SHA->new(1)
                ->addfile($file)
                ->digest;
        };
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
