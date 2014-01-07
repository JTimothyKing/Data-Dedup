package Data::Dedup::Files::DigestFactory::_guts;
use 5.016;
use strict;
use warnings;
use mop 0.03;
use signatures 0.07;

use Digest::SHA 5.82;

use Data::Dedup::Engine::BlockingFactory;
use Data::Dedup::Engine::BlockingFunction;

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

    has $!from_filesize is ro = Data::Dedup::Engine::BlockingFunction->new(
        class => __CLASS__,
        id => 'filesize',
        name => 'filesize',
        impl => sub ($file) { -s $file },
    );


    sub _retrieve_sample($file, $offset, $size) {
        my $data;
        open my $fd, '<', $file or die "cannot read from $file";
        seek $fd, $offset, 0;
        read $fd, $data, $size;
        close $fd;
        return $data;
    }

    has $!from_sample is ro = Data::Dedup::Engine::BlockingFunction->new(
        class => __CLASS__,
        id => 'sample',
        name => 'first-cluster sample',
        impl => sub ($file) {
            my ($size,$blksize) = (lstat $file)[7,11];
            my $cluster_size = min $size, ($blksize || 4096);
            return '' unless $cluster_size > 0;
            my $offset = max 0, ($cluster_size/2 - 128);
            return _retrieve_sample($file, $offset, 128);
        },
    );

    has $!from_end_sample is ro = Data::Dedup::Engine::BlockingFunction->new(
        class => __CLASS__,
        id => 'end_sample',
        name => 'last-cluster sample',
        impl => sub ($file) {
            my ($size,$blksize) = (lstat $file)[7,11];
            my $cluster_size = min $size, ($blksize || 4096);
            return '' unless $cluster_size > 0;
            my $last_cluster_offset = int( ($size-1) / $cluster_size ) * $cluster_size;
            my $last_cluster_size = $size - $last_cluster_offset;
            if ($last_cluster_size < 128) {
                $last_cluster_offset -= $cluster_size;
                $last_cluster_size = $cluster_size;
            }
            my $offset = max 0, ($last_cluster_offset + $last_cluster_size/2 - 128);
            return _retrieve_sample($file, $offset, 128);
        },
    );

    has $!from_mid_sample is ro = Data::Dedup::Engine::BlockingFunction->new(
        class => __CLASS__,
        id => 'mid_sample',
        name => 'mid-file sample',
        impl => sub ($file) {
            my ($size,$blksize) = (lstat $file)[7,11];
            my $cluster_size = min $size, ($blksize || 4096);
            return '' unless $cluster_size > 0;
            my $mid_cluster_offset = int( ($size/2 - 1) / $cluster_size ) * $cluster_size;
            my $offset = max 0, ($mid_cluster_offset + $cluster_size/2 - 128);
            return _retrieve_sample($file, $offset, 128);
        },
    );


    has $!from_initial_sha is ro = Data::Dedup::Engine::BlockingFunction->new(
        class => __CLASS__,
        id => 'initial_sha',
        name => 'first-cluster SHA-1',
        impl => sub ($file) {
            my ($size,$blksize) = (lstat $file)[7,11];
            my $cluster_size = min $size, ($blksize || 4096);
            Digest::SHA->new(1)
                ->add( _retrieve_sample($file, 0, $cluster_size) )
                ->digest;
        },
    );


    has $!from_sha is ro = Data::Dedup::Engine::BlockingFunction->new(
        class => __CLASS__,
        id => 'sha',
        name => 'SHA-1',
        impl => sub ($file) {
            Digest::SHA->new(1)
                ->addfile($file)
                ->digest;
        },
    );
}


=head1 AUTHOR

J. Timothy King (www.JTimothyKing.com, github:JTimothyKing)

=head1 LICENSE

This software is copyright 2014 J. Timothy King.

This is free software. You may modify it and/or redistribute it under the terms of
The Apache License 2.0. (See the LICENSE file for details.)

=cut

1;
