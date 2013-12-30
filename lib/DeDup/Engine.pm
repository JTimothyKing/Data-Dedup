use strict;
use warnings;
use mop;

=head1 NAME

DeDup::Engine - A general-purpose deduplication engine

=head1 SYNOPSIS

    my $engine = DeDup::Engine->new(
        blocking => [
            sub { -s $_[0] },   # first blocking key: filesize
            sub {               # second blocking key: sha1
                Digest::SHA::new('sha1')
                    ->addfile($_[0])
                    ->hexdigest;
            },
        ],
    );

    $engine->add(@files);
    $engine->add($another_file);
    $engine->add(@even_more_files);

    my $blocks = $engine->blocks;
    for my $block (@$blocks) {
        my ($keys, $files) = @$block{'keys', 'objects'};
        my ($filesize, $sha1) = @$keys;
        print (@$files > 1) ? 'duplicates' : 'unique',
              ": filesize $filesize, sha1 $sha1\n",
              (map "  $_\n", @$files);
    }

=cut

class DeDup::Engine {
    has $!blocking is ro;

    has $!_blocks = [];
    has $!_blocks_by_key = {};
    has $!_block_without_key;

    method add(@objects) {
        for my $object (@objects) {
            my ($blockref, @keys);

            my $blocking_sub = ref $!blocking eq 'CODE' ? $!blocking
                : ref $!blocking eq 'ARRAY' ? $!blocking->[0]
                : die("Invalid blocking sub");

            my $bwkref = \($!_block_without_key);
            my $keyhash = $!_blocks_by_key;

            # Key any previous block without key
            if ($$bwkref) {
                my $key = $blocking_sub->(
                    $$bwkref->{objects}[0], # only one object, by definition
                );
                push @{$$bwkref->{keys}}, $key; # no longer without key
                $keyhash->{$key} = $$bwkref;
                $$bwkref = undef;
            }

            # Find an existing block, if one exists
            if (%$keyhash) {
                my $key = $blocking_sub->($object);
                push @keys, $key;
                $blockref = \($keyhash->{$key}); # adds it if doesn't already exist
            } else {
                $blockref = $bwkref;
            }

            if (! $$blockref) {
                $$blockref = {
                    keys => [@keys],
                    objects => [],
                };
                push @{$!_blocks}, $$blockref;
            }

            push @{$$blockref->{objects}}, $object;
        }
    }

    method blocks { $!_blocks }
}

1;
