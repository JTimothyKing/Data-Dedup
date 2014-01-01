package Dedup::Engine::_guts;
use 5.016;
use strict;
use warnings;
use mop;
use signatures;

use List::MoreUtils ();

# core modules
use Scalar::Util ();

=head1 NAME

Dedup::Engine - A general-purpose deduplication engine

=head1 SYNOPSIS

    my $engine = Dedup::Engine->new(
        blocking => [
            sub { -s $_[0] },   # first blocking key: filesize
            sub {               # second blocking key: SHA-1
                Digest::SHA->new(1)
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
        print @$files > 1 ? 'duplicates' : 'unique',
              ": filesize $filesize, sha1 $sha1\n",
              (map "  $_\n", @$files);
    }

=head1 DESCRIPTION

=head1 NOTES

  * Does not yet support deduplicating based on direct comparisons of multiple objects.

  * Does not yet support blocking algorithms that put the same object in multiple blocks.

  * Does not yet support fuzzy comparison algorithms (that depend on how closely two
    objects match, rather than a simple yea or nay).

=cut

class Dedup::Engine {
    has $!blocking is ro; # a sub or array of subs

    sub _is_code($r) { eval { ref $r && \&{$r} } }
    sub _is_array($r) { eval { ref $r && \@{$r} } }

    method BUILD {
        $!blocking = [ ] unless defined $!blocking;
        $!blocking = [ $!blocking ] if _is_code($!blocking);
        die "blocking attribute must be a coderef or an arrayref of coderefs"
            unless _is_array($!blocking) && List::MoreUtils::all { _is_code($_) } @{$!blocking};
    }


    class Dedup::Engine::Block is repr('HASH') {
        has $!keys = [];
        has $!objects = [];

        method _sync_repr {
            $self->{keys} = $!keys;
            $self->{objects} = $!objects;
        }

        method BUILD { $self->_sync_repr }

        method keys { $!keys }
        method add_keys(@keys) { push @{$!keys}, @keys; $self->_sync_repr; $!keys }
        method key($idx) { $!keys->[$idx] }

        method objects { $!objects }
        method add_objects(@objects) { push @{$!objects}, @objects; $self->_sync_repr; $!objects }
        method object($idx) { $!objects->[$idx] }
    }

    has $!_blocks = []; # an array of Block objects


    class Dedup::Engine::BlockKeyStore {
        has $!_keyhash = {};

        method set($key, $content) { $!_keyhash->{$key} = $content }

        # Returns a reference to the key slot within the key store
        method get_ref($key) { \($!_keyhash->{$key}) }
    }

    has $!_blocks_by_key; # may contain a BlockKeyStore or Block


    sub _block_to_keystore($block, $blockingsub) {
        # Only called on blocks without a key at this level,
        # because blocks with a key would be stored in a keystore at this level.

        my $key = $blockingsub->(
            $block->object(0), # Blocks without keys can only have one object.
        );

        $block->add_keys($key); # Now the block is no longer without a key.

        my $keystore = Dedup::Engine::BlockKeyStore->new;
        $keystore->set($key => $block);
        return $keystore;
    }

    # Returns a reference to the new block created for the object, if a new block was created.
    sub _block($object, $blockingsubs, $rblockslot, $keys) {
        $keys //= [];

        my $blockslot_isa = sub ($class) {
            Scalar::Util::blessed($$rblockslot) && $$rblockslot->isa($class)
        };

        if (@$blockingsubs) {
            my ($blockingsub, @other_blocking_subs) = @$blockingsubs;

            if ($blockslot_isa->('Dedup::Engine::Block')) {
                # Found a block that hasn't been keyed at this level;
                # push it down the hierarchy into a keystore.
                $$rblockslot = _block_to_keystore($$rblockslot, $blockingsub);
            }

            if ($blockslot_isa->('Dedup::Engine::BlockKeyStore')) {
                # File the current object in the appropriate slot in the keystore.
                my $key = $blockingsub->($object);
                push @$keys, $key;
                return _block( $object, \@other_blocking_subs,
                    $$rblockslot->get_ref($key), # creates a new sub-slot if needed
                    $keys );
            }

        } elsif ($$rblockslot) {
            # No more blocking subs at this level means this is a block (not a keystore);
            # therefore, we've found the block in which to file the object.
            $$rblockslot->add_objects($object);
        }

        if (! $$rblockslot) {
            # This is the first object keyed to this level with this key sequence.
            $$rblockslot = Dedup::Engine::Block->new(
                keys => [@$keys],
                objects => [$object],
            );
            return $$rblockslot;
        }

        return; # no new block was created (or else we would have returned it earlier)
    }


    method add(@objects) {
        for my $object (@objects) {
            push @{$!_blocks}, _block( $object, $!blocking, \($!_blocks_by_key) );
        }
    }

    method blocks { $!_blocks }
}

1;
