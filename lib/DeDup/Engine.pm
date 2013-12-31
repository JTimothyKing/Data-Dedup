package DeDup::Engine::_GUTS;
use strict;
use warnings;
use mop;
use signatures;

use Scalar::Util ();
use List::MoreUtils ();

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
    has $!blocking is ro; # a sub or array of subs

    sub _is_code($r) { eval { ref $r && \&{$r} } }
    sub _is_array($r) { eval { ref $r && \@{$r} } }

    method BUILD {
        $!blocking = [ ] unless defined $!blocking;
        $!blocking = [ $!blocking ] if _is_code($!blocking);
        die "blocking attribute must be a coderef or an arrayref of coderefs"
            unless _is_array($!blocking) && List::MoreUtils::all { _is_code($_) } @{$!blocking};
    }


    class DeDup::Engine::Block is repr('HASH') {
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


    class DeDup::Engine::BlockKeyStore {
        has $!_keyhash = {};

        method set($key, $content) { $!_keyhash->{$key} = $content }

        # Returns a reference to the key slot within the key store
        method get_ref($key) { \($!_keyhash->{$key}) }
    }

    has $!_blocks_by_key; # may contain a BlockKeyStore or Block


    sub _block($object, $blockingsubs, $rkeystore, $keys) {
        $keys //= [];

        my $keystore_isa = sub ($class) {
            Scalar::Util::blessed($$rkeystore) && $$rkeystore->isa($class)
        };

        my $r_block; # the block slot into which to place the object

        my @other_blocking_subs;
        if (@$blockingsubs) {
            my $blocking_sub;
            ($blocking_sub, @other_blocking_subs) = @$blockingsubs;

            if ($keystore_isa->('DeDup::Engine::Block')) {
                # Key this previous block without key
                my $block = $$rkeystore;

                my $key = $blocking_sub->(
                    $block->object(0), # blocks without keys can only have one object
                );

                $block->add_keys($key); # no longer without a key

                $$rkeystore = DeDup::Engine::BlockKeyStore->new;
                $$rkeystore->set($key => $block);
            }


            if ($keystore_isa->('DeDup::Engine::BlockKeyStore')) {
                # Find an existing block, if one exists
                my $key = $blocking_sub->($object);
                push @$keys, $key;
                $r_block = $$rkeystore->get_ref($key); # creates a new slot if needed

            } else {
                # Still must add the very first block
                $r_block = $rkeystore;
            }

        } else {
            # Just add to or create the current block.
            $r_block = $rkeystore;
        }


        my $new_block;

        if ($$r_block) {
            # Create a sub-block, if possible.
            $new_block = _block($object, \@other_blocking_subs, $r_block, $keys)
                if @other_blocking_subs;

            $$r_block->add_objects($object) unless $new_block;

        } else {
            $new_block = DeDup::Engine::Block->new(
                keys => [@$keys],
                objects => [],
            );
            $new_block->add_objects($object);
            $$r_block =  $new_block;
        }

        return $new_block;
    }

    method add(@objects) {
        for my $object (@objects) {
            my $rkeystore = \($!_blocks_by_key);

            push @{$!_blocks},
                (_block($object, $!blocking, $rkeystore) // ());
        }
    }

    method blocks { $!_blocks }
}

1;
