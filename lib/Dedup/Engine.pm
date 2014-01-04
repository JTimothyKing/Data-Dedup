package Dedup::Engine::_guts;
use 5.016;
use strict;
use warnings;
use mop;
use signatures;

use List::MoreUtils ();

# core modules
use Carp;
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
        my $keys = $block->keys;
        my $files = $block->objects;
        my ($filesize, $sha1) = @$keys;
        print @$files > 1 ? 'duplicates' : 'unique',
              ": filesize $filesize, sha1 $sha1\n",
              (map "  $_\n", @$files);
    }

=head1 DESCRIPTION

This module implements a general-purpose deduplication engine, which uses one or
more digest algorithms to detect differences between a number of objects.

By steps, Dedup::Engine groups the objects together in blocks, by computing
digests on the objects. Two objects that each produce a different value for the
same digest are confirmed as distinct.

Some digest algorithms are very fast, but they tend to result in collisions.
That is, two distinct objects may produce the same digest. So Dedup::Engine can
try a sequence of digest algorithms, of increasing selectivity but decreasing
speed, in an attempt to further distinguish objects that it thinks might be
duplicates.

After it has tried all available digest algorithms, any objects for which the
engine has been unable to detect differences, they are listed in the same block,
as duplicates of each other.

=cut


class Dedup::Engine {

=head1 CONSTRUCTION

=over

=item new

Instantiate a new deduplication engine with the given configuration. The configuration
may be passed into C<new> as a list of keys and values.

    my $engine = Dedup::Engine->new( blocking => \&my_digest_sub );

Alternatively, C<new> will accept a hash reference:

    my %config;
    build_config( \%config );
    my $engine = Dedup::Engine->new( \%config );

=back

=head2 Configuration

C<< Dedup::Engine->new >> accepts the following configuration keys:

=over

=item blocking

One or more digest functions (i.e., blocking key functions) by which the engine
will attempt to arrange objects into blocks.

This value can be a single value or an array of values. Each value may be a
function, or an object that implements L<Dedup::Engine::BlockingFactory>. Each
such blocking factory will be expanded in-place into a list of functions, by
calling its L<all_functions
method|Dedup::Engine::BlockingFactory/all_functions>. After all such expansions,
the resulting list of functions will be used in order of preference to
deduplicate objects.

(Note that this may also be any object that can be called like a sub or
dereferenced like an array. If C<blocking> is an object that can do both, its
array behavior will dominate. And if a blocking-factory object can be called
like a sub, its blocking-factory behavior will dominate.)

Each function in C<blocking> takes an object as its argument, and returns a
scalar digest. The return value is usually a string (and may be stringified for
use as a hash key) but could conceptually be anything that can fit into a
scalar, including a reference to an opaque object.

To deduplicate a series of files by name, one might use:

    my $engine = Dedup::Engine->new(
        blocking => [
            sub { -s $_[0] },   # first blocking key: filesize
            sub {               # second blocking key: SHA-1
                Digest::SHA->new(1)
                    ->addfile($_[0])
                    ->digest;
            },
        ],
    );

In this example, the "objects" being deduplicated are mere filenames, as
strings. The blocking functions contain all the intelligence necessary to make
sense of them, and Dedup::Engine is agnostic regarding their contents. (See the
L<add method|Dedup::Engine/add> for more about how the engine handles objects.)

If no C<blocking> parameter is specified, or if it's empty, then as a degenerate
case, all objects will be considered duplicates of each other.

=cut

    has $!blocking is ro;

=back

The value of any configuration key can be retrieved from an instantiated engine
object by calling the accessor method named after it:

    my $blocking = $engine->blocking; # returns the list of blocking functions

=cut

    sub _is_code($r) { eval { ref $r && \&{$r} } }
    sub _is_array($r) { eval { ref $r && \@{$r} } }

    sub _is_blocking_factory($ref) {
        my $class = Scalar::Util::blessed($ref) or return;
        my $meta = mop::meta($class) or return;
        return $meta->does_role('Dedup::Engine::BlockingFactory');
    }

    method BUILD {
        $!blocking //= [];
        $!blocking = [ $!blocking ] unless _is_array($!blocking);

        $!blocking = do {
            my @expanded_blocking;

            for (my $idx_blocking = 0; $idx_blocking < @{$!blocking}; $idx_blocking++) {
                my $blocking_elem = $!blocking->[$idx_blocking];

                if (_is_blocking_factory($blocking_elem)) {
                    my $factory = $blocking_elem;
                    my $all_functions = $factory->all_functions;

                    croak "($factory)->all_functions must return an arrayref;"
                        . " returned a value of type "
                        . (ref($all_functions) || '(not a reference)')
                        unless _is_array($all_functions);

                    for (my $idx_fns = 0; $idx_fns < @$all_functions; $idx_fns++) {
                        my $fn = $all_functions->[$idx_fns];
                        croak "($factory)->all_functions must return an array of coderefs;"
                            . " the value at index $idx_fns was of type "
                            . (ref($fn) || '(not a reference)')
                            unless _is_code($fn);
                    }

                    push @expanded_blocking, @$all_functions;

                } elsif (_is_code($blocking_elem)) {
                    push @expanded_blocking, $blocking_elem;

                } else {
                    croak "blocking attribute must contain only blocking factories and coderefs;"
                        . " the value at index $idx_blocking was of type "
                        . (ref($blocking_elem) || '(not a reference)');
                }
            }

            \@expanded_blocking;
        };

        croak "blocking attribute must be a blocking factory, a coderef,"
            . " or an arrayref of blocking factories and coderefs"
            unless _is_array($!blocking) && List::MoreUtils::all { _is_code($_) } @{$!blocking};
    }


=head1 BLOCKS

The deduplicator engine arranges objects into blocks. Each block contains only
objects that the engine believes are duplicates of each other. It returns these
blocks (via the L<blocks method|Dedup::Engine/blocks> as instances of
Dedup::Engine::Block, an opaque class with the following accessors:

=over

=cut

    class Dedup::Engine::Block {
        has $!keys = [];
        has $!objects = [];

        method _add_keys(@keys) { push @{$!keys}, @keys }
        method _add_objects(@objects) { push @{$!objects}, @objects }


=item keys

Returns the blocking keys that distinguish this block.

    my $keys = $block->keys;
    print "digest values for this block: ", join(', ', @$keys);

This method returns an arrayref, which contains the values that were returned by
each of the blocking functions for the objects in this block. The values are in
the same order as the corresponding blocking functions (passed to
L<< Dedup::Engine->new|Dedup::Engine/new >>). However, the array of values may be
shorter than the list of blocking functions, if one or more of the later
blocking functions was not needed to distinguish this block. (In this case, the
block will only contain one object, because otherwise the engine would have used
further blocking functions to attempt to distinguish the multiple objects.)

Each block has a unique set of key values, distinguishing it from all other
blocks, and distinguishing the (duplicate) objects in each block from the
objects in all other blocks.

Note that, for efficiency, the arrayref returned by this method points into
Dedup::Engine's internal data structures. If you modify any of the contents of
this array, it will modify the engine's perception of the universe. Please don't
do this. If you want to modify the data returned by this method, please modify a
copy, rather than the original.

=cut

        method keys { $!keys }

=item num_keys

Returns the number of keys in this block. C<< $block->num_keys >> is equivalent
to C<< scalar(@{$block->keys}) >>. (See L<keys>.)

=cut

        method num_keys { scalar( @{ $!keys } ) };

=item key

Returns a blocking key by index. C<< $block->key($idx) >> is equivalent to C<<
$block->keys->[$idx] >>. (See L<keys>.)

=cut

        method key($idx) { $!keys->[$idx] }


=item objects

Returns the objects in this block.

    my $objects = $block->objects;
    print "The following objects are duplicates of one another:\n",
        map "  $_\n", @$objects;

This method returns an arrayref, which contains the objects that were passed
into L<< $engine->add|Dedup::Engine/add >>. All the objects in a single block are
being reported as duplicates of each other. The block may contain only one
object, in which case that object is unique (not a duplicate of any other object
that the deduplication engine has seen).

Note that, for efficiency, the arrayref returned by this method points into
Dedup::Engine's internal data structures. If you modify any of the contents of
this array, it will modify the engine's perception of the universe. Please don't
do this. If you want to modify the data returned by this method, please modify a
copy, rather than the original.

=cut

        method objects { $!objects }

=item num_objects

Returns the number of objects in this block. C<< $block->num_objects >> is
equivalent to C<< scalar(@{$block->objects}) >>. (See
L<objects>.)

=cut

        method num_objects { scalar( @{ $!objects } ) };

=item object

Returns an object by index. C<< $block->object($idx) >> is equivalent to C<<
$block->objects->[$idx] >>. (See L<objects>.)

=cut

        method object($idx) { $!objects->[$idx] }
    }

=back

=cut

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

        $block->_add_keys($key); # Now the block is no longer without a key.

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
            $$rblockslot->_add_objects($object);
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


=head1 METHODS

=over

=item add

Process one or more objects through the deduplication engine.

    $engine->add($an_object);
    $engine->add($another_object, $yet_another_object);
    $engine->add(@even_more_objects);



=cut

    method add(@objects) {
        for my $object (@objects) {
            push @{$!_blocks}, _block( $object, $!blocking, \($!_blocks_by_key) );
        }
    }


=item blocks

Returns all the objects seen, arranged into blocks by distinctiveness.

    my $blocks = $engine->blocks;
    print "Seen a block with ", $_->num_objects, " objects\n"
        for @$blocks;

This method returns an arrayref of block objects. (See L<BLOCKS>.) Each block
contains one or more duplicate objects, along with the blocking keys that
identify them as distinct from objects in all the other blocks.

Note that, for efficiency, the arrayref returned by this method points into
Dedup::Engine's internal data structures. If you modify any of the contents of
this array, it will modify the engine's perception of the universe. Please don't
do this. If you want to modify the data returned by this method, please modify a
copy, rather than the original.

=cut

    method blocks { $!_blocks }

=back

=cut

}


=head1 SEE ALSO

L<Dedup::Theory> for a discussion of the theory behind this module.


=head1 FEATURES THAT WOULD BE NICE TO ADD

=over

=item *

A method that would deeply compare multiple objects in each block, to
conclusively dedup them.

=item *

Support fuzzy comparison algorithms (that depend on how closely two objects
match, rather than a simple yea or nay).

=item *

Support for blocking algorithms that put the same object in multiple blocks.

=item *

Support for dynamic blocking algorithms (e.g., a sliding window) that generate a
large number of (overlapping) blocks.

=item *

Dynamic selection of digest algorithms based on expected cost and effectiveness
for each object.

=item *

Support for threaded execution to improve performance of CPU-heavy object sets.

=back


=head1 AUTHOR

J. Timothy King (www.JTimothyKing.com, github:JTimothyKing)

=head1 LICENSE

This software is copyright 2014 J. Timothy King.

This is free software. You may modify it and/or redistribute it under the terms of
The MIT License. (See the LICENSE file for details.)

=cut

1;
