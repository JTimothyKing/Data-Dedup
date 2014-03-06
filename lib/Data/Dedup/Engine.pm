package Data::Dedup::Engine;
# VERSION: dist tool inserts version here

package Data::Dedup::Engine::_guts;
use 5.019_009;
use strict;
use warnings;
use feature 'signatures';
no warnings 'experimental::signatures';
use mop 0.03;

use List::MoreUtils 0.33 ();

# core modules
use Carp;
use Scalar::Util ();

=head1 NAME

Data::Dedup::Engine - A general-purpose deduplication engine

=head1 SYNOPSIS

    my $engine = Data::Dedup::Engine->new(
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

By steps, Data::Dedup::Engine groups the objects together in blocks, by computing
digests on the objects. Two objects that each produce a different value for the
same digest are confirmed as distinct.

Some digest algorithms are very fast, but they tend to result in collisions.
That is, two distinct objects may produce the same digest. So Data::Dedup::Engine can
try a sequence of digest algorithms, of increasing selectivity but decreasing
speed, in an attempt to further distinguish objects that it thinks might be
duplicates.

After it has tried all available digest algorithms, any objects for which the
engine has been unable to detect differences, they are listed in the same block,
as duplicates of each other.

=cut


class Data::Dedup::Engine {

=head1 CONSTRUCTION

=over

=item new

Instantiate a new deduplication engine with the given configuration. The configuration
may be passed into C<new> as a list of keys and values.

    my $engine = Data::Dedup::Engine->new( blocking => \&my_digest_sub );

Alternatively, C<new> will accept a hash reference:

    my %config;
    build_config( \%config );
    my $engine = Data::Dedup::Engine->new( \%config );

=back

=head2 Configuration

C<< Data::Dedup::Engine->new >> accepts the following configuration keys:

=over

=item blocking

One or more digest functions (i.e., blocking key functions) by which the engine
will attempt to arrange objects into blocks.

This value can be a single value or an array of values. Each value may be a
function, or an object that implements L<Data::Dedup::Engine::BlockingFactory>. Each
such blocking factory will be expanded in-place into a list of functions, by
calling its L<all_functions
method|Data::Dedup::Engine::BlockingFactory/all_functions>. After all such expansions,
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

    my $engine = Data::Dedup::Engine->new(
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
sense of them, and Data::Dedup::Engine is agnostic regarding their contents. (See the
L<add method|Data::Dedup::Engine/add> for more about how the engine handles objects.)

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
        return $meta->does_role('Data::Dedup::Engine::BlockingFactory');
    }

    sub _object_isa($obj, $class) {
        Scalar::Util::blessed $obj && $obj->isa($class)
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
                    croak "blocking attribute must contain only blocking factories and"
                        . " coderefs; the value at index $idx_blocking was of type "
                        . (ref($blocking_elem) || '(not a reference)');
                }
            }

            \@expanded_blocking;
        };

        croak "blocking attribute must be a blocking factory, a coderef,"
            . " or an arrayref of blocking factories and coderefs"
            unless _is_array($!blocking)
                && List::MoreUtils::all { _is_code($_) } @{$!blocking};
    }


=head1 BLOCKS

The deduplicator engine arranges objects into blocks. Each block contains only
objects that the engine believes are duplicates of each other. It returns these
blocks (via the L<blocks method|Data::Dedup::Engine/blocks> as instances of
Data::Dedup::Engine::Block, an opaque class with the following accessors:

=over

=cut

    class Data::Dedup::Engine::Block {
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
the same order as the corresponding blocking functions (passed to L<<
Data::Dedup::Engine->new|Data::Dedup::Engine/new >>). However, the array of
values may be shorter than the list of blocking functions, if one or more of the
later blocking functions was not needed to distinguish this block. (In this
case, the block will only contain one object, because otherwise the engine would
have used further blocking functions to attempt to distinguish the multiple
objects.)

Each block has a unique set of key values, distinguishing it from all other
blocks, and distinguishing the (duplicate) objects in each block from the
objects in all other blocks.

Note that, for efficiency, the arrayref returned by this method points into
Data::Dedup::Engine's internal data structures. If you modify any of the contents of
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
into L<< $engine->add|Data::Dedup::Engine/add >>. All the objects in a single block are
being reported as duplicates of each other. The block may contain only one
object, in which case that object is unique (not a duplicate of any other object
that the deduplication engine has seen).

Note that, for efficiency, the arrayref returned by this method points into
Data::Dedup::Engine's internal data structures. If you modify any of the contents of
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


    class Data::Dedup::Engine::BlockKeyStore {
        has $!_keyhash is ro = {};

        method set($key, $content) { $!_keyhash->{$key} = $content }

        # Returns a reference to the key slot within the key store
        method get_ref($key) { \($!_keyhash->{$key}) }

        method slots { [ values %{$!_keyhash} ] }
    }

    has $!_blocks_by_key; # may contain a BlockKeyStore or Block


    sub _block_to_keystore($block, $blockingsub) {
        # Only called on blocks without a key at this level,
        # because blocks with a key would be stored in a keystore at this level.

        my $key = $blockingsub->(
            $block->object(0), # Blocks without keys can only have one object.
        );

        $block->_add_keys($key); # Now the block is no longer without a key.

        my $keystore = Data::Dedup::Engine::BlockKeyStore->new;
        $keystore->set($key => $block);
        return $keystore;
    }

    # Returns a reference to the new block created for the object, if a new
    # block was created.
    sub _block($object, $blockingsubs, $rblockslot, $keys=undef) {
        $keys //= [];

        my $blockslot_isa = sub ($class) { _object_isa($$rblockslot, $class) };

        if (@$blockingsubs) {
            my ($blockingsub, @other_blocking_subs) = @$blockingsubs;

            if ($blockslot_isa->('Data::Dedup::Engine::Block')) {
                # Found a block that hasn't been keyed at this level;
                # push it down the hierarchy into a keystore.
                $$rblockslot = _block_to_keystore($$rblockslot, $blockingsub);
            }

            if ($blockslot_isa->('Data::Dedup::Engine::BlockKeyStore')) {
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
            $$rblockslot = Data::Dedup::Engine::Block->new(
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
Data::Dedup::Engine's internal data structures. If you modify any of the contents of
this array, it will modify the engine's perception of the universe. Please don't
do this. If you want to modify the data returned by this method, please modify a
copy, rather than the original.

=cut

    method blocks { $!_blocks }


=item count_collisions

Returns number of key collisions at each blocking level.

    my $num_key_collisions = $engine->count_collisions;
    print "Number of key collisions: ", (join ' ', @$num_key_collisions), "\n";

This method counts the number of observed key collisions for each blocking
level, i.e., corresponding with each item of L<C<blocking>|blocking>.

It returns an arrayref containing one element for each blocking level. The
returned array might contain fewer elements than C<blocking>, if all
deduplicated objects are unique, and if not all blocking algorithms were needed
to discover this fact.

A collision is defined as follows: If two distinct (non-duplicate) objects are
reachable through the same key, that's one collision. Note that each block
contains only duplicate objects, and each block is distinct from all other
blocks; therefore, if two blocks are reachable through a single key, that
represents a collision. Two blocks through one key, that's two collisions. But
three blocks that are reachable through two keys represents only one collision,
because each of the keys uniquely identify at least one block each, with only
one block colliding. So in general, the number of collisions at a given blocking
level is the number of blocks reachable through keys at that level minus the
number of keys at that level.

Objects are cumulatively blocked. That is, each successive blocking algorithm is
used only to distinguish objects that the former algorithms failed to
distinguish. So let's say a relatively strong algorithm, with few collisions, is
followed by a relatively weak one, with many collisions. The latter blocking
level will have no more observed collisions than the former, because the latter
algorithm is only used to distinguish objects that the former couldn't
distinguish.

If all blocking algorithms identify two objects as duplicates of each other,
only then will the engine consider the objects as duplicates. And if those two
objects are in fact distinct--but no blocking level detected the difference--the
engine will still consider them to be duplicates. Therefore, by definition, the
last blocking level has 0 observed collisions (because even if a block on that
level contains two distinct objects, there is no way to detect it).

=cut

    # returns ( [ collision_counts ], total_distinctive_blocks )
    sub _count_keystore_collisions($keystore) {
        my $collisions = 0;
        my @sub_collisions;
        my $total_blocks = 0;
        SLOT: for my $slot (@{$keystore->slots}) {
            if (_object_isa($slot, 'Data::Dedup::Engine::Block')) {
                $total_blocks++;
            }

            next SLOT unless _object_isa($slot, 'Data::Dedup::Engine::BlockKeyStore');

            my ($slot_collisions, $slot_blocks) = _count_keystore_collisions($slot);
            @sub_collisions = List::MoreUtils::pairwise
                { ($a // 0) + ($b // 0) } @sub_collisions, @$slot_collisions;

            # A theoretical edge condition: If a key exists in the keystore but
            # has no blocks reachable, it represents no collision, even though
            # 0 blocks minus 1 key would produce the nonsensical answer of -1.
            # In practice, this should not happen.

            $collisions += $slot_blocks > 1 ? $slot_blocks - 1 : 0;
            $total_blocks += $slot_blocks;
        }

        return( [ $collisions, @sub_collisions ], $total_blocks );
    }

    method count_collisions {
        return []  # no collisions even defined for non-keystore levels of the structure
            unless _object_isa($!_blocks_by_key, 'Data::Dedup::Engine::BlockKeyStore');

        return ( _count_keystore_collisions( $!_blocks_by_key ) )[0];
    }


=item count_keys_computed

Returns the number of times each blocking key was calculated at each blocking level.

    my $num_keys_computed = $engine->count_keys_computed;
    print "Number of times each key was computed: ",
        (join ' ', @$num_keys_computed), "\n";

This method counts the number of times each blocking key was calculated.

It returns an arrayref containing one element for each blocking level. The
returned array might contain fewer elements than C<blocking>, if all
deduplicated objects are unique, and if not all blocking algorithms were needed
to discover this fact.

Note that the number of times a blocking key was calculated is often only a
rough guide to how much time the blocking algorithm took in deduplicating the
objects, because the size or complexity of the objects themselves are often a
factor. For example, in deduplicating files, the time taken to compute a digest
of the entire contents of the file is some function of the number of bytes in
the file. On the other hand, finding the filesize of a file probably runs in a
fixed amount of time.

Objects are cumulatively blocked. That is, each successive blocking algorithm is
used only to distinguish objects that the former algorithms failed to
distinguish. Therefore, each digest function will be called at most the same
number of times as the function before it. An optimally chosen series of digest
algorithms will place the fastest, weakest ones at the front and the slowest,
strongest last. In between, each algorithm should eliminate as large as possible
a number of possible duplicates, so that by the very end, the final algorithm
runs as few times as possible.

=cut

    method count_keys_computed {
        my @num_keys;
        for my $block (@{$!_blocks}) {
            my $num_objects = $block->num_objects;
            @num_keys = List::MoreUtils::pairwise
                { ($a // 0) + (defined($b) ? $num_objects : 0) }
                @num_keys, @{$block->keys};
        }
        return \@num_keys;
    }

=back

=cut

}


=head1 SEE ALSO

L<Data::Dedup::Theory> for a discussion of the theory behind this module.


=head1 AUTHOR

J. Timothy King (www.JTimothyKing.com, github:JTimothyKing)

=head1 LICENSE

This software is copyright 2014 J. Timothy King.

This is free software. You may modify it and/or redistribute it under the terms of
The Apache License 2.0. (See the LICENSE file for details.)

=cut

1;
