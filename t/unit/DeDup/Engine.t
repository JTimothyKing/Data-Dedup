#!/usr/bin/env perl

package t::unit::DeDup::Engine;
use strict;
use warnings;
use Test::Most;
use parent 'Test::Class';
INIT { __PACKAGE__->runtests }

use Data::Dumper;


{ # Load module under test, and bail out if it dies.
    my $module_loaded;
    END { BAIL_OUT "Could not load module under test" unless $module_loaded }
    use DeDup::Engine;
    $module_loaded = 1;
}


sub dedup_000_default : Test(2) {
    my $engine = DeDup::Engine->new;
    ok($engine, "instantiate default DeDup::Engine");

    $engine->add();

    my $blocks = $engine->blocks;
    cmp_deeply(
        $blocks,
        bag(),
        "no blocks"
    );
}

sub dedup_001_blocking : Test(5) {
    my $engine = DeDup::Engine->new(
        blocking => sub { $_[0][0] },
    );
    ok($engine, "instantiate DeDup::Engine with blocking sub");


    my $block_A1_unkeyed = noclass({
        keys => [ ],
        objects => bag( [ A => 1 ] ),
    });
    my $block_A1 = noclass({
        keys => [ 'A' ],
        objects => bag( [ A => 1 ] ),
    });
    my $block_A1_A4 = noclass({
        keys => [ 'A' ],
        objects => bag( [ A => 1 ], [ A => 4 ] ),
    });
    my $block_B2 = noclass({
        keys => [ 'B' ],
        objects => bag( [ B => 2 ] ),
    });
    my $block_C3 = noclass({
        keys => [ 'C' ],
        objects => bag( [ C => 3 ] ),
    });



    $engine->add([ A => 1 ]);

    my $blocks = $engine->blocks;
    cmp_deeply(
        $blocks,
        bag( $block_A1_unkeyed ),
        "one object in a block"
    ) or diag( Data::Dumper->Dump([$blocks], ['blocks']) );

    $engine->add([ B => 2 ]);

    $blocks = $engine->blocks;
    cmp_deeply(
        $blocks,
        bag( $block_A1, $block_B2 ),
        "two objects in two blocks"
    ) or diag( Data::Dumper->Dump([$blocks], ['blocks']) );

    $engine->add([ A => 4 ]);

    $blocks = $engine->blocks;
    cmp_deeply(
        $blocks,
        bag( $block_A1_A4, $block_B2 ),
        "three objects in two blocks"
    ) or diag( Data::Dumper->Dump([$blocks], ['blocks']) );

    $engine->add([ C => 3 ]);

    $blocks = $engine->blocks;
    cmp_deeply(
        $blocks,
        bag( $block_A1_A4, $block_B2, $block_C3 ),
        "single-blocked blocks"
    ) or diag( Data::Dumper->Dump([$blocks], ['blocks']) );

}

=pod

sub dedup_002_multiple_blocking : Test(2) {
    my $engine = DeDup::Engine->new(
        blocking => [
            sub { $_[0][0] },
            sub { $_[0][1] % 2 },
        ],
    );
    ok($engine, "instantiate DeDup::Engine with multiple blocking subs");

    $engine->add([ A => 1 ], [ B => 2 ], [ C => 3 ], [ A => 4 ]);

    my $blocks = $engine->blocks;
    cmp_deeply(
        $blocks,
        bag(
            {
                keys => [ 'A', 0 ],
                objects => bag( [ A => 4 ] ),
            },
            {
                keys => [ 'A', 1 ],
                objects => bag( [ A => 1 ] ),
            },
            {
                keys => [ 'B' ],
                objects => bag( [ B => 2 ] ),
            },
            {
                keys => [ 'C' ],
                objects => bag( [ C => 3 ] ),
            },
        ),
        "multi-blocked blocks"
    );
}

=cut

1;
