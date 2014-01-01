#!/usr/bin/env perl

package t::unit::Dedup::Engine;
use strict;
use warnings;
use Test::Most;
use parent 'Test::Class';
__PACKAGE__->runtests;

BEGIN {


use Data::Dumper;


{ # Load module under test, and bail out if it dies.
    my $module_loaded;
    END { BAIL_OUT "Could not load module under test" unless $module_loaded }
    use Dedup::Engine;
    $module_loaded = 1;
}


# Block descriptions (in Test::Deep format) used in following tests.

sub _block {
    my ($keyspec, $objspec) = @_;

    my @keys = split '', $keyspec;

    my @objects = map { [ split '' ] } split ' ', $objspec;

    return noclass({
        keys => \@keys,
        objects => bag( @objects ),
    });
}


sub dedup_engine_000_default : Test(3) {
    my $engine = Dedup::Engine->new;
    ok($engine, "instantiate default Dedup::Engine");

    $engine->add([ A => 1 ]);

    my $blocks = $engine->blocks;
    cmp_deeply(
        $blocks,
        bag( _block(''=>'A1') ),
        "one object default blocking"
    ) or diag( Data::Dumper->Dump([$blocks], ['blocks']) );

    $engine->add([ B => 2 ], [ C => 3 ]);

    $blocks = $engine->blocks;
    cmp_deeply(
        $blocks,
        bag( _block(''=>'A1 B2 C3') ),
        "three objects default blocking"
    ) or diag( Data::Dumper->Dump([$blocks], ['blocks']) );
}

sub dedup_engine_001_blocking : Test(5) {
    my $engine = Dedup::Engine->new(
        blocking => sub { $_[0][0] },
    );
    ok($engine, "instantiate Dedup::Engine with blocking sub");

    $engine->add([ A => 1 ]);

    my $blocks = $engine->blocks;
    cmp_deeply(
        $blocks,
        bag( _block(''=>'A1') ),
        "one object in a block"
    ) or diag( Data::Dumper->Dump([$blocks], ['blocks']) );

    $engine->add([ B => 2 ]);

    $blocks = $engine->blocks;
    cmp_deeply(
        $blocks,
        bag( _block(A=>'A1'), _block(B=>'B2') ),
        "two objects in two blocks"
    ) or diag( Data::Dumper->Dump([$blocks], ['blocks']) );

    $engine->add([ A => 4 ]);

    $blocks = $engine->blocks;
    cmp_deeply(
        $blocks,
        bag( _block(A=>'A1 A4'), _block(B=>'B2') ),
        "three objects in two blocks"
    ) or diag( Data::Dumper->Dump([$blocks], ['blocks']) );

    $engine->add([ C => 3 ]);

    $blocks = $engine->blocks;
    cmp_deeply(
        $blocks,
        bag( _block(A=>'A1 A4'), _block(B=>'B2'), _block(C=>'C3') ),
        "single-blocked blocks"
    ) or diag( Data::Dumper->Dump([$blocks], ['blocks']) );

}

sub dedup_engine_002_multiple_blocking : Test(2) {
    my $engine = Dedup::Engine->new(
        blocking => [
            sub { $_[0][0] },
            sub { $_[0][1] % 2 },
        ],
    );
    ok($engine, "instantiate Dedup::Engine with multiple blocking subs");

    $engine->add([ A => 1 ], [ B => 2 ], [ C => 3 ], [ A => 4 ]);

    my $blocks = $engine->blocks;
    cmp_deeply(
        $blocks,
        bag( _block(A0=>'A4'), _block(A1=>'A1'), _block(B=>'B2'), _block(C=>'C3') ),
        "multi-blocked blocks"
    ) or diag( Data::Dumper->Dump([$blocks], ['blocks']) );
}


}

1;
