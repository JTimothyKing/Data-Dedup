#!/usr/bin/env perl

package t::unit::Dedup::Engine;
use 5.016;
use strict;
use warnings;
use Test::Most;
use parent 'Test::Class';
__PACKAGE__->runtests;

BEGIN {

use Dedup::Engine::BlockingFactory;

# core modules
use Data::Dumper;


{ # Load module under test, and bail out if it dies.
    my $module_loaded;
    END { BAIL_OUT "Could not load module under test" unless $module_loaded }
    use Dedup::Engine;
    $module_loaded = 1;
}

# signal to Test::Class not to implicitly skip tests
sub fail_if_returned_early { 1 }


package t::unit::Dedup::Engine::_mock_blocking_factory {
    use mop;
    class t::unit::Dedup::Engine::_mock_blocking_factory
        with Dedup::Engine::BlockingFactory {
        has $!all_functions_returns;
        method all_functions { $!all_functions_returns }
    }
}


# Block descriptions (in Test::Deep format) used in following tests.
sub _block {
    my ($keyspec, $objspec) = @_;

    my @keys = split '', $keyspec;

    my @objects = map { [ split '' ] } split ' ', $objspec;

    return methods(
        keys => \@keys,
        objects => bag( @objects ),
    );
}

# returns (in Data::Dumper format) the output of $engine->blocks
sub _dump_blocks {
    my ($blocks) = @_;
    return Data::Dumper->Dump([
        [ map {
            mop::dump_object($_)
        } @$blocks ]
    ], ['blocks']);
}


sub dedup_engine__default : Test(3) {
    my $engine = Dedup::Engine->new;
    ok($engine, "instantiate default Dedup::Engine");

    $engine->add([ A => 1 ]);

    my $blocks = $engine->blocks;
    cmp_deeply(
        $blocks => bag( _block(''=>'A1') ),
        "one object default blocking"
    ) or diag( _dump_blocks($blocks) );

    $engine->add([ B => 2 ], [ C => 3 ]);

    $blocks = $engine->blocks;
    cmp_deeply(
        $blocks => bag( _block(''=>'A1 B2 C3') ),
        "three objects default blocking"
    ) or diag( _dump_blocks($blocks) );
}

sub dedup_engine__blocking : Test(5) {
    my $engine = Dedup::Engine->new(
        blocking => sub { $_[0][0] },
    );
    ok($engine, "instantiate Dedup::Engine with blocking sub");

    $engine->add([ A => 1 ]);

    my $blocks = $engine->blocks;
    cmp_deeply(
        $blocks => bag( _block(''=>'A1') ),
        "one object in a block"
    ) or diag( _dump_blocks($blocks) );

    $engine->add([ B => 2 ]);

    $blocks = $engine->blocks;
    cmp_deeply(
        $blocks => bag( _block(A=>'A1'), _block(B=>'B2') ),
        "two objects in two blocks"
    ) or diag( _dump_blocks($blocks) );

    $engine->add([ A => 4 ]);

    $blocks = $engine->blocks;
    cmp_deeply(
        $blocks => bag( _block(A=>'A1 A4'), _block(B=>'B2') ),
        "three objects in two blocks"
    ) or diag( _dump_blocks($blocks) );

    $engine->add([ C => 3 ]);

    $blocks = $engine->blocks;
    cmp_deeply(
        $blocks => bag( _block(A=>'A1 A4'), _block(B=>'B2'), _block(C=>'C3') ),
        "single-blocked blocks"
    ) or diag( _dump_blocks($blocks) );

}

sub dedup_engine__multiple_blocking : Test(2) {
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
        $blocks => bag( _block(A0=>'A4'), _block(A1=>'A1'), _block(B=>'B2'), _block(C=>'C3') ),
        "multi-blocked blocks"
    ) or diag( _dump_blocks($blocks) );
}


sub dedup_engine__blocking_factory : Test(6) {
    my $blocking_factory_1 = t::unit::Dedup::Engine::_mock_blocking_factory->new(
        all_functions_returns => [
            sub { $_[0][0] },
            sub { $_[0][1] % 2 },
        ],
    );

    my $engine = Dedup::Engine->new(
        blocking => $blocking_factory_1,
    );
    ok($engine, "instantiate Dedup::Engine with a blocking factory");

    $engine->add([ A => 1 ], [ B => 2 ], [ A => 4 ], [ A => 2 ]);

    my $blocks = $engine->blocks;
    cmp_deeply(
        $blocks => bag( _block(A0=>'A4 A2'), _block(A1=>'A1'), _block(B=>'B2') ),
        "blocks blocked by a blocking factory"
    ) or diag( _dump_blocks($blocks) );


    my $blocking_factory_2 = t::unit::Dedup::Engine::_mock_blocking_factory->new(
        all_functions_returns => [
            sub { $_[0][1] % 4 },
        ],
    );

    $engine = Dedup::Engine->new(
        blocking => [
            $blocking_factory_1,
            $blocking_factory_2,
        ],
    );
    ok($engine, "instantiate Dedup::Engine with an array of blocking factories");

    $engine->add([ A => 1 ], [ B => 2 ], [ A => 4 ], [ A => 2 ]);

    $blocks = $engine->blocks;
    cmp_deeply(
        $blocks => bag( _block(A00=>'A4'), _block(A02=>'A2'), _block(A1=>'A1'), _block(B=>'B2') ),
        "blocks blocked by multiple blocking factories"
    ) or diag( _dump_blocks($blocks) );


    $engine = Dedup::Engine->new(
        blocking => [
            $blocking_factory_1,
            $blocking_factory_2,
            sub { $_[0][1] % 3 },
        ],
    );
    ok($engine, "instantiate Dedup::Engine with an array of blocking factories");

    $engine->add([ A => 1 ], [ B => 2 ], [ A => 4 ], [ A => 2 ], [ B => 6 ] );

    $blocks = $engine->blocks;
    cmp_deeply(
        $blocks => bag( _block(A00=>'A4'), _block(A02=>'A2'), _block(A1=>'A1'),
                        _block(B022=>'B2'),  _block(B020=>'B6') ),
        "blocks blocked by multiple blocking factories"
    ) or diag( _dump_blocks($blocks) );
}


sub dedup_engine__invalid_blocking : Test(3) {
    throws_ok {
        Dedup::Engine->new( blocking => 'FOO!' );
    } qr/value at index 0 /, "'FOO!' is an invalid blocking function";

    throws_ok {
        Dedup::Engine->new(
            blocking => t::unit::Dedup::Engine::_mock_blocking_factory->new(
                all_functions_returns => 'Not an array!',
            )
        );
    } qr/all_functions must return an arrayref/,
        "blocking factory all_functions must return an arrayref";

    throws_ok {
        Dedup::Engine->new(
            blocking => t::unit::Dedup::Engine::_mock_blocking_factory->new(
                all_functions_returns => [ sub {}, 'Not a coderef!' ],
            )
        );
    } qr/all_functions must return an array of coderefs.+value at index 1 /,
        "blocking factory all_functions must return an array of coderefs";
}


}

1;
