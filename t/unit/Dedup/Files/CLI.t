#!/usr/bin/env perl

package t::unit::Dedup::Files::CLI;
use 5.016;
use strict;
use warnings;
use Test::Most;
use parent 'Test::Class';
__PACKAGE__->runtests;

use Test::MockObject;

BEGIN {

{ # Load module under test, and bail out if it dies.
    my $module_loaded;
    END { BAIL_OUT "Could not load module under test" unless $module_loaded }
    use Dedup::Files::CLI;
    $module_loaded = 1;
}

# signal to Test::Class not to implicitly skip tests
sub fail_if_returned_early { 1 }


sub create_CLI : Test(setup) {
    my $self = shift;

    my $dedup = $self->{dedup} = Test::MockObject->new;

    open my $fdout, '>', \($self->{output})
        or die "Can't open fd to output variable: $!";
    open my $fderr, '>', \($self->{errors})
        or die "Can't open fd to errors variable: $!";
    @$self{qw(fdout fderr)} = ($fdout, $fderr);

    $self->{cli} = Dedup::Files::CLI->new(
        dedup => $dedup,
        stdout => $fdout,
        stderr => $fderr,
    );
}


sub cleanup_CLI : Test(teardown) {
    my $self = shift;

    delete $self->{cli};
    close delete $self->{fderr};
    close delete $self->{fdout};
    delete @$self{qw(errors output dedup)};
}


sub dedup_files_cli__basic_function : Test(4) {
    my $self = shift;
    my ($dedup, $cli) = @$self{qw(dedup cli)};

    my @dirs_to_scan = qw(dir1 dir2 dir3);
    my @dirs_scanned; # stuffed by $dedup->scan(), mocked below

    my @duplicates = (
        [ qw(foo bar baz) ],
        [ qw(qux quux) ],
        [ qw(alpha beta gamma delta epsilon) ],
    );
    my $duplicate_report = <<"END_DUPLICATE_REPORT"; # sorted in both dimensions
alpha\tbeta\tdelta\tepsilon\tgamma
bar\tbaz\tfoo
quux\tqux
END_DUPLICATE_REPORT

    $dedup->mock(scan => sub {
        my ($self, %args) = @_;
        push @dirs_scanned, $args{dir};
    });
    $dedup->mock(duplicates => sub {
        my ($self, %args) = @_;

        state $already_called;
        fail("dedup->duplicates() must be called once and only once")
            if $already_called++;

        my $resolve_hardlinks = $args{resolve_hardlinks};
        lives_and {
            my $preferred_name = $resolve_hardlinks->(
                [qw(the quick brown fox)],
            );
            is($preferred_name, 'brown')
        } "resolve hardlinks is a function that chooses the first alphabetically";

        return \@duplicates;
    });

    local @ARGV = qw(--format=robot --quiet);
    push @ARGV, "--dir=$_" for @dirs_to_scan;

    lives_and { cmp_ok($cli->run(), '>=', 0) }
        "returns success code";

    cmp_deeply(\@dirs_scanned => \@dirs_to_scan,
        "directories to scan should be passed to scan()");

    eq_or_diff($self->{output} => $duplicate_report,
        "produces expected output");
}


}

1;
