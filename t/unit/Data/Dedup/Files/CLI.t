#!/usr/bin/env perl

package t::unit::Data::Dedup::Files::CLI;
use 5.016;
use strict;
use warnings;
use Test::Most 0.31;
use parent 'Test::Class';
__PACKAGE__->runtests;

use Test::MockObject 1.20120301;

# core modules
use Carp ();
use File::Spec ();
use File::Temp 'tempdir', 'mktemp';

BEGIN {

{ # Load module under test, and bail out if it dies.
    my $module_loaded;
    END { BAIL_OUT "Could not load module under test" unless $module_loaded }
    use Data::Dedup::Files::CLI;
    $module_loaded = 1;
}

# signal to Test::Class not to implicitly skip tests
sub fail_if_returned_early { 1 }


sub create_CLI : Test(setup) {
    my $self = shift;

    my $dedup = $self->{dedup} = Test::MockObject->new;

    $self->{output} = $self->{errors} = '';

    open my $fdout, '>>', \($self->{output})
        or die "Can't open fd to output variable: $!";
    open my $fderr, '>>', \($self->{errors})
        or die "Can't open fd to errors variable: $!";
    @$self{qw(fdout fderr)} = ($fdout, $fderr);

    $self->{cli} = Data::Dedup::Files::CLI->new(
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
        } "resolve_hardlinks is a function that chooses the first alphabetically";

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


sub _setup_for_warnings_test {
    my $self = shift;
    my ($dedup, $cli) = @$self{qw(dedup cli)};

    my $plain_warning = "Plain warning";
    my $sourceloc_warning = "Source loc warning";
    my $carp_warning = "Carp warning";
    my $cluck_warning = "Cluck warning";
    $dedup->mock(scan => sub {
        warn "$plain_warning\n";
        warn "$sourceloc_warning";
        Carp::carp("$carp_warning\n");
        Carp::cluck($cluck_warning);
    });

    my $line = __LINE__;
    my $sourceloc_warning_loc = ' at ' . __FILE__ . ' line ' . ($line-5) . '.';
    my $cluck_warning_loc = ' at ' . __FILE__ . ' line ' . ($line-3) . '.';

    $dedup->mock(duplicates => sub { [] });

    return ($plain_warning, $sourceloc_warning, $carp_warning, $cluck_warning,
            $sourceloc_warning_loc, $cluck_warning_loc);
}

sub dedup_files_cli__normal_warnings : Test(2) {
    my $self = shift;
    my ($dedup, $cli) = @$self{qw(dedup cli)};

    my ($plain_warning, $sourceloc_warning, $carp_warning, $cluck_warning)
        = $self->_setup_for_warnings_test;

    local @ARGV = qw(--format=robot --dir=foo);

    # Unfortunately, we can't use Test::Warn here, because it f*cks with
    # the actual warning text sent upstream, trying to be too clever by half.
    # (See Test::Warn::_canonical_got_warning.)
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };

    lives_and { cmp_ok($cli->run(), '>=', 0) }
        "returns success code";

    cmp_deeply( \@warnings => [
        map "$_\n",
            $plain_warning,
            $sourceloc_warning,
            $carp_warning,
            $cluck_warning
    ], "warnings normally displayed to the user");
}

sub dedup_files_cli__quiet_warnings : Test(2) {
    my $self = shift;
    my ($dedup, $cli) = @$self{qw(dedup cli)};

    $self->_setup_for_warnings_test;

    local @ARGV = qw(--format=robot --quiet --dir=foo);

    # Unfortunately, we can't use Test::Warn here, because it f*cks with
    # the actual warning text sent upstream, trying to be too clever by half.
    # (See Test::Warn::_canonical_got_warning.)
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };

    lives_and { cmp_ok($cli->run(), '>=', 0) }
        "returns success code";

    cmp_deeply( \@warnings => [
        # none
    ], "warnings with --quiet switch");
}

sub dedup_files_cli__debug_warnings : Test(2) {
    my $self = shift;
    my ($dedup, $cli) = @$self{qw(dedup cli)};

    my ($plain_warning, $sourceloc_warning, $carp_warning, $cluck_warning,
        $sourceloc_warning_loc, $cluck_warning_loc)
            = $self->_setup_for_warnings_test;

    local @ARGV = qw(--format=robot --debug --dir=foo);

    # Unfortunately, we can't use Test::Warn here, because it f*cks with
    # the actual warning text sent upstream, trying to be too clever by half.
    # (See Test::Warn::_canonical_got_warning.)
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };

    lives_and { cmp_ok($cli->run(), '>=', 0) }
        "returns success code";

    my $carp_loc = qr[ at \S*?/Test/MockObject.pm line \d+.];
    cmp_deeply( \@warnings, [
        re(qr[^\Q$plain_warning\E\n$]),
        re(qr[^\Q$sourceloc_warning$sourceloc_warning_loc\E\n$]),
        re(qr[^\Q$carp_warning\E\n$carp_loc\n$]),
        re(qr[^\Q$cluck_warning$cluck_warning_loc\E\n\s+.*?called at]),
    ], "warnings with --debug switch");
}


sub dedup_files_cli__outfile : Test(4) {
    my $self = shift;
    my ($dedup, $cli) = @$self{qw(dedup cli)};

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

    $dedup->mock(scan => sub { });
    $dedup->mock(duplicates => sub { \@duplicates });

    my $outfile = mktemp( File::Spec->catfile(tempdir( CLEANUP => 1 ), 'X' x 10) );

    local @ARGV = qw(--format=robot --quiet --dir=foo);
    push @ARGV, "--outfile=$outfile";

    lives_and { cmp_ok($cli->run(), '>=', 0) }
        "returns success code";

    eq_or_diff($self->{output} => '',
        "produces no output with --quiet and --outfile");

    ok( open(my $outfh, '<', $outfile), "--outfile creates readable output file" );

    my $outfile_contents = join '', <$outfh>;
    eq_or_diff( $outfile_contents => $duplicate_report,
        "duplicate report in specified outfile" );

    close $outfh;
}


}

1;
