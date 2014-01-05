#!/usr/bin/env perl

package t::unit::Data::Dedup::Files;
use 5.016;
use strict;
use warnings;
use Test::Most 0.31;
use parent 'Test::Class';
__PACKAGE__->runtests;

BEGIN {

# core modules
use Data::Dumper;
use File::Path 'make_path', 'remove_tree';
use File::Spec ();
use File::Temp 'tempdir', 'tempfile', 'mktemp';
use Time::HiRes ();


{ # Load module under test, and bail out if it dies.
    my $module_loaded;
    END { BAIL_OUT "Could not load module under test" unless $module_loaded }
    use Data::Dedup::Files;
    $module_loaded = 1;
}

# signal to Test::Class not to implicitly skip tests
sub fail_if_returned_early { 1 }


sub _random_string {
    my $length = shift // 256;
    return pack 'C*', map { rand(256) } 1 .. $length;
}

sub _create_files {
    my @files;
    my $seed = srand();
    for my $filespec (@_) {
        srand();
        my ($fh, $file) = tempfile( DIR => $filespec->{dir} );

        if ($filespec->{duplicate}) {
            srand($seed);
        } else {
            $seed = srand();
        }

        print $fh map { _random_string } 1 .. $filespec->{length};
        close $fh;
        push @files, $file;
    }
    return @files;
}


sub generate_test_dir : Test(setup) {
    my $self = shift;
    $self->{test_dir} = tempdir();
}

sub cleanup_test_dir : Test(teardown) {
    my $self = shift;
    remove_tree( $self->{test_dir} );
}


sub dedup_files__traverse_directory_trees : Test(2) {
    my $self = shift;
    my $test_dir = $self->{test_dir};

    my @dirs = (
        File::Spec->catdir($test_dir, 'dir1', 'dir1a'),
        File::Spec->catdir($test_dir, 'dir1', 'dir1b'),
        File::Spec->catdir($test_dir, 'dir2'),
    );
    make_path(@dirs);

    my @files = _create_files( map {
        my $dir = $_;
        map { {
            dir => $dir,
            length => 42,
            duplicate => 1,
        } } (0 .. rand(3))
    } @dirs );

    my $dedup = Data::Dedup::Files->new(dir => $test_dir);
    ok($dedup, "instantiate Data::Dedup::Files");

    $dedup->scan();

    my $file_list = $dedup->duplicates;
    cmp_deeply(
        $file_list => [ bag(@files) ],
    ) or diag( Data::Dumper->Dump([$file_list, [\@files]], ['got', 'expected']) );
}


sub dedup_files__ignore_symlinks : Test(2) {
SKIP: {
    skip "OS does not support symlinks", 2 unless eval { symlink("",""); 1 };

    my $self = shift;
    my $test_dir = $self->{test_dir};

    my @files = _create_files( map { {
        dir => $test_dir,
        length => 42,
        duplicate => 1,
    } } (1..1) );

    my $file = $files[0];
    for (1..10) {
        my $link = mktemp( File::Spec->catfile($test_dir, 'X' x 10) );
        symlink($file, $link) or die "cannot create symlink $link";
    }

    my $dedup = Data::Dedup::Files->new(dir => $test_dir);
    ok($dedup, "instantiate Data::Dedup::Files");

    $dedup->scan();

    my $file_list = $dedup->duplicates;
    cmp_deeply(
        $file_list => [ bag(@files) ],
    ) or diag( Data::Dumper->Dump([$file_list, [\@files]], ['got', 'expected']) );
}
}


sub dedup_files__hardlinks : Test(5) {
SKIP: {
    my $self = shift;
    my $test_dir = $self->{test_dir};

    my @files = _create_files( map { {
        dir => $test_dir,
        length => 42,
        duplicate => 1,
    } } (1..1) );

    my $file = $files[0];
    for (1..10) {
        my $link = mktemp( File::Spec->catfile($test_dir, 'X' x 10) );
        link($file, $link) or skip "cannot create hard link", 5;
        push @files, $link;
    }

    my $dedup = Data::Dedup::Files->new(dir => $test_dir);
    ok($dedup, "instantiate Data::Dedup::Files");

    $dedup->scan();

    my $file_list = $dedup->duplicates;
    cmp_deeply(
        $file_list => [ [ any(@files) ] ],
        "only one hardlink can be considered as a duplicate"
    ) or diag( Data::Dumper->Dump([$file_list], ['got']) );

    my $hardlinks = $dedup->hardlinks;
    cmp_deeply(
        $hardlinks => [ bag(@files) ],
        "hardlinks() returns lists of hardlinks"
    ) or diag( Data::Dumper->Dump([$hardlinks, [\@files]], ['got', 'expected']) );

    my $hardlinks_list = $hardlinks->[0];
    my $preferred_file_path = "The preferred filename!";
    $file_list = $dedup->duplicates(
        resolve_hardlinks => sub {
            my ($files) = @_;
            ok($files == $hardlinks_list, "list of hardlinks passed to resolve_hardlinks sub")
                or diag( Data::Dumper->Dump(["$files", "$hardlinks_list"], ['got', 'expected']) );
            return $preferred_file_path;
        },
    );
    cmp_deeply(
        $file_list => [ [ $preferred_file_path ] ],
        "resolve_hardlinks sub determines the path returned for a hardlink"
    );
}
}


sub dedup_files__filesize : Test(2) {
    my $self = shift;
    my $test_dir = $self->{test_dir};

    my @files = _create_files( map { {
        dir => $test_dir,
        length => $_ * 42,
        duplicate => 1,
    } } (1..10) );

    my $dedup = Data::Dedup::Files->new(dir => $test_dir);
    ok($dedup, "instantiate Data::Dedup::Files");

    $dedup->scan();

    my $file_list = $dedup->duplicates;
    my @expected = map { [$_] } @files;
    cmp_deeply(
        $file_list => bag(@expected),
    ) or diag( Data::Dumper->Dump([$file_list, \@expected], ['got', 'expected']) );
}


sub dedup_files__file_content : Test(2) {
    my $self = shift;
    my $test_dir = $self->{test_dir};

    my @files = _create_files( map { {
        dir => $test_dir,
        length => 42,
    } } (1..10) );

    my $dedup = Data::Dedup::Files->new(dir => $test_dir);
    ok($dedup, "instantiate Data::Dedup::Files");

    $dedup->scan();

    my $file_list = $dedup->duplicates;
    my @expected = map { [$_] } @files;
    cmp_deeply(
        $file_list => bag(@expected),
    ) or diag( Data::Dumper->Dump([$file_list, \@expected], ['got', 'expected']) );
}


sub dedup_zero_length_files : Test(4) {
    my $self = shift;
    my $test_dir = $self->{test_dir};

    my @files = _create_files( map { {
        dir => $test_dir,
        length => 42,
        duplicate => 1,
    } } (1..1) );

    my @empty_files;
    for (1..10) {
        my ($fd, $file) = tempfile( DIR => $test_dir );
        close $fd;
        push @empty_files, $file;
    }


    my $dedup = Data::Dedup::Files->new(dir => $test_dir);
    ok($dedup, "instantiate Data::Dedup::Files");

    $dedup->scan();

    my $file_list = $dedup->duplicates;
    cmp_deeply(
        $file_list => bag( bag(@files), bag(@empty_files) ),
        "by default scans zero-length files"
    ) or diag( Data::Dumper->Dump([$file_list, [\@files, \@empty_files]], ['got', 'expected']) );


    $dedup = Data::Dedup::Files->new(dir => $test_dir, ignore_empty => 1);
    ok($dedup, "instantiate Data::Dedup::Files with ignore_empty");

    $dedup->scan();

    $file_list = $dedup->duplicates;
    cmp_deeply(
        $file_list => [ bag(@files) ],
        "ignore_empty ignores zero-length files"
    ) or diag( Data::Dumper->Dump([$file_list, [\@files]], ['got', 'expected']) );
}


sub dedup_unreadable_files : Test(4) {
    my $self = shift;
    my $test_dir = $self->{test_dir};

    my @files = _create_files( map { {
        dir => $test_dir,
        length => 42,
        duplicate => 1,
    } } (1..3) );

    my $unreadable_file = shift @files;
    chmod 0, $unreadable_file;

    my $dedup = Data::Dedup::Files->new(dir => $test_dir, ignore_empty => 1);
    ok($dedup, "instantiate Data::Dedup::Files with ignore_empty");

    warning_like {
        lives_ok { $dedup->scan() } "scan does not die on unreadable files";
    } qr/cannot read file \Q$unreadable_file\E/,
        "scan emits appropriate warning on unreadable files";

    my $file_list = $dedup->duplicates;
    cmp_deeply(
        $file_list => [ bag( @files ) ],
        "ignore unreadable files, but deduplicate otherwise"
    ) or diag( Data::Dumper->Dump([$file_list], ['got']) );
}


}

1;
