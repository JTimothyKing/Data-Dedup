#!/usr/bin/env perl

package t::unit::Dedup::Files;
use 5.016;
use strict;
use warnings;
use Test::Most;
use parent 'Test::Class';
__PACKAGE__->runtests;

BEGIN {


use Data::Dumper;

use File::Path 'make_path', 'remove_tree';
use File::Spec ();
use File::Temp 'tempdir', 'tempfile';
use Time::HiRes ();


{ # Load module under test, and bail out if it dies.
    my $module_loaded;
    END { BAIL_OUT "Could not load module under test" unless $module_loaded }
    use Dedup::Files;
    $module_loaded = 1;
}


sub generate_test_dir : Test(setup) {
    my $self = shift;
    $self->{test_dir} = tempdir();
}

sub cleanup_test_dir : Test(teardown) {
    my $self = shift;
    remove_tree( $self->{test_dir} );
}


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


sub dedup_files_traverse_directory_trees : Test(2) {
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

    my $dedup = Dedup::Files->new(dir => $test_dir);
    ok($dedup, "instantiate Dedup::Files");

    $dedup->scan();

    my $file_list = $dedup->duplicates;
    cmp_deeply(
        $file_list,
        [ bag(@files) ],
    ) or diag( Data::Dumper->Dump([$file_list, [\@files]], ['got', 'expected']) );
}


sub dedup_files_ignore_symlinks : Test(2) {
    return "OS does not support symlinks" unless eval { symlink("",""); 1 };

    my $self = shift;
    my $test_dir = $self->{test_dir};

    my @files = _create_files( map { {
        dir => $test_dir,
        length => 42,
        duplicate => 1,
    } } (1..1) );

    my $file = $files[0];
    for (1..10) {
        my (undef, $link) = tempfile( DIR => $test_dir, OPEN => 0 );
        symlink($file, $link) or die "cannot create symlink $link";
    }

    my $dedup = Dedup::Files->new(dir => $test_dir);
    ok($dedup, "instantiate Dedup::Files");

    $dedup->scan();

    my $file_list = $dedup->duplicates;
    cmp_deeply(
        $file_list,
        [ bag(@files) ],
    ) or diag( Data::Dumper->Dump([$file_list, [\@files]], ['got', 'expected']) );
}


sub dedup_files_ignore_hardlinks : Test(2) {
    my $self = shift;
    my $test_dir = $self->{test_dir};

    my @files = _create_files( map { {
        dir => $test_dir,
        length => 42,
        duplicate => 1,
    } } (1..1) );

    my $file = $files[0];
    for (1..10) {
        my (undef, $link) = tempfile( DIR => $test_dir, OPEN => 0 );
        link($file, $link) or return "cannot create hard link";
        push @files, $link;
    }

    my $dedup = Dedup::Files->new(dir => $test_dir);
    ok($dedup, "instantiate Dedup::Files");

    $dedup->scan();

    my $file_list = $dedup->duplicates;
    cmp_deeply(
        $file_list,
        [ [ any(@files) ] ],
    ) or diag( Data::Dumper->Dump([$file_list], ['got']) );
}


sub dedup_files_filesize : Test(2) {
    my $self = shift;
    my $test_dir = $self->{test_dir};

    my @files = _create_files( map { {
        dir => $test_dir,
        length => $_ * 42,
        duplicate => 1,
    } } (1..10) );

    my $dedup = Dedup::Files->new(dir => $test_dir);
    ok($dedup, "instantiate Dedup::Files");

    $dedup->scan();

    my $file_list = $dedup->duplicates;
    my @expected = map { [$_] } @files;
    cmp_deeply(
        $file_list,
        bag(@expected),
    ) or diag( Data::Dumper->Dump([$file_list, \@expected], ['got', 'expected']) );
}


sub dedup_files_file_content : Test(2) {
    my $self = shift;
    my $test_dir = $self->{test_dir};

    my @files = _create_files( map { {
        dir => $test_dir,
        length => 42,
    } } (1..10) );

    my $dedup = Dedup::Files->new(dir => $test_dir);
    ok($dedup, "instantiate Dedup::Files");

    $dedup->scan();

    my $file_list = $dedup->duplicates;
    my @expected = map { [$_] } @files;
    cmp_deeply(
        $file_list,
        bag(@expected),
    ) or diag( Data::Dumper->Dump([$file_list, \@expected], ['got', 'expected']) );
}


sub dedup_ignore_zero_length_files : Test(2) {
    my $self = shift;
    my $test_dir = $self->{test_dir};

    my @files = _create_files( map { {
        dir => $test_dir,
        length => 42,
        duplicate => 1,
    } } (1..1) );

    for (1..10) {
        my ($fd, $file) = tempfile( DIR => $test_dir );
        close $fd;
    }

    my $dedup = Dedup::Files->new(dir => $test_dir);
    ok($dedup, "instantiate Dedup::Files");

    $dedup->scan();

    my $file_list = $dedup->duplicates;
    cmp_deeply(
        $file_list,
        [ [ any(@files) ] ],
    ) or diag( Data::Dumper->Dump([$file_list, [\@files]], ['got', 'expected']) );
}


}

1;
