package Data::Dedup::Files; # for auto-placed symbols, like $VERSION

package Data::Dedup::Files::_guts;
use 5.016;
use strict;
use warnings;
use mop 0.03;
use signatures 0.07;

use Data::Dedup::Engine;
use Data::Dedup::Files::DigestFactory;

# core modules
use File::Find ();
use Scalar::Util 'blessed';

=head1 NAME

Data::Dedup::Files - Detect duplicate files using Data::Dedup::Engine

=head1 SYNOPSIS

    my $dedup = Data::Dedup::Files->new(
        dir => '/path/to/directory/structure/to/dedup',
    );

    $dedup->scan();

    my $file_list = $dedup->duplicates;
    for my $files (@$file_list) {
        print @$files > 1 ? 'duplicates' : 'unique', "\n",
              (map "  $_\n", @$files);
    }

Or...

    my $dedup = Data::Dedup::Files->new;

    $dedup->scan( dir => '/a/path/to/dedup' );
    $dedup->scan( dir => '/another/path' );
    $dedup->scan( dir => '/yet/another/path' );

    my $file_list = $dedup->duplicates;

=head1 DESCRIPTION

This module scans a file structure and uses Data::Dedup::Engine to find
duplicates, that is, files with duplicate content.

=cut


class Data::Dedup::Files {

=head1 CONSTRUCTION

=over

=item new

Instantiate a new file-deduplicator with the given configuration. The configuration
may be passed into C<new> as a list of keys and values.

    my $dedup = Data::Dedup::Files->new(
        dir => '/path/to/search/for/duplicates',
        ignore_empty => 1,
        progress => \&display_progress,
    );

Alternatively, C<new> will accept a hash reference:

    my %config;
    build_config( \%config );
    my $dedup = Data::Dedup::Files->new( \%config );

=back

=head2 Configuration

C<< Data::Dedup::Files->new >> accepts the following configuration keys:

=over

=item dir

A path to the directory to scan.

The L<C<scan> method|scan> will recursively search this directory and
directories under it for files. This configuration option is optional, as the
directory path may be passed to C<scan> directly, but the directory to scan must
be specified in one of these places.

C<dir> can currently accept only one directory. If you wish to scan multiple
directories, pass the directory paths one at a time to C<scan> directly. (See
L<scan> below.) Or change the value of the C<dir> attribute in between each call
to C<scan>. (See L<Accessors and Mutators> below.)

=cut

    has $!dir is rw;


=item ignore_empty

True if zero-length files should be ignored.

Set C<ignore_empty> to a true value to have the scanner ignore zero-length
files. Otherwise, they will be passed to the deduplication engine as any other
file, and will all be considered duplicates of each other.

Note that this option may also be passed directly to the L<C<scan> method|scan>,
or its value may be modified between calls to scan by using the corresponding
L<mutator method|Accessors and Mutators>.

=cut

    has $!ignore_empty is rw;


=item progress

A subroutine to which to send progress messages.

If set, the directory scanner will call this sub for each file that is added to the
deduplicator, plus certain ignored files. [*]

    sub display_progress {
        my ($filesize, %flags) = @_;
        ...
    };

In the above stub, C<$filesize> is the size of the file encountered, in bytes.

C<%flags> are optional flags regarding the file. Currently, the only flag
supported is C<ignored_unreadable>, which if set to true indicates that the file
was scanned but will not be deduplicated, because its contents cannot be read
(probably due to insufficient permissions).

Note that this option may also be passed directly to the L<C<scan> method|scan>,
or its value may be modified between calls to scan by using the corresponding
L<mutator method|Accessors and Mutators>.

[*] Conceptually, the C<progress> sub should be called using a uniform and
backwards-compatible calling convention for all files encountered. The current
API is neither uniform, universal, nor backwards-compatible. B<Expect this part
of the API to change in future.>

=cut

    has $!progress is rw;

=back

=head2 Accessors and Mutators

The value of any of these configuration keys can be set on or retrieved from an instantiated
deduplicator object by calling the mutator/accessor method named after it:

    my $dir = $dedup->dir; # returns the directory to scan
    $dedup->dir( '/new/dir/to/scan' );

=cut

    has $!engine;
    has $!inodes_seen = {};

    method BUILD($args) {
        my $blocking = $args->{blocking} // Data::Dedup::Files::DigestFactory->new;
        $!engine = Data::Dedup::Engine->new( blocking => $blocking );
    }


=head1 METHODS

=over

=item scan

Scans a directory tree and finds duplicate files therein.

This method scans for files under the directory identified by the C<dir>
parameter, which can either be set as an object attribute (during
L<construction|CONSTRUCTION> or via L<attribute mutator|Accessors and Mutators>)
or passed directly into this method, as explained below.

While scanning, non-files and symbolic links are silently ignored. Zero-length
files will be silently ignored if C<ignore_empty> is true. (See below.) This
method also detects hardlinks, and will scan only the first link to a given
inode. Further links to the same inode will be silently ignored; however, they
can be reported later via the L<C<duplicates> method|duplicates> and
L<C<hardlinks> method|hardlinks>.

The scanner will call the specified L<C<progress> sub|progress>, if any.

C<scan> optionally accepts a list of arguments, specifying values for C<dir>,
C<ignore_empty>, and C<progress>. Any of these values that are specified in the
call to C<scan> will override the corresponding object attribute. For example:

    $dedup->scan(
        dir => '/dir/to/scan',
    );

This will scan F</dir/to/scan>, ignoring C<< $dedup->dir >>, but using the
preconfigured values (if any) of C<< $dedup->ignore_empty >> and C<<
$dedup->progress >>.

Any of these arguments override the corresponding attribute value only for the
duration of this particular call to C<scan> and will affect no other calls to
any other methods.

=cut

    method scan(%args) {
        my $dir = $args{dir} // $!dir;
        my $progress = $args{progress} // $!progress;
        my $ignore_empty = $args{ignore_empty} // $!ignore_empty;

        File::Find::find({
            no_chdir => 1,
            wanted => sub {
                return unless -f && !-l && (!$ignore_empty || -s > 0);

                return if 1 < push @{ $!inodes_seen->{ (lstat)[1] } }, $_;

                my $filesize = -s;

                if (! -r) {
                    warn "Can't read file '$_': skipping\n";
                    $progress->($filesize, ignored_unreadable => 1) if $progress;
                    return;
                }

                $!engine->add( $_ );

                $progress->($filesize) if $progress;
            },
        }, $dir);
    }


=item duplicates

=cut

    method duplicates(%args) {
        my $resolve_hardlinks = $args{resolve_hardlinks};

        my @file_list = map { $_->objects } @{$!engine->blocks};

        if ($resolve_hardlinks) {
            my %hardlinks = map {
                my $files = $_;
                @$files > 1 ? map { $_ => $files } @$files : ();
            } @{ $self->hardlinks };

            for my $files (@file_list) {
                for my $file (@$files) {
                    # !!! permanently changes the $file stored in $!engine->blocks
                    $file = $resolve_hardlinks->($hardlinks{$file})
                        if exists $hardlinks{$file};
                }
            }
        }

        return \@file_list;
    }


=item hardlinks

=cut

    method hardlinks { [ values %{$!inodes_seen} ] }

=item blocking

=cut

    method blocking { $!engine->blocking }

=item count_digests

=cut

    method count_digests { $!engine->count_keys_computed }

=item count_collisions

=cut

    method count_collisions { $!engine->count_collisions }

=back

=cut

}


=head1 SEE ALSO

L<Data::Dedup::Engine>


=head1 AUTHOR

J. Timothy King (www.JTimothyKing.com, github:JTimothyKing)

=head1 LICENSE

This software is copyright 2014 J. Timothy King.

This is free software. You may modify it and/or redistribute it under the terms of
The Apache License 2.0. (See the LICENSE file for details.)

=cut

1;
