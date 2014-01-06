package Data::Dedup::Files::CLI::_guts;
use 5.016;
use strict;
use warnings;
use mop 0.03;
use signatures 0.07;

use CLI::Startup 0.08;
use Try::Tiny 0.18;

use Data::Dedup::Files;

# core modules
use Scalar::Util 'refaddr';


=head1 NAME

Data::Dedup::Files::CLI - A command-line interface to deduplicate files.

=cut


my %options = (
    'dir|d=s@' => 'directory under which to scan',
    'quiet|q' => 'suppress all messages',
    'verbose|v+' => 'display extra messages',
    'debug' => 'include information only interesting to developers',
    'format|f=s' => 'specify format of output: robot, text',
    'outfile|o=s' => 'write duplicate report to a file instead of standard out',
    'progress|P' => 'displays progress messages on standard error',
);


{
    my $kibi = 1024;
    my $mebi = $kibi * 1024;
    my $gibi = $mebi * 1024;
    my $tebi = $gibi * 1024;

    my %prefix_map = (
        $kibi => 'Ki',
        $mebi => 'Mi',
        $gibi => 'Gi',
        $tebi => 'Ti',
    );

    my @prefix_scales = ( $tebi, $gibi, $mebi, $kibi ); # from biggest to smallest

    sub human_readable_bytes {
        my ($bytes) = @_;

        for my $scale (@prefix_scales) {
            if (abs($bytes) > $scale) {
                return sprintf('%.1f', $bytes/$scale) . ' ' . $prefix_map{$scale} . 'B';
            }
        }
        return $bytes . ' B';
    }
}


# Removes " at FILE line ##" and everything after it from a warning message.
# This message may appear on the same line as the warning message,
# or it may appear on a line by itself (e.g., with Carp).
sub _remove_source_loc {
    my ($msg) = @_;
    return @_ if ref $msg;
    my @lines;
    for my $line ($msg =~ m/^.*$/gm) {
        if ($line =~ s/ at .+? line \d+.*$//) {
            push @lines, $line if $line;
            last;
        } else {
            push @lines, $line;
        }
    }
    return map "$_\n", @lines;
}


class Data::Dedup::Files::CLI {
    has $!CLI;
    has $!dedup = Data::Dedup::Files->new;

    has $!stdout is rw = *STDOUT;
    has $!stderr is rw = *STDERR;

    method BUILD($args) {
        $!CLI = CLI::Startup->new({
            usage => '--dir /path/to/scan [options...]',
            options => \%options,
        });
    }

    has $!files_count = 0;
    has $!file_bytes_count = 0;
    has $!files_unreadable_count = 0;

    method _update_progress_sub($display_progress) {
        return sub {
            state $granularity = 1000; # number of files between updates

            my $filesize = (@_ % 2) && shift // 0;
            my (%args) = @_;

            state $next_min_files_to_print = 0;

            $!files_count++;
            $!file_bytes_count += $filesize;
            $!files_unreadable_count++ if $args{ignored_unreadable};

            if ($!files_count >= $next_min_files_to_print || $args{force_display}) {
                my $human_readable_bytes = human_readable_bytes($!file_bytes_count);
                print $!stderr ("\rscanned $!files_count files, $human_readable_bytes")
                    if $display_progress;

                print $!stderr ((' ' x 12), ("\b" x 12)) if $display_progress;
                $next_min_files_to_print
                    = (int($!files_count / $granularity) + 1) * $granularity;
            }
        };
    }

    method run {
        $!CLI->init;
        my $opts = $!CLI->get_options;

        my $syswarn = $SIG{__WARN__} || sub { warn @_ }; # original "warn"
        local $SIG{__WARN__}
            = $opts->{quiet} ? sub { } # quiet = suppress all
            : $opts->{debug} ? sub { $syswarn->(@_) } # debug = let all through
            : sub { $syswarn->(_remove_source_loc @_) };

        try {
            my $update_progress = $self->_update_progress_sub($opts->{progress});

            $!dedup->scan(
                dir => $_,
                progress => $update_progress,
            ) for @{$opts->{dir}};

            # display final update
            $update_progress->(force_display => 1) if ($opts->{progress});
            print $!stderr ("\n") if ($opts->{progress});

        } catch {
            print $!stderr ("\n") if ($opts->{progress});
            die $_;
        };

        my $file_list = $!dedup->duplicates(
            resolve_hardlinks => sub { ( sort { $a cmp $b } @{$_[0]} )[0] },
        );

        my $outfile = $opts->{outfile};
        my $outfh = do {
            if ($outfile) {
                open my $outfh, '>', $outfile
                    or die "Can't open '$outfile' for writing";
                $outfh;
            } else {
                $!stdout;
            }
        };

        print $outfh
            sort { $a cmp $b }
            map {
                (join "\t", sort { $a cmp $b } @$_) . "\n"
            } grep { @$_ > 1 } @$file_list;

        close $outfh if $outfile; # mirrors "if ($outfile) { open $outfh ..." above

        return 0;
    }
}


=head1 AUTHOR

J. Timothy King (www.JTimothyKing.com, github:JTimothyKing)

=head1 LICENSE

This software is copyright 2014 J. Timothy King.

This is free software. You may modify it and/or redistribute it under the terms of
The Apache License 2.0. (See the LICENSE file for details.)

=cut

1;
