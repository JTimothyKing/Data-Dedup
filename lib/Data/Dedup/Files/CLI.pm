package Data::Dedup::Files::CLI::_guts;
use 5.016;
use strict;
use warnings;
use mop 0.03;
use signatures 0.07;

use CLI::Startup 0.08;
use List::MoreUtils 0.33 'pairwise';
use Try::Tiny 0.18;

use Data::Dedup::Files;

# core modules
use IO::Handle ();
use List::Util 'sum', 'max';
use Scalar::Util 'refaddr';


=head1 NAME

Data::Dedup::Files::CLI - A command-line interface to deduplicate files.

=cut


my %options = (
    'alg|a=s@' => 'digest algorithm(s) to use (can be specified multiple times)',
    'debug' => 'include information only interesting to developers',
    'dir|d=s@' => 'directory under which to scan (can be specified multiple times)',
    'format|f=s' => 'specify format of output: (only the default, "robot," currently supported)',
    'outfile|o=s' => 'write duplicate report to a file instead of standard out',
    'progress|P' => 'displays progress messages on standard error',
    'quiet|q' => 'suppress all messages',
    'verbose|v+' => 'display extra messages',
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


sub _algs_to_blocking($algs) {
    my $digest_factory = Data::Dedup::Files::DigestFactory->new;
    my @blocking;
    for my $alg (@$algs) {
        my $alg_method = 'from_' . $alg;
        push @blocking, $digest_factory->$alg_method();
    }
    return \@blocking;
}


class Data::Dedup::Files::CLI {
    has $!CLI;

    has $!dedup; # mockable for unit testing

    has $!stdout is rw = *STDOUT;
    has $!stderr is rw = *STDERR;

    method BUILD($args) {
        $!CLI = CLI::Startup->new({
            usage => '--dir /path/to/scan [options...]',
            options => \%options,
        });
    }

    # progress accumulators
    has $!files_count = 0;
    has $!file_bytes_count = 0;
    has $!files_unreadable_count = 0;
    has $!file_unreadable_bytes_count = 0;

    # state of the progress display
    has $!next_min_files_to_print = 0;
    has $!progress_message_length = 0;

    method _update_progress_sub($display_progress) {
        return sub {
            state $granularity = 1000; # number of files between updates

            my $filesize = (@_ % 2) && shift // 0;
            my (%args) = @_;

            $!files_count++;
            $!file_bytes_count += $filesize;
            if ($args{ignored_unreadable}) {
                $!files_unreadable_count++;
                $!file_unreadable_bytes_count += $filesize;
            }

            return unless $display_progress; # All the stuff below is just for display.

            if ($!files_count >= $!next_min_files_to_print || $args{force_display}) {
                my $human_readable_bytes = human_readable_bytes($!file_bytes_count);
                my $progress_message = "scanned $!files_count files, $human_readable_bytes";

                my $new_msg_length = length $progress_message;
                my $msg_overflow_chars = $!progress_message_length - $new_msg_length;
                $msg_overflow_chars = 0 if $msg_overflow_chars < 0;
                $!progress_message_length = $new_msg_length;

                print $!stderr ("\r$progress_message", (' ' x $msg_overflow_chars));

                $!next_min_files_to_print
                    = (int($!files_count / $granularity) + 1) * $granularity;
            }
        };
    }

    method _clear_progress_display {
        print $!stderr ("\r", (' ' x $!progress_message_length), "\r");
        $!next_min_files_to_print = 0; # Re-display ASAP
    }

    method _scan_dirs($opts) {
        my $display_progress = $opts->{progress};

        try {
            my $update_progress = $self->_update_progress_sub($display_progress);

            my $syswarn = $SIG{__WARN__} || sub { warn @_ }; # original "warn"
            local $SIG{__WARN__} # $opts->{quiet} in the following line is a HACK
                = ($display_progress && !$opts->{quiet}) ? sub {
                    # in case $!stderr and warnings appear on the same terminal
                    $self->_clear_progress_display;
                    $syswarn->(@_);
                }
                : $SIG{__WARN__}; # nothing special if not $display_progress

            my %seen_dir;
            DIR: for my $dir (@{$opts->{dir}}) {
                if ($seen_dir{$dir}++) {
                    warn "Skipping repeated instance of $dir\n";
                    next DIR;
                }

                if ($opts->{verbose}) {
                    # in case $!stderr and $!stdout are displaying on the same terminal
                    $self->_clear_progress_display if $display_progress;
                    $!stdout->printflush("Scanning $dir\n");
                }

                $!dedup->scan(
                    dir => $dir,
                    progress => $update_progress,
                );
            }

            # display final update
            if ($display_progress) {
                $update_progress->(force_display => 1);
                print $!stderr ("\n");
            }

        } catch {
            print $!stderr ("\n") if $display_progress;
            die $_;
        };
    }

    method _display_statistics(%stats) {
        my $human_file_bytes = human_readable_bytes($!file_bytes_count);
        my $human_file_unreadable_bytes = human_readable_bytes($!file_unreadable_bytes_count);

        print $!stdout (
            "$!files_count files scanned, $human_file_bytes\n",
            "($!files_unreadable_count were unreadable and ignored, $human_file_unreadable_bytes)\n"
                x!! $!files_unreadable_count,
        );

        print $!stdout (
            "\nFound:\n",
            '  ', $stats{unique}, " unique files\n",
            '+ ', $stats{distinct}, " distinct files with duplicates, and\n",
            '+ ', $stats{duplicate}, " dupliciates of those\n",
            '= ', (sum @stats{qw(unique distinct duplicate)}), " total files deduped\n",
        );

        my $digests = $stats{digests};
        my $num_digests = $stats{num_digests};
        my $collisions = $stats{collisions};

        my $max_digest_length = max map { length $_ } @$digests;
        my $file_count_length = length $!files_count;
        print $!stdout (
            "\nAt each blocking level:\n",
            sprintf('  %-*s : %*s %*s'."\n",
                    $max_digest_length, 'Digest',
                    $file_count_length, '# ',
                    $file_count_length, 'coll'),

            ( map {
                sprintf('  %-*s : %*d %*d'."\n",
                        $max_digest_length, $digests->[$_],
                        $file_count_length, $num_digests->[$_],
                        $file_count_length, $collisions->[$_])
            } 0 .. $#{$digests} ),
        );
    }


    method run {
        $!CLI->init;
        my $opts = $!CLI->get_options;
        $!CLI->die_usage if (@ARGV);

        my $quiet = $opts->{quiet};
        my $verbose = $opts->{verbose};
        my $debug = $opts->{debug};

        $quiet = undef if $verbose || $debug;

        $!dedup = Data::Dedup::Files->new(
           ($opts->{alg} ? ( blocking => _algs_to_blocking($opts->{alg}) ) : () ),
        ) unless $!dedup;

        if ($verbose) {
            my $blocking = $!dedup->blocking;
            $!stdout->printflush(
                "Using digest algorithms: ", (join ' ', map { $_->id } @$blocking), "\n",
            );
        }

        my $syswarn = $SIG{__WARN__} || sub { warn @_ }; # original "warn"
        local $SIG{__WARN__}
            = $quiet ? sub { } # quiet = suppress all
            : $debug ? sub { $syswarn->(@_) } # debug = let all through
            : sub { $syswarn->(_remove_source_loc @_) };

        $self->_scan_dirs($opts);

        my $file_list = $!dedup->duplicates(
            resolve_hardlinks => sub { ( sort { $a cmp $b } @{$_[0]} )[0] },
        );

        my $outfile = $opts->{outfile};
        $outfile = undef if $outfile && $outfile eq '-';
        my $outfh = do {
            if ($outfile) {
                open my $outfh, '>', $outfile
                    or die "Can't open '$outfile' for writing";
                $outfh;
            } else {
                $!stdout;
            }
        };

        my $is_report_on_stdout = !$outfile;
        my @separator = ( '-' x 30, "\n" )x!! ($verbose && $is_report_on_stdout);

        print $!stdout (@separator) if @separator;

        print $outfh
            sort { $a cmp $b }
            map {
                (join "\t", sort { $a cmp $b } @$_) . "\n"
            } grep { @$_ > 1 } @$file_list;

        print $!stdout (@separator) if @separator;

        close $outfh if $outfile; # mirrors "if ($outfile) { open $outfh ..." above

        if ($verbose) {
            my ($unique, $distinct, $duplicate) = (0) x 3;
            for my $files (@$file_list) {
                if (@$files == 1) {
                    $unique++;
                } elsif (@$files > 1) {
                    $distinct++;
                    $duplicate += @$files - 1;
                }
            }

            $self->_display_statistics(
                unique => $unique,
                distinct => $distinct,
                duplicate => $duplicate,
                digests => [ map { $_->name } @{$!dedup->blocking} ],
                num_digests => $!dedup->count_digests,
                collisions => $!dedup->count_collisions,
            );
        }

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
