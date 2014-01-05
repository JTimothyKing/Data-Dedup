package Dedup::Engine::BlockingFactory::_guts; ## no critic (RequireFilenameMatchesPackage)
use 5.016;
use strict;
use warnings;
use mop;
use signatures;

## no critic (ProhibitSubroutinePrototypes)
#   ...because of signatures


=head1 NAME

Dedup::Engine::BlockingFactory - A role for Dedup::Engine blocking-function factories

=head1 DESCRIPTION

This is the role of an object that can generate blocking functions for an
Dedup::Engine object.

=cut

role Dedup::Engine::BlockingFactory {

=head1 METHODS

=over

=item all_functions

Returns instances of all the functions that can be created by this factory, in
order of preference.

This method must be implemented in classes that implement this role. It takes no
arguments and returns an arrayref that contains one or more coderefs, each of which
is a L<blocking function|Dedup::Engine/blocking>.

    my @factory_methods = qw(factory_method_1 factory_method_2 factory_method_3);

    method all_functions {
        return [ map { $self->$_ } @factory_methods ];
    }

=cut

    method all_functions;

=back

=cut

}


=head1 SEE ALSO

L<Dedup::Engine>


=head1 AUTHOR

J. Timothy King (www.JTimothyKing.com, github:JTimothyKing)

=head1 LICENSE

This software is copyright 2014 J. Timothy King.

This is free software. You may modify it and/or redistribute it under the terms of
The MIT License. (See the LICENSE file for details.)

=cut


1;
