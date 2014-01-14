package Data::Dedup::Engine::BlockingFactory;
# VERSION: dist tool inserts version here

package Data::Dedup::Engine::BlockingFactory::_guts;
use 5.016;
use strict;
use warnings;
use mop 0.03;
use signatures 0.07;


=head1 NAME

Data::Dedup::Engine::BlockingFactory - A role for Data::Dedup::Engine
blocking-function factories

=head1 DESCRIPTION

This is the role of an object that can generate blocking functions for an
Data::Dedup::Engine object.

=cut

role Data::Dedup::Engine::BlockingFactory {

=head1 METHODS

=over

=item all_functions

Returns instances of all the functions that can be created by this factory, in
order of preference.

This method must be implemented in classes that implement this role. It takes no
arguments and returns an arrayref that contains one or more coderefs, each of which
is a L<blocking function|Data::Dedup::Engine/blocking>.

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

L<Data::Dedup::Engine>


=head1 AUTHOR

J. Timothy King (www.JTimothyKing.com, github:JTimothyKing)

=head1 LICENSE

This software is copyright 2014 J. Timothy King.

This is free software. You may modify it and/or redistribute it under the terms of
The Apache License 2.0. (See the LICENSE file for details.)

=cut

1;
