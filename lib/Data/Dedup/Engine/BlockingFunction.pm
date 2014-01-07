package Data::Dedup::Engine::BlockingFunction::_guts;
use 5.016;
use strict;
use warnings;
use mop 0.03;
use signatures 0.07;


=head1 NAME

Data::Dedup::Engine::BlockingFunction - A Data::Dedup::Engine blocking function

=head1 DESCRIPTION

This is the class of an object that can serve as a blocking function for an
Data::Dedup::Engine object. It is callable, like any other function, but also
supports informational attributes that can be queried, especially for UI.

=cut

class Data::Dedup::Engine::BlockingFunction {

    has $!impl is required;

    method impl is overload('&{}') {
        return $!impl;
    }


    has $!class is ro;
    has $!id is ro;
    has $!name is ro;

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
