requires 'mop', '0.02';
requires 'signatures', '0.07';

requires 'List::MoreUtils', '0.33';
requires 'Digest::SHA', '5.85';
recommends 'Proc::ProcessTable', '0.50';

on 'test' => sub {
    requires 'Test::Most', '0.31';
    requires 'Test::Class', '0.41';
};
