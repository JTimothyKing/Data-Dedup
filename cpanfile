requires 'mop', '0.02';
requires 'signatures', '0.07';

requires 'CLI::Startup', '0.08';
requires 'List::MoreUtils', '0.33';
requires 'Digest::SHA', '5.85';

on 'test' => sub {
    requires 'Test::Most', '0.31';
    requires 'Test::Class', '0.41';
    requires 'Test::MockObject', '1.20120301';
};
