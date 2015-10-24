#!/usr/local/bin/perl
use File::Glob ':globally';
use JSON;

my %eventCounter;
my @files = <*.json>;
foreach $file (@files) {
  print "Processing $file...\n";
  open(my $in, "<$file");
  while (<$in>) {
    $obj = from_json($_);
    my $eventType = $obj->{"type"};
    if (exists($eventCounter{$eventType})) {
      $eventCounter{$eventType} ++;
    } else {
      $eventCounter{$eventType} = 1;
    }
  }  
  close($in);
}

foreach $key (keys %eventCounter) {
  print $key, ": ", $eventCounter{$key}, "\n";
}
