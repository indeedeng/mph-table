#!/usr/bin/perl

use strict;
use warnings;

use Tie::IxHash;

sub new_ixhash_ref {
  my %h;
  tie(%h, 'Tie::IxHash');
  return \%h;
}

sub min {
  return $_[1] if not defined $_[0];
  return $_[0] > $_[1] ? $_[1] : $_[0];
}

sub max {
  return $_[1] if not defined $_[0];
  return $_[0] < $_[1] ? $_[1] : $_[0];
}

my $epsilon = 1e-3;
my $num_keys = 1;
my %sizes;
my %reads;
my %writes;
my $tab;

tie(%sizes, 'Tie::IxHash');
tie(%reads, 'Tie::IxHash');
tie(%writes, 'Tie::IxHash');

while (<>) {
  if (/^-+ generating benchmark data: (\d+)/) {
    $num_keys = $1;
  } elsif (/^-+ write .*\b([^\/_.]+)_([^\/_.]+)\.(\w+)/) {
    $writes{$3} //= new_ixhash_ref();
    $writes{$3}{$1} //= new_ixhash_ref();
    $tab = \$writes{$3}{$1}{$2};
  } elsif (/^-+ read .*\b([^\/_.]+)_([^\/_.]+)\.(\w+)/) {
    $reads{$3} //= new_ixhash_ref();
    $reads{$3}{$1} //= new_ixhash_ref();
    $tab = \$reads{$3}{$1}{$2};
  } elsif (/Elapsed.*time.*\b(\d+):([.\d]+)/) {
    $$tab = min($$tab, $1 * 60 + $2);
  } elsif (/^-+ size .*\b([^\/_.]+)_([^\/_.]+)\.(\w+)\s+(\d+)/) {
    $sizes{$3} //= new_ixhash_ref();
    $sizes{$3}{$1} //= new_ixhash_ref();
    $sizes{$3}{$1}{$2} = min($sizes{$3}{$1}{$2}, int($4/(1024*1024)));
  }
}

my %impls;
my %stores;
my %variants;
tie(%impls, 'Tie::IxHash');
tie(%stores, 'Tie::IxHash');
tie(%variants, 'Tie::IxHash');
for my $impl (keys %sizes) {
  $impls{$impl}++;
  for my $store (keys %{$sizes{$impl}}) {
    $stores{$store}++;
    $variants{$store} //= new_ixhash_ref();
    $variants{$store}{$_}++ for keys %{$sizes{$impl}{$store}};
  }
}

sub print_metric {
  my $metric = shift;
  my $f = shift || sub {$_[0]};
  for my $store (keys %stores) {
    print "$store\t".join("\t", keys %{$variants{$store}})."\n";
    for my $impl (keys %impls) {
      print "$impl";
      for my $variant (keys %{$variants{$store}}) {
        my $val = $$metric{$impl}{$store}{$variant};
        print "\t".(defined $val ? $f->($val) : "");
      }
      print "\n";
    }
    print "\n";
  }
}

print "---- sizes\n";
print_metric(\%sizes);
print "---- writes\n";
print_metric(\%writes, sub {int($num_keys/(1000*max($_[0],$epsilon)/60))});
print "---- reads\n";
print_metric(\%reads, sub {$_[0]*1000000/$num_keys});
