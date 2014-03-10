#!/usr/bin/perl

#
# *******************************************************************************
# * Copyright (C)2014, International Business Machines Corporation and *
# * others. All Rights Reserved. *
# *******************************************************************************
# 
use Cwd;

sub checkEnv() {
  die "STREAMS_INSTALL not set\n" if(!defined($ENV{STREAMS_INSTALL}));
  die "YARN_HOME not set\n" if(!defined($ENV{YARN_HOME}));
}                                     
checkEnv();

my $instanceid="yarntest_recoveryMode";
my $st="$ENV{STREAMS_INSTALL}/bin/streamtool";
my $projdir=getcwd + "../";
my $styarn= "$projdir/../../com.ibm.streams.yarn/bin/streams-on-yarn";
my $yarncmd="$ENV{YARN_HOME}/bin/yarn";
my $startProps="--property RecoveryMode=on";

sub runCmd($$) {

  my ($cmd,$dieonerror) = @_;
  print "Running command: \"$cmd\"\n";
  my $rc = system($cmd);
  if($dieonerror && $rc !=0) {
    print "ERROR: Command Failed\n";
    cleanup();
    die "ERROR: command failed: $rc\nTest Failed.\n";
  }
  elsif( $rc != 0 ) {
    print "Ignoring previous error\n";
  }
}


sub cleanup() {
  runCmd("$st stopinstance -i $instanceid --force --immediate", 0);
  runCmd("$st rminstance -i $instanceid --noprompt", 0);
  runCmd("$styarn stop", 0);#TODO- force kill
}


cleanup();

runCmd("$st mkinstance -i $instanceid $startProps", 1);
runCmd("$styarn start", 1);
sleep 5;
runCmd("$styarn startinstance $instanceid", 1);

runCmd("$st getresource -i $instanceid", 1);

runCmd("$styarn stopinstance $instanceid", 1);
runCmd("$styarn stop", 1);
cleanup();

print "Test Passed\n";
