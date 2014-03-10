#!/usr/bin/perl

#
# *******************************************************************************
# * Copyright (C)2014, International Business Machines Corporation and *
# * others. All Rights Reserved. *
# *******************************************************************************
# 
use Cwd;



#tests adding and removing a host which is not in the yarn cluster
my $host = $ARGV[0];
if( ! $host ) {
  die "No host specified. Please specify a host to add\n";
}


sub checkEnv() {
  die "STREAMS_INSTALL not set\n" if(!defined($ENV{STREAMS_INSTALL}));
  die "YARN_HOME not set\n" if(!defined($ENV{YARN_HOME}));
}                                     
checkEnv();

my $instanceid="yarntest_addBadHost";
my $st="$ENV{STREAMS_INSTALL}/bin/streamtool";
my $projdir=getcwd + "../";
my $styarn= "$projdir/../../com.ibm.streams.yarn/bin/streams-on-yarn";
my $yarncmd="$ENV{YARN_HOME}/bin/yarn";
my $startProps="";

sub runCmd($$) {

  my ($cmd,$dieonerror) = @_;
  print "Running command: \"$cmd\"\n";
  my $rc = system($cmd);
  if( $dieonerror == -1 && $rc == 0) {
    print "ERROR: Command Passed. Supposed to fail.\"$cmd\"\n";
    cleanup();
    die "ERROR: command passed when it was supposed to fail: $rc, \"$cmd\". \nTest Failed.\n";    
  }
  if($dieonerror == 1 && $rc !=0) {
    print "ERROR: Command Failed: \"$cmd\"\n";
    cleanup();
    die "ERROR: command failed: $rc, \"$cmd\"\nTest Failed.\n";
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

runCmd("$styarn addhost $instanceid $host", -1);

runCmd("$st getresource -i $instanceid", 1);

runCmd("$st getresource -i $instanceid", 1);

runCmd("$styarn stopinstance $instanceid", 1);
runCmd("$styarn stop", 1);
cleanup();

print "Test Passed\n";
