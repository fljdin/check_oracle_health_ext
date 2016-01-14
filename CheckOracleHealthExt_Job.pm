package MyJob;

our @ISA = qw(DBD::Oracle::Server);

sub init {
  my $self = shift;
  my %params = @_;
  
  if ($params{mode} =~ /my::job::refresh/) {
    my @results = $self->{handle}->fetchall_array(q{
      select 
        schema_user, what, to_char(last_date, 'YYYY/MM/DD HH24:MI:SS'), 
        failures, case broken when 'Y' then 1 else 0 end broken,
        trunc(24 * 60 * 60 * (sysdate - last_date)) as sec_gap 
      from dba_jobs where lower(what) like '%dbms_refresh.refresh%'
    });
    my $count = 0;
    foreach (@results) {
      $self->{results}->{$count} = \@{$_};
      $count++;
    }
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  my $errors = 0;
  
  if ($params{mode} =~ /my::job::refresh/) {    
    foreach (values %{$self->{results}}) {
      my($schema, $what, $last_executed, $failures, $broken, $sec_gap) = @{$_};
      
      # grap materialized view name from "dbms_refresh.refresh" syntax
      $what =~ /refresh\('([^\(]+)'\)/;
      my $view = $1; $view =~ s/"//g;
      
      if ($broken > 0) {
        $self->add_nagios_critical(sprintf "Job is broken since %s for %s view", $last_executed, $view);
		$errors++;
      } else {
        my $status = $self->check_thresholds($failures, 5, 15);
        $self->add_nagios($status, sprintf "Last job execution for %s view : %s (%d attemps)", $view, $last_executed, $failures);
		$errors++ if ($status > 0);
      }
    }
    
    # remove unnecessary messages from running jobs
    $self->{nagios}->{messages}->{$ERRORS{OK}} = ();
    
    # test if "results" hash contains no row
    if ((scalar keys %{$self->{results}}) == 0) {
      $self->add_nagios_ok("No refresh job has been configured");
    } elsif ($errors == 0) {
      $self->add_nagios_ok("All refresh jobs are running well")
    }
  }
}