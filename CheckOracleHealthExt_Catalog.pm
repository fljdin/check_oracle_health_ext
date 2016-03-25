package MyCatalog;

our @ISA = qw(DBD::Oracle::Server);

sub init {
  my $self = shift;
  my %params = @_;
  
  if (! defined $params{name}) {
    return;
  }
  
  if ($params{mode} =~ /my::catalog::(full|incr|arch)/) {
   my $mode = 'D';
   $mode = 'I' if ($params{mode} =~ /incr/);
   $mode = 'L' if ($params{mode} =~ /arch/);
    
   my($output, $duration, $ratio, $hours_in_past, $mins_in_past, $latest_end) =
	  $self->{handle}->fetchrow_array(q{
        SELECT * FROM (
        SELECT output_bytes, round(elapsed_seconds) AS duration,
        	   round(compression_ratio,2) AS compression_ratio,
        	   round(24*(sysdate-completion_time),0) AS hours_in_past,
               round(24*60*(sysdate-completion_time),0) AS mins_in_past,
        	   to_char(completion_time, 'YYYY/MM/DD HH24:MI:SS') AS latest_end
          FROM rc_backup_set_details 
         WHERE db_name = ?
           AND backup_type = ? AND status = 'A'
           AND (controlfile_included = 'NONE' 
		    OR (controlfile_included = 'BACKUP' AND pieces > 1))
         ORDER BY completion_time desc
        ) WHERE rownum = 1
      }, $params{name}, $mode);
	$self->{output} = $output;
	$self->{duration} = $duration;
	$self->{ratio} = $ratio;
	$self->{hours_in_past} = $hours_in_past;
	$self->{mins_in_past} = $mins_in_past;
	$self->{latest_end} = $latest_end;
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  my $units = defined $params{units} ? $params{units} : "MB";
  my $factor = 1024 * 1024; # default MB
  if ($units eq "GB") {
    $factor = 1024 * 1024 * 1024;
  } elsif ($units eq "MB") {
    $factor = 1024 * 1024;
  } elsif ($units eq "KB") {
    $factor = 1024;
  }
  
  if (! defined $params{name}) {
    $self->add_nagios_unknown("Please provide --name option"); 
    return;
  }
  
  if ($params{mode} !~ /my::catalog::(full|incr|arch)/) {
    $self->add_nagios_unknown(sprintf "Unknown mode (%s)", $params{mode}); 
    return;
  }
  
  if (! defined $self->{latest_end}) {
    $self->add_nagios_critical(sprintf "No backup found (%s)", $params{name});
	return;
  }
  
  if ($params{mode} =~ /my::catalog::full/) {
    $self->add_perfdata(sprintf "\'backup_output\'=%d%s;'ratio'=%.2f", $self->{output} / $factor, $units, $self->{ratio});
    $self->add_perfdata(sprintf "\'backup_duration\'=%dmin", $self->{duration} / 60);
    $self->add_nagios(
      $self->check_thresholds($self->{hours_in_past}, 30, 50),
      sprintf "Last full backup : %s", $self->{latest_end});
  } elsif ($params{mode} =~ /my::catalog::incr/) {
    $self->add_perfdata(sprintf "\'backup_output\'=%d%s;'ratio'=%.2f", $self->{output} / $factor, $units, $self->{ratio});
    $self->add_perfdata(sprintf "\'backup_duration\'=%dsec", $self->{duration} / 60);
    $self->add_nagios(
      $self->check_thresholds($self->{hours_in_past}, 30, 50),
      sprintf "Last incr backup : %s", $self->{latest_end});
  }elsif ($params{mode} =~ /my::catalog::arch/) {
    $self->add_perfdata(sprintf "\'backup_output\'=%d%s;'ratio'=%.2f", $self->{output} / $factor, $units, $self->{ratio});
    $self->add_perfdata(sprintf "\'backup_duration\'=%dsec", $self->{duration});
    $self->add_nagios(
      $self->check_thresholds($self->{mins_in_past}, 120, 180),
      sprintf "Last arch backup : %s", $self->{latest_end});
  }
}