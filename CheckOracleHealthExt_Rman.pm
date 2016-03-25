package MyRman;

our @ISA = qw(DBD::Oracle::Server);

sub init {
  my $self = shift;
  my %params = @_;

  $self->{results} = ();
  my @results = $self->{handle}->fetchall_array(q{
      SELECT backup_type, output_bytes, round(compression_ratio,2) AS compression_ratio,
            (end_time - start_time)*24*60*60 AS time_taken,
             round(24*(sysdate-end_time),0) AS hours_in_past,
             round(24*60*(sysdate-end_time),0) AS mins_in_past,
             to_char(end_time, 'YYYY/MM/DD HH24:MI:SS') AS latest_end
      FROM (SELECT bkp.*, row_number() over (partition by backup_type order by end_time desc) num
      FROM
        (SELECT j.status , j.start_time start_time , j.end_time end_time, 
                j.output_bytes, j.compression_ratio,
                CASE WHEN (FULL_PIECES > 0) OR (INC0_PIECES > 0) THEN 'DB_FULL'
                     WHEN (INC1_PIECES > 0) THEN 'DB_INC' 
                     WHEN (ARCH_PIECES > 0) THEN 'ARCH' ELSE 'MISC' 
                END AS backup_type
         FROM V$RMAN_BACKUP_JOB_DETAILS j
         JOIN
           (SELECT /*+ RULE */ d.session_recid,
                   d.session_stamp,
                   sum(CASE WHEN d.controlfile_included = 'YES' THEN d.pieces ELSE 0 END) CF_pieces,
                   sum(CASE WHEN d.controlfile_included = 'NO' AND bs.recid IS NULL -- spfile excluded
                       AND d.backup_type||d.incremental_level = 'D' THEN d.pieces ELSE 0 END) Full_pieces,
                   sum(CASE WHEN d.backup_type||d.incremental_level IN ('D0', 'I0') THEN d.pieces ELSE 0 END) inc0_pieces,
                   sum(CASE WHEN d.backup_type||d.incremental_level = 'I1' THEN d.pieces ELSE 0 END) inc1_pieces,
                   sum(CASE WHEN d.backup_type = 'L' THEN d.pieces ELSE 0 END) arch_pieces
            FROM V$BACKUP_SET_DETAILS d
            JOIN V$BACKUP_SET s ON s.set_stamp = d.set_stamp
			LEFT JOIN V$BACKUP_SPFILE bs ON d.set_count = bs.set_count
            AND s.set_count = d.set_count
            WHERE s.input_file_scan_only = 'NO'
            GROUP BY d.session_recid,
                     d.session_stamp) x ON x.session_recid = j.session_recid
         AND x.session_stamp = j.session_stamp) bkp) bkp
      WHERE status LIKE 'COMPLETED%' and num = 1
  });
   
  my $count = 0;
  foreach (@results) {
    $self->{results}->{$count} = \@{$_};
    $count++;
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  my $bkpfound = 0;
  my $units = defined $params{units} ? $params{units} : "MB";
  my $factor = 1024 * 1024; # default MB
  if ($units eq "GB") {
    $factor = 1024 * 1024 * 1024;
  } elsif ($units eq "MB") {
    $factor = 1024 * 1024;
  } elsif ($units eq "KB") {
    $factor = 1024;
  }
  
  if ($params{mode} !~ /my::rman::(full|incr|arch)/) {
    $self->add_nagios_unknown("unknown mode"); 
    return;
  }
    
  foreach (values %{$self->{results}}) {
    my($type, $output, $ratio, $duration, $hours_in_past, $mins_in_past, $latest_end) = @{$_};
    
    if ($params{mode} =~ /my::rman::full/ and $type eq "DB_FULL") {
      $bkpfound = 1;
	  $self->add_perfdata(sprintf "\'backup_output\'=%d%s;'ratio'=%.2f", $output / $factor, $units, $ratio);
	  $self->add_perfdata(sprintf "\'backup_duration\'=%dmin", $duration / 60);
      $self->add_nagios(
        $self->check_thresholds($hours_in_past, 30, 50),
        sprintf "Last full backup : %s", $latest_end);
    } elsif ($params{mode} =~ /my::rman::incr/ and $type eq "DB_INC") {
      $bkpfound = 1;
	  $self->add_perfdata(sprintf "\'backup_output\'=%d%s;'ratio'=%.2f", $output / $factor, $units, $ratio);
	  $self->add_perfdata(sprintf "\'backup_duration\'=%dmin", $duration / 60);
      $self->add_nagios(
        $self->check_thresholds($hours_in_past, 30, 50),
        sprintf "Last incr backup : %s", $latest_end);
    } elsif ($params{mode} =~ /my::rman::arch/ and $type eq "ARCH") {
      $bkpfound = 1;
	  $self->add_perfdata(sprintf "\'backup_output\'=%d%s;'ratio'=%.2f", $output / $factor, $units, $ratio);
	  $self->add_perfdata(sprintf "\'backup_duration\'=%dsec", $duration);
      $self->add_nagios(
        $self->check_thresholds($mins_in_past, 120, 180),
        sprintf "Last arch backup : %s", $latest_end);
    }
  }
  
  if ( ! $bkpfound ) {
    $self->add_nagios_critical("No backup found");
  }
}

