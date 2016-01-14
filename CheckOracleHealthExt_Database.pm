package MyDatabase;

our @ISA = qw(DBD::Oracle::Server);

sub init {
  my $self = shift;
  my %params = @_;
  $self->{results} = ();
  
  if ($params{mode} =~ /my::database::size/) {
    my @results = $self->{handle}->fetchall_array(q{
      SELECT NVL(type, 'TOTAL') type, SUM(bytes_used) real_bytes_used,
       SUM(real_bytes_free) real_bytes_free, SUM(real_bytes_max) real_bytes_max
      FROM (
        SELECT
             CASE WHEN a.tablespace_name IN ('SYSTEM','SYSAUX')
                  THEN 'SYSTEM' ELSE 'DATA' END type,
             a.bytes, a.maxbytes bytes_max,
             a.bytes - c.bytes_free bytes_used, c.bytes_free,
       CASE WHEN a.maxbytes = 0 OR a.maxbytes < a.bytes THEN c.bytes_free 
        ELSE c.bytes_free + (a.maxbytes - a.bytes) END real_bytes_free,
       CASE WHEN a.maxbytes = 0 OR a.maxbytes < a.bytes THEN a.bytes
        ELSE a.maxbytes END real_bytes_max
         FROM (
             SELECT a.tablespace_name, SUM(a.bytes) bytes, SUM(a.bytes) maxbytes
             FROM dba_data_files a GROUP BY tablespace_name
           ) a,
           sys.dba_tablespaces b, (
             SELECT a.tablespace_name, SUM(a.bytes) bytes_free
             FROM dba_free_space a GROUP BY tablespace_name
           ) c
         WHERE a.tablespace_name = c.tablespace_name (+)
           AND a.tablespace_name = b.tablespace_name
      ) GROUP BY ROLLUP (type)
    });
  } else if ($params{mode} =~ /my::database::activity/) {
    my @results = $self->{handle}->fetchall_array(q{
      SELECT statistic#, name, class, VALUE FROM v$sysstat WHERE name = 'DB time';
    });
  }
  my $count = 0;
  foreach (@results) {
      $self->{results}->{$count} = \@{$_};
      $count++;
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  
  if ($params{mode} =~ /my::database::size/) {
    my $units = defined $params{units} ? $params{units} : "MB";
    my $factor = 1024 * 1024; # default MB
    if ($units eq "GB") {
      $factor = 1024 * 1024 * 1024;
    } elsif ($units eq "MB") {
      $factor = 1024 * 1024;
    } elsif ($units eq "KB") {
      $factor = 1024;
    }
    
    foreach (values %{$self->{results}}) {
      my($type, $used, $free, $max) = @{$_};
      
      $self->add_perfdata(sprintf "\'%s_size\'=%.2f%s", lc $type, $max / $factor, $units);
      $self->add_perfdata(sprintf "\'%s_usage\'=%.2f%s", lc $type, $used / $factor, $units);
      if ($type eq "TOTAL") {
        $self->add_nagios_ok(sprintf(
          "Global database size is %.2f%s (%.2f%% full)",
          $used / $factor, $units, $used / $max * 100
        ));
      }
    }
  } else if ($params{mode} =~ /my::database::activity/) {
  }
}