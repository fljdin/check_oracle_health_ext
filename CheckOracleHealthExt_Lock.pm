package MyLock;

our @ISA = qw(DBD::Oracle::Server);

sub init {
  my $self = shift;
  my %params = @_;
  $self->{results} = ();
  if ($params{mode} =~ /my::lock::blocking/) {
    my @results = $self->{handle}->fetchall_array(q{
      SELECT s1.username || '@' || s1.machine as blocker,
              s1.sid as blocker_sid, s1.serial# as blocker_serial,
              s2.username || '@' || s2.machine as blocked,
              decode(l1.type,'TM','DML','TX','Trans','UL','User',l1.type),
              decode(l1.lmode,0,'None',1,'Null',2,'Row-S',3,'Row-X',4,'Share',5,'S/Row-X',6,'Exclusive', l1.lmode),
              round(l1.ctime / 60) as blocker_duration, round(l2.ctime / 60) as blocked_duration
      FROM gv$lock l1, gv$session s1, gv$lock l2, gv$session s2
      WHERE s1.sid=l1.sid AND s2.sid=l2.sid
          AND l1.block=1 AND l2.request > 0
          AND l1.id1 = l2.id1
          AND l1.id2 = l2.id2
    });
    my $count = 0;
    foreach (@results) {
      $self->{results}->{$count} = \@{$_};
      $count++;
    }
  } elsif ($params{mode} =~ /my::lock::duration/) {
    my @results = $self->{handle}->fetchall_array(q{
      select count(sid) sessions, block, max(round(ctime/60)) duration 
        from gv$lock where type = 'TX' and lmode > 0 group by block
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
  if ($params{mode} =~ /my::lock::blocking/) {
    my $nb_locks = 0;
    foreach (values %{$self->{results}}) {
      my($blocker, $blocker_sid, $blocker_serial, $blocked, $lock_type,
          $lock_mode, $blocker_duration, $blocked_duration) = @{$_};

      my $level = $self->check_thresholds($blocked_duration, 15, 30);
      $self->add_nagios(
        $level, sprintf "%s ('%s,%s') is blocking %s since %s min (Total: %s min, Type: %s, Mode: %s)",
        $blocker, $blocker_sid, $blocker_serial, $blocked,
        $blocked_duration, $blocker_duration, $lock_type, $lock_mode
      );
      $nb_locks++;
    }
    if ($nb_locks == 0) {
      $self->add_nagios_ok("no persistent lock detected");
    }
    $self->add_perfdata(sprintf "nb_locks=%d", $nb_locks);

  } elsif ($params{mode} =~ /my::lock::duration/) {
    my $nb_locks = 0;
    foreach (values %{$self->{results}}) {
      my($sessions, $block, $duration) = @{$_};
      $self->add_nagios(
        $self->check_thresholds($duration, 15, 30),
        sprintf "%s %s locks detected since %s minutes", $sessions, $block > 0 ? "blocking" : "non-blocking", $duration);
      $nb_locks++;
    }
    if ($nb_locks == 0) {
      $self->add_nagios_ok("no lock detected");
    }
  } else {
    $self->add_nagios_unknown("unknown mode");
  }
}
