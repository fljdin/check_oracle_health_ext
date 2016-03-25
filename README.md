# check_oracle_health_ext

Some specifics extensions

  * Database global size (used and allocated)
  * Job state (retry or broken)
  * Locked sessions
  * RMAN backup history from controlfile
  * RMAN backup history from Catalog

How to compile

    cd /usr/lib/nagios/downloads/check_oracle_health-<version>
    find . -type f -name check_oracle_health -delete
    ./configure --prefix=/usr/lib/nagios --with-mymodules-dir=/usr/lib/nagios/downloads/ext
    make
    make install

Prerequisites for Catalog plugin

    GRANT select ON <rman>.rc_backup_set_details TO nagios;
    CREATE SYNONYM nagios.rc_backup_set_details FOR rman.rc_backup_set_details;
