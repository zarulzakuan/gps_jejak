package JejakKtmb;
use Mojo::Base 'Mojolicious';
use DBI;
use MongoDB;
use JSON;
use Data::Dumper;
use DateTime::Format::Strptime;
use Mojo::Redis;
use File::Basename;


# This method will run once at server start
sub startup {
  my $self = shift;
  push @{$self->static->paths} => '/home/nora/public/advert';
  push @{$self->static->paths} => '/home/nora/public/mobile';
  push @{$self->static->paths} => '/home/nora/public/mobile/assets';
  push @{$self->static->paths} => '/home/nora/public/mobile/build';
  # Load configuration from hash returned by config file
  my $config = $self->plugin('Config');

  # Configure the application
  $self->secrets($config->{secrets});


  #

  #####################
  #### Connections ####
  #####################

  # Oracle

  $self->helper(db => sub {
       my $dsn = "DBI:Oracle:sid=XE;host=47.254.193.72;port=1521";
       my $dbh = DBI->connect($dsn, "SYSTEM","jejakora1234") or die "Could not connect to database";
       return $dbh;
    });

  # Mongo

  $self->helper(mongo => sub {
        my $c = shift;
        my $DATABASE = 'jejakktmb';
        my $client = MongoDB::MongoClient->new(host => "mongodb://mongomaster:27017,mongoslave:27017,mongoslave2:27017/?replicaSet=rs0",);
        my $db = $client->get_database($DATABASE);
    });

  # Redis

  $self->helper(redis => sub {
        state $redis = Mojo::Redis->new("redis://127.0.0.1:jejakredis1234%21@127.0.0.1:6379");
    });

  #################
  #### API Helpers ####
  #################

  # find all available services

  $self->helper(all_services => sub {
        my $self = shift;
        my $sth = eval { $self->db->prepare('
          --find all available services
          select service_name from xls_all_timetable group by service_name order by service_name
        ') } || return undef;
        $sth->execute;
        return encode_json($sth->fetchall_arrayref({}));
    });

  # find all available origin based on selected service

  $self->helper(find_available_origins => sub {
        my $self = shift;
        my $selected_service = shift; # no data passed yet
        $selected_service = uc($selected_service);
        
        my $sth = eval { $self->db->prepare('
          --find all depart station for X service
          select stop_code, initcap(stop_name) as STOP_NAME from xls_all_timetable where upper(service_name) = \''.$selected_service.'\' group by stop_code, stop_name order by stop_name
        ') } || return undef;
        $sth->execute;
        my $result = $sth->fetchall_arrayref({});
        #warn Dumper $result;
        return encode_json($result);

    });

  # find all available origin based on selected service and user's current location

  $self->helper(find_available_origins_by_coord => sub {
        my $self = shift;
        my $selected_service = shift; # no data passed yet
        my $lat = shift; # no data passed yet
        my $long = shift; # no data passed yet
        $selected_service = uc($selected_service);
        $lat = $lat + 0;
        $long = $long + 0;
        my $max_distance = 100000; # 5KM
        

        # get from Timetable first

        my $sth = eval { $self->db->prepare('
              --find all depart station for X service
              select stop_code, initcap(stop_name) as STOP_NAME from xls_all_timetable where upper(service_name) = \''.$selected_service.'\' group by stop_code, stop_name order by stop_name
            ') } || return undef;
            $sth->execute;
            my $result = $sth->fetchall_arrayref({});
            
        # then get the nearest station

        my $c_stops = $self->mongo->get_collection('stops');
        my $nearest_station = $c_stops->find_one( { 'loc' =>  {'$near' => {'$geometry' => {'type' => 'Point', 'coordinates' => [$long,$lat]},'$maxDistance' => $max_distance} } }  );
        my $nearest_station_id = $nearest_station->{stid};
        #warn Dumper $result;
        my @modified_result;
        foreach my $station (@$result) {
          
          
          if ($station->{STOP_CODE} eq $nearest_station_id){
            
            $station->{SELECTED} = "true";
          }else{
            $station->{SELECTED} = "false";
          }
          push @modified_result, $station;
          
        }
        #warn Dumper \@modified_result;

        return  encode_json(\@modified_result);
        });

  

  # find all available destination based on selected origin

  $self->helper(find_available_destinations => sub {
        my $self = shift;
        my $selected_origin = shift; # no data passed yet
        my $selected_service = shift || 'KTM KOMUTER';
       
        
        my $sth = eval { $self->db->prepare('
          --find all stops departed from A
          select distinct tmtb.stop_code, initcap(tmtb.stop_name) as STOP_NAME
          from xls_all_timetable tmtA
          inner join xls_all_timetable tmtB on tmtA.train_num = tmtB.train_num and tmtA.stop_seq < tmtB.stop_seq
          where 
          tmtA.stop_code = '.$selected_origin.'
          and
          tmtA.service_name = \''.$selected_service.'\'
          order by STOP_NAME
        ') } || return undef;
        $sth->execute;
        my $result = $sth->fetchall_arrayref({});
        #warn Dumper $result;
        return encode_json($result);

    });

  # find live data

  $self->helper(find_live_data => sub {
        my $self = shift;
        my $trainnum = shift;
        my $delay = $self->redis->db->get("L".$trainnum) || 0;
        return $delay;
  });

  
  
  ########## HELPERS FOR COMMAND CENTER ##########


  # create announcement

  $self->helper(insert_announcement => sub {
        my $self = shift;
        my $text = shift;
        my $start = shift;
        my $end = shift;

        my $dt = DateTime->now;
        my $current = $dt->strftime( '%Y-%m-%d %H:%M:%S' );
        my $sth = eval { $self->db->prepare('
          --find all available services
          INSERT INTO PIS_PUBLIC_ANNOUNCEMENT (TEXT, ENDDT, STARTDT, SUBMITTED, ACTIVE) VALUES ( ?, TO_TIMESTAMP(?, \'YYYY-MM-DD HH24:MI:SS\'), TO_TIMESTAMP(?, \'YYYY-MM-DD HH24:MI:SS\'), TO_TIMESTAMP(?, \'YYYY-MM-DD HH24:MI:SS\'), ?)
        ') } || return undef;
        $sth->execute($text, $end, $start, $current, 1);
        return encode_json($sth->fetchall_arrayref({}));
    });

  # delete announcement

  $self->helper(delete_announcement => sub {
        my $self = shift;
        my $id = shift;

        my $sth = eval { $self->db->prepare('
          --find all available services
          UPDATE PIS_PUBLIC_ANNOUNCEMENT SET ACTIVE = 0 WHERE ID = ?
        ') } || return undef;
        $sth->execute($id);
        return encode_json($sth->fetchall_arrayref({}));
    });


  # update announcement

  $self->helper(update_announcement => sub {
        my $self = shift;
        my $text = shift;
        my $start = shift;
        my $end = shift;
        my $id = shift;

        my $dt = DateTime->now;
        my $current = $dt->strftime( '%Y-%m-%d %H:%M:%S' );
        my $sth = eval { $self->db->prepare('
          --find all available services
          UPDATE PIS_PUBLIC_ANNOUNCEMENT SET TEXT = ?, ENDDT = TO_TIMESTAMP(?, \'YYYY-MM-DD HH24:MI:SS\'), STARTDT = TO_TIMESTAMP(?, \'YYYY-MM-DD HH24:MI:SS\'), SUBMITTED = TO_TIMESTAMP(?, \'YYYY-MM-DD HH24:MI:SS\'), ACTIVE = ? WHERE ID = ?
        ') } || return undef;
        $sth->execute($text, $end, $start, $current, 1, $id);
        return encode_json($sth->fetchall_arrayref({}));
    });

  # get announcement for JP

  $self->helper(get_announcement => sub {
        my $self = shift;
        my $dt = DateTime->now;
        my $current = $dt->strftime( '%Y-%m-%d %H:%M:%S' );
        warn $current;
        my $sth = eval { $self->db->prepare('
          --find all available services
          SELECT  PPA.* FROM PIS_PUBLIC_ANNOUNCEMENT PPA WHERE STARTDT <= TO_DATE(?, \'YYYY-MM-DD HH24:MI:SS\') and ENDDT >= TO_DATE(?, \'YYYY-MM-DD HH24:MI:SS\') and ACTIVE = 1
        ') } || return undef;
        $sth->execute($current,$current);
        return encode_json($sth->fetchall_arrayref({}));
    });

  # get announcement for CC

  $self->helper(get_announcement_CC => sub {
        my $self = shift;
        my $dt = DateTime->now;
        my $current = $dt->strftime( '%Y-%m-%d %H:%M:%S' );
        warn $current;
        my $sth = eval { $self->db->prepare('
          --find all available services
          SELECT  PPA.* FROM PIS_PUBLIC_ANNOUNCEMENT PPA
        ') } || return undef;
        $sth->execute();
        return encode_json($sth->fetchall_arrayref({}));
    });

  
  ########## HELPERS FOR ADVERT MANAGER ##########

  # insert new advert

  $self->helper(insert_advert => sub {
        my $self = shift;
        
        my $start = shift;
        my $end = shift;
        my $desc = shift;
        my $image_path = shift;
        my $status;
        my $id = time();

        my $dt = DateTime->now;
        my $current = $dt->strftime( '%Y-%m-%d %H:%M:%S' );

        my $start_dt = $self->dtparser_dmy->parse_datetime($start);
        my $end_dt = $self->dtparser_dmy->parse_datetime($end);

        $start = $start_dt->strftime( '%Y-%m-%d %H:%M:%S' );
        $end = $end_dt->strftime( '%Y-%m-%d %H:%M:%S' );
        if (($dt > $start_dt) && ($dt < $end_dt )){
              $status = 1
            }else{
              $status = 2
            }

        my $sth = eval { $self->db->prepare('
          --find all available services
          INSERT INTO PIS_PUBLIC_ADVERT (ID, PATH, DESCRIPTION, STARTDT, ENDDT, SUBMITTED, STATUS) VALUES (?, ?, ?, TO_TIMESTAMP(?, \'YYYY-MM-DD HH24:MI:SS\'), TO_TIMESTAMP(?, \'YYYY-MM-DD HH24:MI:SS\'), TO_TIMESTAMP(?, \'YYYY-MM-DD HH24:MI:SS\'), ?)
        ') } || return undef;
        $sth->execute($id, $image_path, $desc, $start, $end, $current, $status) || return undef;
        return encode_json($sth->fetchall_arrayref({}));
    });

  # get advert

  $self->helper(get_advert => sub {
        my $self = shift;
        
        my $dt = DateTime->now;
        
        my $sth = eval { $self->db->prepare('
          --find all available services
          SELECT  PPA.*, to_char(STARTDT,\'DD-MM-YYYY HH24:MM:SS\') STARTDT_FMT, to_char(ENDDT,\'DD-MM-YYYY HH24:MM:SS\') ENDDT_FMT FROM PIS_PUBLIC_ADVERT PPA
        ') } || return undef;
        $sth->execute() || return undef;

        my $result = $sth->fetchall_arrayref({});
        
        foreach my $row (@$result){
          foreach my $key (keys %$row){
            
            my $start = $row->{STARTDT_FMT};
            my $end = $row->{ENDDT_FMT};
            my $start_datetime = $self->dtparser_dmy->parse_datetime($start);
            my $end_datetime = $self->dtparser_dmy->parse_datetime($end);

            if (($dt > $start_datetime) && ($dt < $end_datetime )){
              $row->{STATUS} = 1 if $row->{STATUS} ne 0
            }else{
              $row->{STATUS} = 2 if $row->{STATUS} ne 0
            }
            # $row->{STARTDT_FMT} = $start_datetime->strftime( '%d-%m-%Y %H:%M:%S' );
            # $row->{ENDDT_FMT} = $end_datetime->strftime( '%d-%m-%Y %H:%M:%S' );
          }
        }
        #$result = to_json( $result, { utf8 => 1} );
        return ($result)
    });


  # delete advert

  $self->helper(delete_advert => sub {
        my $self = shift;
        my $id = shift;

        $id = $id + 0;
       
        my $sth = eval { $self->db->prepare('
          --find all available services
          UPDATE PIS_PUBLIC_ADVERT SET STATUS = 0 WHERE ID = ?
        ') } || return undef;
        $sth->execute($id) || return undef;
        return encode_json($sth->fetchall_arrayref({}));
    });

  # update advert

  $self->helper(update_advert => sub {
        my $self = shift;
        my $desc = shift;
        my $start = shift;
        my $end = shift;
        my $id = shift;
        my $status = 1;


        my $dt = DateTime->now;
        my $current = $dt->strftime( '%Y-%m-%d %H:%M:%S' );
        
        
        my $start_dt = $self->dtparser_dmy->parse_datetime($start);
        my $end_dt = $self->dtparser_dmy->parse_datetime($end);

        $start = $start_dt->strftime( '%Y-%m-%d %H:%M:%S' );
        $end = $end_dt->strftime( '%Y-%m-%d %H:%M:%S' );

        if (($dt > $start_dt) && ($dt < $end_dt )){
          $status = 2;
        }

        my $sth = eval { $self->db->prepare('
          --find all available services
          UPDATE PIS_PUBLIC_ADVERT SET DESCRIPTION = ?, ENDDT = TO_TIMESTAMP(?, \'YYYY-MM-DD HH24:MI:SS\'), STARTDT = TO_TIMESTAMP(?, \'YYYY-MM-DD HH24:MI:SS\'), SUBMITTED = TO_TIMESTAMP(?, \'YYYY-MM-DD HH24:MI:SS\'), STATUS = ? WHERE ID = ?
        ') } || return undef;
        $sth->execute($desc, $end, $start, $current, $status, $id);
        return encode_json($sth->fetchall_arrayref({}));
   });

  #######################
  #### Other helpers ####
  #######################
  $self->helper(dtparser_dmy => sub {
        my $parser = DateTime::Format::Strptime->new(
          pattern => '%d-%m-%Y %H:%M:%S',
          time_zone => 'Asia/Singapore',
          on_error => 'croak',
        );
        return $parser;
      });

  $self->helper(dtparser => sub {
        my $parser = DateTime::Format::Strptime->new(
          pattern => '%F %T',
          time_zone => 'Asia/Singapore',
          on_error => 'croak',
        );
        return $parser;
      });

  $self->helper(convertSectoHHMM => sub {
    my $self = shift;
    my $seconds = shift;
    my $hours = int( $seconds / (60*60) );
    my $mins = ( $seconds / 60 ) % 60;
    return sprintf("%02d%02d", $hours,$mins);
  });

  $self->helper('render_file' => sub {
    my $c = shift;
    my %args  = @_;
    my $filepath = $args{filepath};

    unless ( -f $filepath && -r $filepath ) {
      $c->app->log->error("Cannot read file [$filepath]. error [$!]");
      return;
    }

    my $filename = $args{filename} || fileparse($filepath);
    my $status   = $args{status}   || 200;

    my $headers = Mojo::Headers->new();
    $headers->add( 'Content-Type',        'application/x-download;name=' . $filename );
    $headers->add( 'Content-Disposition', 'attachment;filename=' . $filename );
    $c->res->content->headers($headers);

    # Stream content directly from file
    $c->res->content->asset( Mojo::Asset::File->new( path => $filepath ) );
    return $c->rendered($status);
  });


  ################
  #### Router ####
  ################

  my $r = $self->routes;

  # Normal route to controller
  $r->get("/")->to(cb => sub {
        my $c = shift;
        $c->reply->static('index.html')
    });


  $r->get("/get_service")->to('Front#getService');
  $r->get("/get_origin")->to('Front#getOrigin');
  $r->get("/get_origin_by_coord")->to('Front#getOriginByCoord');
  $r->get("/get_destination")->to('Front#getDestination');
  $r->get("/get_live_schedule")->to('Main#getLiveSchedule');
  $r->get("/get_schedule")->to('Main#getSchedule');
  $r->get("/get_coord")->to('Main#getStopCoordinate');
  $r->get("/get_announcement")->to('Main#getAnnouncement');
  
  # Command Center endpoints

  $r->get("/live")->to('CC#getLive');
  $r->get("/get_data")->to('CC#getData');
  $r->get("/get_setnum")->to('CC#getSetNum');
  $r->get("/get_summary")->to('CC#getSummary');
  $r->any("/create_announcement")->to('CC#createAnnouncement');

  ## TO DO ###
  $r->get("/get_announcement_cc")->to('CC#getAnnouncementCC');
  $r->any("/delete_announcement")->to('CC#deleteAnnouncement');
  $r->any("/update_announcement")->to('CC#updateAnnouncement');

  # Advert Manager endpoints
  $r->any("/upload_advert")->to('AdMgr#uploadAdvert');
  $r->any("/get_advert")->to('AdMgr#getAdvert');
  $r->any("/delete_advert")->to('AdMgr#deleteAdvert');
  $r->any("/update_advert")->to('AdMgr#updateAdvert');
  
  
    
  $r->get("/demo")->to(cb => sub {
      my $c = shift;
      $c->reply->static('indexdemo.html')
  });

  $r->get("/advert")->to(cb => sub {
      my $c = shift;
      $c->reply->static('index_advert.html')
  });
}


1;
