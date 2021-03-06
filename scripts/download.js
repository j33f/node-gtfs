var request = require('request')
  , exec = require('child_process').exec
  , fs = require('fs')
  , path = require('path')
  , csv = require('csv')
  , async = require('async')
  , unzip = require('unzip')
  , proj4 = require('proj4')
  , glob = require('glob')
  , downloadDir = 'downloads'
  , Db = require('mongodb').Db
  , q;

//load config.js
try {
  var config = require('../config.js');
} catch (e) {
  handleError(new Error('Cannot find config.js'));
}


var GTFSFiles = [
  {
      fileNameBase: 'agency'
    , collection: 'agencies'
  },
  {
      fileNameBase: 'calendar_dates'
    , collection: 'calendardates'
  },
  {
      fileNameBase: 'calendar'
    , collection: 'calendars'
  },
  {
      fileNameBase: 'fare_attributes'
    , collection: 'fareattributes'
  },
  {
      fileNameBase: 'fare_rules'
    , collection: 'farerules'
  },
  {
      fileNameBase: 'feed_info'
    , collection: 'feedinfos'
  },
  {
      fileNameBase: 'frequencies'
    , collection: 'frequencies'
  },
  {
      fileNameBase: 'routes'
    , collection: 'routes'
  },
  {
      fileNameBase: 'stop_times'
    , collection: 'stoptimes'
  },
  {
      fileNameBase: 'stops'
    , collection: 'stops'
  },
  {
      fileNameBase: 'transfers'
    , collection: 'transfers'
  },
  {
      fileNameBase: 'trips'
    , collection: 'trips'
  }
];

if(!config.agencies){
  handleError(new Error('No agency_key specified in config.js\nTry adding \'capital-metro\' to the agencies in config.js to load transit data'));
  process.exit();
}

//open database and create queue for agency list
Db.connect(config.mongo_url, {w: 1}, function(err, db) { 
  q = async.queue(downloadGTFS, 1);
  //loop through all agencies specified
  //If the agency_key is a URL, download that GTFS file, otherwise treat 
  //it as an agency_key and get file from gtfs-data-exchange.com
  for(var i in config.agencies) {
    var item = config.agencies[i];
    if (item.dir) {
      var format = item.format || '*.zip';
      var files = glob.sync(item.dir + format);

      files.forEach(function(item){
        var p = path.parse(item);

        config.agencies.push({
              agency_key: p.name 
            , path: item
        });

      });

      delete config.agencies[i];
    }
  }
  
  var seenPath = [];

  config.agencies.forEach(function(item){
    var ok = false;
    if(typeof(item) == 'string') {
      var agency = {
              agency_key: item
            , agency_url: 'http://www.gtfs-data-exchange.com/agency/' + item + '/latest.zip'
          }
          ok = true;
    } else if (item.url) {
      var agency = {
              agency_key: item.agency_key
            , agency_url: item.url
          }
          ok = true;
    } else if (item.path) {
      if (seenPath.indexOf(item.path) < 0) {
        var agency = {
              agency_key: item.agency_key
            , agency_path: item.path
          }
        seenPath.push(item.path);
        ok = true;
      }
    }

    if (ok) {
      if (item.proj) {
          agency.agency_proj = item.proj;
      }

      if(!agency.agency_key || (!agency.agency_url && !agency.agency_path)) {
        handleError(new Error('No URL or file path or Agency Key provided.'));
      }
      q.push(agency);
    }
  });

  q.drain = function(e) {
    console.log('All agencies completed (' + config.agencies.length + ' total)');
    db.close();
    process.exit();
  }


  function downloadGTFS(task, cb){
    var agency_key = task.agency_key
      , agency_bounds = {sw: [], ne: []}
      , agency_url = task.agency_url
      , agency_path = task.agency_path
      , agency_proj = task.agency_proj
    ;

    console.log('Starting ' + agency_key);

    async.series([
      cleanupFiles,
      downloadFiles,
      removeDatabase,
      importFiles,
      postProcess,
      cleanupFiles
    ], function(e, results){
      console.log( e || agency_key + ': Completed')
      cb();
    });


    function cleanupFiles(cb){
      //remove old downloaded file
  		exec( (process.platform.match(/^win/) ? 'rmdir /Q /S ' : 'rm -rf ') + downloadDir, function(e, stdout, stderr) {
  		  try {
    			//create downloads directory
    			fs.mkdirSync(downloadDir);
    			cb();
  		  } catch(e) {
          if(e.code == 'EEXIST') {
            cb();
          } else {
            handleError(e);
          }
        }
  		});
    }
    

    function downloadFiles(cb){
      //do download
      if (agency_url) {
        request(agency_url, processDownloadedFile).pipe(fs.createWriteStream(downloadDir + '/latest.zip'));
      } else {
        processFile(agency_path);
      }

      function processDownloadedFile(e, response, body){
        if(response && response.statusCode != 200){ cb(new Error('Couldn\'t download files')); }
        console.log(agency_key + ': Download successful');
        processFile(downloadDir + '/latest.zip');
      }

      function processFile(path) {
        fs.createReadStream(path)
          .pipe(unzip.Extract({ path: downloadDir }).on('close', cb))
          .on('error', handleError);       
      }
    }


    function removeDatabase(cb){
      //remove old db records based on agency_key
      async.forEach(GTFSFiles, function(GTFSFile, cb){
        db.collection(GTFSFile.collection, function(e, collection){
          collection.remove({ agency_key: agency_key }, {safe: true}, cb);
        });
      }, function(e){
          cb(e, 'remove');
      });
    }


    function importFiles(cb){
      //Loop through each file and add agency_key
      async.forEachSeries(GTFSFiles, function(GTFSFile, cb){
        if(GTFSFile){
          var filepath = path.join(downloadDir, GTFSFile.fileNameBase + '.txt');
          if (!fs.existsSync(filepath)) return cb();
          console.log(agency_key + ': ' + GTFSFile.fileNameBase + ' Importing data');
          db.collection(GTFSFile.collection, function(e, collection){
            csv()
              .from.path(filepath, {columns: true})
              .on('record', function(line, index){
                 //remove null values
                for(var key in line){
                  if(line[key] === null){
                    delete line[key];
                  }
                }
                
                //add agency_key
                line.agency_key = agency_key;

                //convert fields that should be int
                if(line.stop_sequence){
                  line.stop_sequence = parseInt(line.stop_sequence, 10);
                }
                if(line.direction_id){
                  line.direction_id = parseInt(line.direction_id, 10);
                }

                //make lat/lon array
                if(line.stop_lat && line.stop_lon){

                  line.loc = [parseFloat(line.stop_lon), parseFloat(line.stop_lat)];

                  // correct empty coords
                  if (isNaN(line.loc[0])) line.loc[0] = 0;
                  if (isNaN(line.loc[1])) line.loc[1] = 0;

                  // convert to epsg4326 if needed
                  if (agency_proj) {
                    line.loc = proj4(agency_proj, 'WGS84', line.loc);
                    line.stop_lon = line.loc[0];
                    line.stop_lat = line.loc[1];                    
                  }
                  
                  //Calulate agency bounds
                  if(agency_bounds.sw[0] > line.loc[0] || !agency_bounds.sw[0]){
                    agency_bounds.sw[0] = line.loc[0];
                  }
                  if(agency_bounds.ne[0] < line.loc[0] || !agency_bounds.ne[0]){
                    agency_bounds.ne[0] = line.loc[0];
                  }
                  if(agency_bounds.sw[1] > line.loc[1] || !agency_bounds.sw[1]){
                    agency_bounds.sw[1] = line.loc[1];
                  }
                  if(agency_bounds.ne[1] < line.loc[1] || !agency_bounds.ne[1]){
                    agency_bounds.ne[1] = line.loc[1];
                  }
                }

                //insert into db
                collection.insert(line, function(e, inserted) {
                  if(e) { handleError(e); }
                });
              })
              .on('end', function(count){
                cb();
              })
              .on('error', handleError);
          });
        }
      }, function(e){
        cb(e, 'import');
      });
    }


    function postProcess(cb){
      console.log(agency_key + ':  Post Processing data');

      async.series([
          agencyCenter
        , longestTrip
        , fixCoordinates
      ], function(e, results){
        cb();
      });
    }


    function agencyCenter(cb){
      var agency_center = [
          (agency_bounds.ne[0] - agency_bounds.sw[0])/2 + agency_bounds.sw[0]
        , (agency_bounds.ne[1] - agency_bounds.sw[1])/2 + agency_bounds.sw[1]
      ];

      db.collection('agencies')
        .update({agency_key: agency_key}, {$set: {agency_bounds: agency_bounds, agency_center: agency_center}}, {safe: true}, cb);
    }


    function longestTrip(cb){
      /*db.trips.find({agency_key: agency_key}).for.toArray(function(e, trips){
          async.forEach(trips, function(trip, cb){
            db.collection('stoptimes', function(e, collection){

            });
            console.log(trip);
            cb();
          }, cb);
        });
      });*/
      cb();
    }

    function fixCoordinates(cb) {
      console.log(agency_key + ':  Post Processing data - fix coordinates');
      db.collection('stops').find({agency_key: agency_key, location_type: 1}, function(e, stations){
        if (e) handleError(e);
        async.forEach(stations, function(station, cb){
          if (station.loc[0]==0 || (station.loc[1]==0)) {
            // its a station, and coordinates are wrong... lest find a stop in this station and copy its coordinates
            console.log(agency_key + ':  Post Processing data - fix coordinates - found "'+station.stop_id+'" have bad location');
            db.collection('stops').findOne({agency_key: agency_key, parent_station: station.stop_id}, function(e, stop){
              if (e) handleError(e);
              sation.loc = stop.loc;
              db.collection('stops').update({_id: station._id}, {$set: {loc: station.loc}}, {safe: true}, function(e) {
                if (e) handleError(e);
                console.log(agency_key + ':  Post Processing data - fix coordinates - found "'+station.stop_id+'" have bad location : location fixed');
              });
            });
          }
          //cb();
        }, cb);
      });
      //cb();
    }
  }
});


function handleError(e) {
  console.error(e || 'Unknown Error');
  process.exit(1)
};
