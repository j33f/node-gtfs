/*global
  node
*/
var request = require('request')
  , exec = require('child_process').exec
  , fs = require('fs')
  , path = require('path')
  , csv = require('csv')
  , Kuzzle = require('node-kuzzle')
  , kuzzle
  , async = require('async')
  , unzip = require('unzip')
  , proj4 = require('proj4')
  , glob = require('glob')
  , downloadDir = 'downloads'
  , sleep = require('sleep-async')()
  , Db = require('mongodb').Db
  , esqbuilder = require('elasticsearch-query-builder')
  , esQuery = function(query) {
    var theQuery = {};
    for (var i in query) {
      switch (typeof query[i]) {
        case 'string':
          esqbuilder.stringCriteria(query[i], theQuery, i);
          break;
        case 'object':
          if (Array.isArray(query[i])) {
            var c = {};
            for (var j in query[i]) {
              c[j] = true;
            }
            esqbuilder.selectCriteria(c, theQuery, i);
          } else {
            esqbuilder.selectCriteria(query[i], theQuery, i);
          }
          break;
      }
    }
    return theQuery;
  }
  , q
  , writes = 0 // how many writes are been made and not answered
;

Object.defineProperty(global, '__stack', {
  get: function(){
    var orig = Error.prepareStackTrace;
    Error.prepareStackTrace = function(_, stack){ return stack; };
    var err = new Error;
    Error.captureStackTrace(err, arguments.callee);
    var stack = err.stack;
    Error.prepareStackTrace = orig;
    return stack;
  }
});

Object.defineProperty(global, '__line', {
  get: function(){
    return __stack[1].getLineNumber();
  }
});

//load config.js
try {
  var config = require('../config.js');
} catch (e) {
  throw new Error('Cannot find config.js');
}


var GTFSFiles = [
  {
      fileNameBase: 'agency'
    , collection: 'agencies'
    , mapping: {
        agency_id :              {type: 'string'}
        , agency_name :          {type: 'string'}
        , agency_key :           {type: 'string'}
        , agency_url :           {type: 'string'}
        , agency_timezone :      {type: 'string'}
        , agency_lang :          {type: 'string'}
    }
  },
  {
      fileNameBase: 'calendar_dates'
    , collection: 'calendardates'
    , mapping: {
        service_id :          {type: 'string'}
        , agency_key :        {type: 'string'}
        , date :              {type: 'integer'}
        , exception_type :    {type: 'integer'}
    }
  },
  {
      fileNameBase: 'calendar'
    , collection: 'calendars'
    , mapping: {
        service_id :          {type: 'string'}
        , agency_key :        {type: 'string'}
        , monday :            {type: 'integer'} 
        , tuesday :           {type: 'integer'} 
        , wednesday :         {type: 'integer'} 
        , thursday :          {type: 'integer'} 
        , friday :            {type: 'integer'} 
        , saturday :          {type: 'integer'} 
        , sunday :            {type: 'integer'} 
        , start_date :        {type: 'integer'} 
        , end_date :          {type: 'integer'} 
    }
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
    , mapping: {
        route_id :                    {type: 'string'}
        , agency_id :                 {type: 'string'}
        , agency_key :                {type: 'string'}
        , route_short_name :          {type: 'string'}
        , route_long_name :           {type: 'string'}
        , route_desc :                {type: 'string'}
        , route_type :                {type: 'string'}
        , route_url :                 {type: 'string'}
        , route_color :               {type: 'string'}
        , route_text_color :          {type: 'string'}
    }
  },
  {
      fileNameBase: 'stop_times'
    , collection: 'stoptimes'
    , mapping: {
          trip_id :                 {type: 'string'}
        , agency_key :              {type: 'string'}
        , arrival_time :            {type: 'string'}
        , departure_time :          {type: 'string'}
        , stop_id :                 {type: 'string'}
        , stop_sequence :           {type: 'integer'}
        , stop_headsign :           {type: 'string'}
        , pickup_type :             {type: 'string'}
        , drop_off_type :           {type: 'string'}
        , shape_dist_traveled :     {type: 'string'}
    }
  },
  {
      fileNameBase: 'stops'
    , collection: 'stops'
    , mapping: {
        stop_id :         {type: 'string'}
      , agency_key :      {type: 'string'}
      , stop_name :       {type: 'string'}
      , stop_desc :       {type: 'string'}
      , stop_lat :        {type: 'float'}
      , stop_lon :        {type: 'float'}
      , zone_id :         {type: 'string'}
      , stop_url:         {type: 'string'}
      , location_type:    {type: 'string'}
      , parent_station :  {type: 'string'}
      , loc :             {type: 'geo_point'}

    }
  },
  {
      fileNameBase: 'transfers'
    , collection: 'transfers'
    , mapping: {
        from_stop_id :        {type: 'string'}
        , agency_key :        {type: 'string'}
        , to_stop_id :        {type: 'string'}
        , transfer_type :     {type: 'string'}
        , min_transfer_time : {type: 'string'}
    }
  },
  {
      fileNameBase: 'trips'
    , collection: 'trips'
    , mapping: {
        route_id :        {type: 'string'}
      , agency_key :      {type: 'string'}
      , service_id :      {type: 'string'}
      , trip_id :         {type: 'string'}
      , trip_headsign :   {type: 'string'}
      , direction_id :    {type: 'string'}
      , block_id :        {type: 'string'}
      , shape_id :        {type: 'string'}
    }
  }
];
if(!config.agencies){
  throw new Error('No agency_key specified in config.js\nTry adding \'capital-metro\' to the agencies in config.js to load transit data');
  process.exit();
}

try {
  kuzzle = Kuzzle.init(config.kuzzle_url);
} catch (e) {
  console.log('Pb with Kuzzle');
  throw e;
}
//open database and create queue for agency list
//Db.connect(config.mongo_url, {w: 1}, function(err, db) { 
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
        throw new Error('No URL or file path or Agency Key provided.');
      }
      q.push(agency);
    }
  });

  q.drain = function(e) {
    console.log('All agencies completed (' + config.agencies.length + ' total)');
    //db.close();
    kuzzle.close();
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
            throw e;
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
          .on('error', handleerror);       
      }
    }


    function removeDatabase(cb){
      //remove old db records based on agency_key
      async.forEach(GTFSFiles, function(GTFSFile, cb){
        kuzzle.deleteByQuery(GTFSFile.collection, esQuery({agency_key: agency_key}), function(error, result){
          if (error) {
            console.log('ERROR REMOVE');
            throw error;
          } else {
            console.log('**** REMOVED ', GTFSFile.collection, agency_key);
            cb();
          }
        });
      }, function(e){
          cb(e, 'remove');
      });
    }


    function importFiles(cb){
      //Loop through each file and add agency_key
      var lines = [{create: {}}];
      function doImportFile(GTFSFile, cb) {
        var filepath = path.join(downloadDir, GTFSFile.fileNameBase + '.txt');
        if (!fs.existsSync(filepath)) return cb();
        console.log(agency_key + ': ' + GTFSFile.fileNameBase + ' Importing data');
        //db.collection(GTFSFile.collection, function(e, collection){
          csv()
            .from.path(filepath, {columns: true})
            .on('record', function(line, index){
              if (!Array.isArray(line)) {
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
                //console.log('INSERSION: ',line);
                // writes++;
                // kuzzle.create(GTFSFile.collection, line, true, function(inserted) {
                //   if (inserted.error) {
                //     console.log('INSERSION ERROR', GTFSFile.fileNameBase, line);
                //     throw (inserted.error);
                //   }
                //   writes--;
                // });
                
                //lines.push(line);

                kuzzle.prepare(GTFSFile.collection, 'insert', line);
              }
            })
            .on('end', function(count){
              writes++
              // kuzzle.bulk(GTFSFile.collection, lines, function(inserted) {
              //   if (inserted.error) {
              //     console.log('INSERSION ERROR', GTFSFile.fileNameBase, lines);
              //     throw (inserted.error);
              //   }
              //   writes--;
              //   console.log(GTFSFile.collection, 'finished !');
              // });
              kuzzle.commit(GTFSFile.collection, function(error, response) {
                //console.log(error,response);
                if (error) {
                  console.log('INSERSION ERROR', GTFSFile.fileNameBase, lines);
                  throw (inserted.error);
                }
                writes--;
                console.log(GTFSFile.collection, 'finished !');
              });
              cb();
            })
            .on('error', handleerror)
        ;        
      }
      async.forEachSeries(GTFSFiles, function(GTFSFile, cb){
        if(GTFSFile){
          //create the collection and add the mapping if there is a mapping
          if (GTFSFile.mapping) {
            kuzzle.admin(GTFSFile.fileNameBase, 'deleteCollection', null, function(response){
              //console.log('deleteCollection',response);
              var mapping = {properties: GTFSFile.mapping};   
              kuzzle.putMapping(GTFSFile.fileNameBase, mapping, function(response){
                //console.log('putMapping',response);
                doImportFile(GTFSFile, cb);
              });
            });
          } else {
            // no mapping, just import the file
            doImportFile(GTFSFile, cb);
          }
         // doImportFile(GTFSFile, cb);
        }
      }, function(e){
        cb(e, 'import');
      });
    }


    function postProcess(cb){
      console.log(agency_key + ':  Post Processing data');

      console.log(agency_key + ': ....awaiting...');

      function doPostProcess(cb) {
//        sleep.sleep(2000, function(){
          console.log('Starting post processing');
          async.series([
              agencyCenter
            , longestTrip
            , fixCoordinates
          ], function(e, results){
            cb();
          });
//        });
      }

      var await = setInterval(function(){
        if (writes==0) {
          clearInterval(await);
          console.log('...lets go !');
          doPostProcess(cb);
        } else {
          console.log('...', writes, 'to go...');
        }
      }, 1000);
    }


    function agencyCenter(cb){
      console.log(agency_key + ':  Post Processing data - agencyCenter');
      var agency_center = [
          (agency_bounds.ne[0] - agency_bounds.sw[0])/2 + agency_bounds.sw[0]
        , (agency_bounds.ne[1] - agency_bounds.sw[1])/2 + agency_bounds.sw[1]
      ];

      // db.collection('agencies')
      //   .update({agency_key: agency_key}, {$set: {agency_bounds: agency_bounds, agency_center: agency_center}}, {safe: true}, cb);
      //var query = esQuery({agency_key: agency_key});
      kuzzle.search('agencies', {query: {match: {agency_key: agency_key}}}, function(_error, _response) {
        //console.log(_error, _response);
        if (_error) {
          console.log('agencyCenter ERROR');
          throw _error;
        }
        if (_response.hits.total) {
          var _id = _response.hits.hits[0]._id;
          kuzzle.update('agencies', {_id: _id, agency_bounds: agency_bounds, agency_center: agency_center}, cb);
        } else {
          console.log('Post Processing > Agency Center', agency_key, 'not found...');
          cb();
        }
      });
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
//      db.collection('stops').find({agency_key: agency_key, location_type: 1}, function(e, stations){
      var query = {
        filter: {
          and: [
              {term: {agency_key: agency_key}}
            , {term: {location_type: 1}}
          ]
        }
      };
      kuzzle.search('stops', query, function(_error, _stations) {
        var stations = _stations.hits.hits;
        if (_error) throw {e:_error, l: __line};
        async.forEach(stations, function(station, cb){
          if (station.loc[0]==0 || (station.loc[1]==0)) {
            // its a station, and coordinates are wrong... lest find a stop in this station and copy its coordinates
            console.log(agency_key + ':  Post Processing data - fix coordinates - found "'+station.stop_id+'" have bad location');
            //db.collection('stops').findOne({agency_key: agency_key, parent_station: station.stop_id}, function(e, stop){
            var query = {filter: {AND: [{term: {agency_key: agency_key}}, {term: {parent_station: station.stop_id}}]}};
            kuzzle.search('stops', query, function(stops){
              if (stops.error) throw {e:stops.error, l: __line};
              var stop = stops[0];
              sation.loc = stop.loc;
              //db.collection('stops').update({_id: station._id}, {$set: {loc: station.loc}}, {safe: true}, function(e) {
              kuzzle.update('stops', {_id: station._id, loc: stop.loc}, function(response) {
                if (response.error) throw {e:response.error, l: __line};
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
//});


function handleerror(e) {
  throw e;
  console.error(e || 'Unknown Error');
  process.exit(1)
};
