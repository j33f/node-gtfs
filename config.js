module.exports = {
    mongo_url: process.env.MONGOHQ_URL || 'mongodb://localhost:27017/gtfs'
  , kuzzle_url: process.env.KUZZLE_URL || 'http://localhost:7512'
//  , kuzzle_url: process.env.KUZZLE_URL || 'http://api.uat/kuzzle.io:7512'
  , agencies: [
      /* Put agency_key names from gtfs-data-exchange.com.  
      Optionally, specify a download URL to use a dataset not from gtfs-data-exchange.com */
    /* You can specify a path to the GTFS file */
    { agency_key: 'INTERCITES', path: '/home/jeff/Téléchargements/INTERCITES.gtfs.zip'}
    /* you can also specify a proj4 projection string to correct the bad formed coordinates */
    /*, {agency_key: 'lambertIIProjection', path: '/path/to/gtfs.zip', proj: '+proj=lcc +lat_1=46.8 +lat_0=46.8 +lon_0=0 +k_0=0.99987742 +x_0=600000 +y_0=2200000 +a=6378249.2 +b=6356515 +towgs84=-168,-60,320,0,0,0,0 +pm=paris +units=m +no_defs'}*/
    /* You can also specify a directory */
    /*, {dir: '/path/to/the/gtfs/dir/with/trailing/slash/'} */
    ]
}
