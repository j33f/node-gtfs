var io = require('socket.io-client');
var uuid = require('node-uuid');
var Kuzzle = function (socketUrl) {
  var
    subscribedRooms = {};
  
  if (!socketUrl || socketUrl === '') {
    console.error('Url to Kuzzle can\'t be empty');
    return false;
  }

  if (!(this instanceof Kuzzle)) {
    return new Kuzzle(socketUrl);
  }

  this.socket = io(socketUrl);
  this.bucket = {};

  /**
   * Subscribe to a filter
   * @param {String} collection
   * @param {Object} filters
   * @param {Function} callback
   */
  this.subscribe = function(collection, filters, callback) {
    var roomName = uuid.v4();

    // subscribe to feedback and map to callback function when receive a message :
    this.socket.once(roomName, function(response) {
      subscribedRooms[roomName] = response.result.roomId;
      this.socket.off(response.result.roomId);
      this.socket.on(response.result.roomId, function(data){
        callback(data);
      });
    }.bind(this));

    // create the feedback room :
    this.socket.emit('subscribe', {
      requestId: roomName,
      action: 'on',
      collection: collection,
      body: filters
    });

    return roomName;
  };

  /**
   * Unsubscribe to a room
   * @param {String} roomName
   */
  this.unsubscribe = function(roomName) {
    if (!subscribedRooms[roomName]) {
      return false;
    }
    
    // Unsubscribes from Kuzzle & closes the socket
    this.socket.emit('subscribe', {
      requestId: roomName,
      action: 'off'
    });
    
    this.socket.off(subscribedRooms[roomName]);
    
    delete subscribedRooms[roomName];
  };

  /**
   * Count subscription to a room
   * @param {String} roomName
   * @param {Function} callback
   */
  this.countSubscription = function (roomName, callback) {
    var requestId = uuid.v4();

    if (callback) {
      this.socket.once(requestId, function(response) {
        callback(response);
      });
    }

    this.socket.emit('subscribe', {
      requestId: requestId,
      action: 'count',
      body: {
        roomId: subscribedRooms[roomName]
      }
    });
  };

  /**
   * Write message to kuzzle
   * @param {String} collection
   * @param {String} action
   * @param {Object} body
   * @param {Boolean} persist
   * @param {Function} callback
   */
  this.write = function(collection, action, body, persist, callback) {
    var requestId = uuid.v4();

    if (callback) {
      this.socket.once(requestId, function(response) {
        callback(response);
      });
    }

    if (persist === undefined) {
      persist = false;
    }

    this.socket.emit('write', {
      requestId: requestId,
      action: action,
      persist: persist,
      collection: collection,
      body: body
    });
  };

/**
   * Send bulk action message to kuzzle
   * @param {String} collection
   * @param {Object} body
   * @param {Function} callback
   */
  this.bulk = function(collection, body, callback) {
    var requestId = uuid.v4();

    if (callback) {
      this.socket.once(requestId, function(response) {
        callback(response);
      });
    }

    this.socket.emit('bulk', {
      requestId: requestId,
      action: 'import',
      persist: true,
      collection: collection,
      body: body
    });
  };

/**
   * Send admin action message to kuzzle
   * @param {String} collection
   * @param {String} action
   * @param {Object} body
   * @param {Function} callback
   */
  this.admin = function(collection, action, body, callback) {
    var requestId = uuid.v4();

    if (callback) {
      this.socket.once(requestId, function(response) {
        callback(response);
      });
    }

    this.socket.emit('admin', {
      requestId: requestId,
      action: action,
      collection: collection,
      body: body
    });
  };

  /**
   * Shortcut for access to the admin controller and put a mapping to a collection
   * @param {String} collection
   * @param {Object} body
   * @param {Function} callback
   */
  this.putMapping = function (collection, body, callback) {
    this.admin(collection, 'putMapping', body, callback);
  };

  /**
   * Shortcut for access to the write controller and create a new document
   * @param {String} collection
   * @param {Object} body
   * @param {Boolean} persist
   * @param {Function} callback
   */
  this.create = function (collection, body, persist, callback) {
    this.write(collection, 'create', body, persist, callback);
  };

  /**
   * Shortcut for access to the write controller and update a new document
   * @param {String} collection
   * @param {Object} body
   * @param {Function} callback
   */
  this.update = function (collection, body, callback) {
    this.write(collection, 'update', body, true, callback);
  };

  /**
   * Shortcut for access to the write controller and delete a document by its id
   * @param {String} collection
   * @param {Object} id
   * @param {Function} callback
   */
  this.delete = function (collection, id, callback) {
    var body = {
      _id: id
    };

    this.write(collection, 'delete', body, true, callback);
  };

  /**
   * Shortcut for access to the write controller and delete documents by query
   * @param {String} collection
   * @param {Object} filters
   * @param {Function} callback
   */
  this.deleteByQuery = function (collection, filters, callback) {
    this.write(collection, 'deleteByQuery', filters, true, callback);
  };

  /**
   * Search document from Kuzzle according to a filter
   * @param {String} collection
   * @param {Object} data
   * @param {Function} callback
   */
  this.search = function(collection, data, callback) {
    this.readWithQuery(collection, data, 'search', callback);
  };

  /**
   * Get specific document from Kuzzle by id
   * @param {String} collection
   * @param {String} id
   * @param {Function} callback
   */
  this.get = function (collection, id, callback) {
    var requestId = uuid.v4();

    this.socket.once(requestId, function(response) {
      callback(response);
    });

    this.socket.emit('read', {
      requestId: requestId,
      action: 'get',
      collection: collection,
      _id: id
    });
  };

  /**
   * Count document from Kuzzle for a specific filter
   * @param {String} collection
   * @param {Object} filters
   * @param {Function} callback
   */
  this.count = function (collection, filters, callback) {
    var requestId = Math.uuid();

    this.socket.once(requestId, function(response) {
      callback(response);
    });

    this.socket.emit('read', {
      requestId: requestId,
      action: 'count',
      collection: collection,
      body: filters
    });
  };

  this.readWithQuery = function (collection, data, action, callback) {
    var requestId = uuid.v4();

    this.socket.once(requestId, function(response) {
      callback(response);
    });

    var object = {
      requestId: requestId,
      action: action,
      collection: collection
    };

    if (Object.keys(data).length > 1) {
      var attr;

      for(attr in data) {
        if (data.hasOwnProperty(attr)) {
          object[attr] = data[attr];
        }
      }
    }
    else {
      object.body = data;
    }
    this.socket.emit('read', object);
  };

 /**
   * Prepare a bulk query
   * @param {String} collection
   * @param {String} action
   * @param {Object} data
   * @param {Function} callback
   */

  this.prepare = function(collection, action, data, callback) {
    var bag = [];
    switch(action) {
      case 'write':
      case 'insert':
      case 'create':
        bag = {create: {}};
        break;
      case 'update':
        bag = {update: {_retry_on_conflict: 3}};
        break;
      // case 'delete': // TODO
      //   bag = {delete: {}};
      //   break;
    }
    bag.push(data);
    
    if (!this.bucket[collection]) {
      this.bucket[collection] = bag;
    } else {
      this.bucket[collection].concat(bag);
    }
  };
/**
   * Send a bulk query for the given collection
   * @param {String} collection
   * @param {Function} callback
   */

  this.commit = function(collection, callback) {
    var bag = JSON.parse(JSON.stringify(this.bucket[collection]));
    this.bucket = [];
    this.bulk(collection, bag, function(response) {
      response.bulkBody = bag;
      return response;
    });
  };


}

module.exports = function (socketUrl) {
  return new Kuzzle(socketUrl);
};