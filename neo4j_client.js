/**
 * Simple wrapper/helpers for the Neo4j NPM client.  Server only.
 */

//var Neo4jNpm = Npm.require('redis');
var Neo4jNpm = Npm.require('neo4j');
var UrlNpm = Npm.require('url');

var INDEX_NAME = 'nodes';
var INDEX_KEY = 'type'; 
var INDEX_VAL = 'item';

Neo4jInternals.NpmModule = Neo4jNpm;

Neo4jClient = function (url, options) {
  var self = this;
  options = options || {};

  var parsedUrl = UrlNpm.parse(url);
  var host = parsedUrl.hostname || '127.0.0.1';
  var port = parseInt(parsedUrl.port || '7474');
  var db_url = host + ':' + port;
  db_url = 'localhost:7474';

  self._connection = new Neo4jNpm.GraphDatabase(db_url);
};

/*
    findOne()
    find()
    insert({name: text}
    update(this._id, {$set: {name: value}})
    remove(this._id)
*/

Neo4jClient.prototype.subscribeKeyspaceEvents = function (callback, listener) {
  var self = this;

  self._connection.on("pmessage", function (pattern, channel, message) {
    var colonIndex = channel.indexOf(":");
    if (channel.indexOf("__keyspace@") != 0 || colonIndex == 0) {
      Meteor._debug("Unrecognized channel: " + channel);
      return;
    }
    var key = channel.substr(colonIndex+1);
    listener(key, message);
  });
  self._connection.psubscribe("__keyspace@*", callback);
};


Neo4jClient.prototype.publish = function (channel, message, callback) {
  var self = this;

  console.log("#### publish called");
 // self._connection.publish(channel, message, Meteor.bindEnvironment(callback));
};

/*
Neo4jClient.prototype.findCandidateKeys = function (collectionName, matcher, callback) {
  var self = this;

  // Special case the single-document matcher
  // {"_paths":{"_id":true},"_hasGeoQuery":false,"_hasWhere":false,"_isSimple":true,"_selector":{"_id":"XhjyfgEbYyoYTiABX"}}
  var simpleKeys = null;
  if (!matcher._hasGeoQuery && !matcher._hasWhere && matcher._isSimple) {
    var keys = _.keys(matcher._selector);
    if (keys.length == 1 && keys[0] === "_id") {
      var selectorId = matcher._selector._id;
      if (typeof selectorId === 'string') {
        simpleKeys = [collectionName + "//" + selectorId];
      }
    }
  }

  if (simpleKeys === null) {
    self._connection.keys(collectionName + "//*", Meteor.bindEnvironment(callback));
  } else {
    callback(null, simpleKeys);
  }
};
*/
/* 'getNodeById', 'getIndexedNodes', 'createNode', 'save', 'index' */

Neo4jClient.prototype.getNodeById = function (id, callback) {
  var self = this;

  self._connection.getNodeById(id, Meteor.bindEnvironment(function(err, node) {
      if(err) return callback(err); 
      callback(null, node);
  }));
};

// This is wrong, should be all the index values as args, or make a getAll function
Neo4jClient.prototype.getIndexedNodes = function (index, callback) {
  var self = this;

  self._connection.getIndexedNodes(INDEX_NAME, INDEX_KEY, INDEX_VAL, Meteor.bindEnvironment(function(err, nodes) {
      if(err) return callback(err); 
      
      callback(null, nodes);
  }));
};

Neo4jClient.prototype.createNode = function (data, callback) {
  var self = this;
  var node = self._connection.createNode(data);
  
  node.save(function(err) {
    if(err) return callback(err);
    console.log("User creation data2:");

    node.index(INDEX_NAME, INDEX_KEY, INDEX_VAL, function(err) {
        console.log("User creation data3:");
        if(err) return callback(err);
        Meteor.bindEnvironment(callback(null, user));
    })
  });  
};

/*
Neo4jClient.prototype.mget = function (keys, callback) {
  var self = this;

  if (!keys.length) {
    // mget is fussy about empty keys array
    callback(null, []);
    return;
  }

  // XXX Strip any null values from results?
  self._connection.mget(keys, Meteor.bindEnvironment(callback));
};

Neo4jClient.prototype.matching = function (pattern, callback) {
  var self = this;

  self._connection.keys(pattern, Meteor.bindEnvironment(function (err, result) {
    if (err) {
      Meteor._debug("Error listing keys: " + err);
      callback(err, null);
    } else {
      self.mget(result, callback);
    }
  }));
};
*/

_.each(NEO4J_COMMANDS_HASH, function (method) {
  Neo4jClient.prototype[method] = function (/* arguments */) {
    var self = this;
    var args = _.toArray(arguments);
    var cb = args.pop();

    if (_.isFunction(cb)) {
      args.push(Meteor.bindEnvironment(function (err, result) {
        // Mongo returns undefined here, our Neo4j binding returns null
        // XXX remove this when we change our mind back to null.
        if (result === null) {
          result = undefined;
        }
        // sometimes the result is a vector of values (like multiple hmget)
        if (_.isArray(result)) {
          result = _.map(result, function (value) {
            return value === null ? undefined : value;
          });
        }
        cb(err, result);
      }));
    } else {
      args.push(cb);
    }

    var ret = self._connection[method].apply(self._connection, args);
    // Replace null with undefined as redis npm client likes to return null
    // when the value is absent. To be consistent with other behavior we
    // prefer undefined as absence of value.
    // XXX remove this when we change our mind back to null.
    return ret === null ? undefined : ret;
  };
});

/*
// XXX: Remove (in favor of default implementation?)
Neo4jClient.prototype.hgetall = function (key, callback) {
  var self = this;

  self._connection.hgetall(key, Meteor.bindEnvironment(function (err, result) {
    // Mongo returns undefined here, our Neo4j binding returns null
    if (result === null) {
      result = undefined;
    }
    callback(err, result);
  }));
};

Neo4jClient.prototype.del = function (keys, callback) {
  var self = this;

  self._connection.del(keys, Meteor.bindEnvironment(callback));
};

Neo4jClient.prototype.get = function (key, callback) {
  var self = this;

  self._connection.get(key, Meteor.bindEnvironment(function (err, res) {
    // Mongo returns undefined here, our Neo4j binding returns null
    if (res === null)
      res = undefined;
    callback(err, res);
  }));
};

Neo4jClient.prototype.set = function (key, value, callback) {
  var self = this;

  self._connection.set(key, value, Meteor.bindEnvironment(callback));
};

Neo4jClient.prototype.getConfig = function (key, callback) {
  var self = this;

  self._connection.config('get', key, Meteor.bindEnvironment(callback));
};

Neo4jClient.prototype.setConfig = function (key, value, callback) {
  var self = this;

  self._connection.config('set', key, value, Meteor.bindEnvironment(callback));
};

Neo4jClient.prototype.incr = function (key, callback) {
  var self = this;

  self._connection.incr(key, Meteor.bindEnvironment(callback));
};

Neo4jClient.prototype.incrby = function (key, delta, callback) {
  var self = this;

  self._connection.incrby(key, delta, Meteor.bindEnvironment(callback));
};

Neo4jClient.prototype.incrbyfloat = function (key, delta, callback) {
  var self = this;

  self._connection.incrbyfloat(key, delta, Meteor.bindEnvironment(callback));
};

Neo4jClient.prototype.decr = function (key, callback) {
  var self = this;

  self._connection.decr(key, Meteor.bindEnvironment(callback));
};

Neo4jClient.prototype.decrby = function (key, delta, callback) {
  var self = this;

  self._connection.decrby(key, delta, Meteor.bindEnvironment(callback));
};

Neo4jClient.prototype.append = function (key, suffix, callback) {
  var self = this;

  self._connection.append(key, suffix, Meteor.bindEnvironment(callback));
};

Neo4jClient.prototype.getAll = function (keys, callback) {
  var self = this;

  var connection = self._connection;

  var errors = [];
  var values = [];
  var replyCount = 0;

  var n = keys.length;

  if (n == 0) {
    callback(errors, values);
    return;
  }

  _.each(_.range(n), function(i) {
    var key = keys[i];
    connection.get(key, Meteor.bindEnvironment(function(err, value) {
      if (err) {
        Meteor._debug("Error getting key from redis: " + err);
      }
      errors[i] = err;
      values[i] = value;

      replyCount++;
      if (replyCount == n) {
        callback(errors, values);
      }
    }));
  });
};

Neo4jClient.prototype.setAll = function (keys, values, callback) {
  var self = this;

  var connection = self._connection;

  var errors = [];
  var results = [];

  var n = keys.length;
  if (n == 0) {
    callback(errors, results);
    return;
  }

  var replyCount = 0;
  _.each(_.range(n), function(i) {
    var key = keys[i];
    var value = values[i];

    connection.set(key, value, Meteor.bindEnvironment(function(err, result) {
      if (err) {
        Meteor._debug("Error setting value in redis: " + err);
      }
      errors[i] = err;
      results[i] = result;

      replyCount++;
      if (replyCount == n) {
        callback(errors, results);
      }
    }));
  });
};


Neo4jClient.prototype.removeAll = function (keys, callback) {
  var self = this;

  var connection = self._connection;

  var errors = [];
  var results = [];

  var n = keys.length;
  if (n == 0) {
    callback(errors, results);
    return;
  }

  var replyCount = 0;
  _.each(_.range(n), function(i) {
    var key = keys[i];
    connection.del(key, Meteor.bindEnvironment(function(err, result) {
      if (err) {
        Meteor._debug("Error deleting key in redis: " + err);
      }
      errors[i] = err;
      results[i] = result;

      replyCount++;
      if (replyCount == n) {
        callback(errors, results);
      }
    }));
  });
};
*/

/*
Neo4jClient.prototype.del = function (keys, callback) {
  var self = this;

  self._connection.del(keys, Meteor.bindEnvironment(callback));
};
Neo4jClient.prototype.findOne = function (params, callback) {
  var self = this;

  //break params into neo4j read-able values

  self._connection.set(key, value, Meteor.bindEnvironment(callback));
};
*/
/*
Neo4jClient.prototype.insert = function (data, callback) {
  var self = this;

  // Do a _.each here for multiple data objects?
    var node = self._connection.createNode(data);
    
    node.save(Meteor.bindEnvironment(function(err) {
        if(err) return callback(err);
        node.index(INDEX_NAME, INDEX_KEY, INDEX_VAL, Meteor.bindEnvironment(function(err) {
            if(err) return callback(err);
            callback(null, node);
        }));
    }));


  self._connection.append(key, suffix, Meteor.bindEnvironment(callback));
};

Neo4jClient.prototype.find = function (params, callback) {
    var self = this;

    var db = self._connection;

    db.getIndexedNodes(INDEX_NAME, INDEX_KEY, INDEX_VAL, Meteor.bindEnvironment(function(err, nodes) {
        if(err) {
            if(err.message.match(/Neo4j NotFoundException/i)) {
                return callback(null, []);
            } else {
                return callback(err);
            }
        }
        callback(err, nodes);
    }));
};
*/
/*
Neo4jClient.prototype.removeAll = function (keys, callback) {
  var self = this;

  var connection = self._connection;

  var errors = [];
  var results = [];

  var n = keys.length;
  if (n == 0) {
    callback(errors, results);
    return;
  }

  var replyCount = 0;
  _.each(_.range(n), function(i) {
    var key = keys[i];
    connection.del(key, Meteor.bindEnvironment(function(err, result) {
      if (err) {
        Meteor._debug("Error deleting key in redis: " + err);
      }
      errors[i] = err;
      results[i] = result;

      replyCount++;
      if (replyCount == n) {
        callback(errors, results);
      }
    }));
  });
};
*/

