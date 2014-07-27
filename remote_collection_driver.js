var path = Npm.require('path');
var Future = Npm.require(path.join('fibers', 'future'));

Neo4jInternals.RemoteCollectionDriver = function (
  url, options) {
  var self = this;
  self.connection = new Neo4jConnection(url, options);
};

_.extend(Neo4jInternals.RemoteCollectionDriver.prototype, {
  open: function (name) {
    var self = this;
    var ret = {};
    _.each(
      ['find', 'findOne', 'insert', 'update', , 'upsert',
       'remove', '_ensureIndex', '_dropIndex', '_createCappedCollection',
       'dropCollection'],
      function (m) {
        ret[m] = function () {
          throw new Error(m + ' is not available on NEO4J! XXX');
        };
      });
      _.each(['getNodeById', 'getIndexedNodes', 'createNode',
              '_observe'].concat(NEO4J_COMMANDS_HASH),
        function (m) {
          ret[m] = function (/* args */) {
            var args = _.toArray(arguments);
            var cb = args.pop();

            if (_.isFunction(cb)) {
              args.push(function (err, res) {
                // In Meteor the first argument (error) passed to the
                // callback is undefined if no error occurred.
                if (err === null) err = undefined;
                cb(err, res);
              });
            } else {
              args.push(cb);
            }

            // XXX 'matching' method is a special case, because it returns a
            // cursor and cursors need to know what collection they belong to.
            if (m === 'getIndexedNodes') {
              args = [name].concat(args);
            }

            return self.connection[m].apply(self.connection, args);
          };
        });
    return ret;
  }
});

// A version of _.once that behaves sensibly under exceptions
// If the first call is an exception:
//  _.once will return undefined on subsequent calls
//  meteorOnce will rethrow the exception every time
meteorOnce = function(f) {
  var resultFuture = null;
  return function () {
    if (!resultFuture) {
      resultFuture = new Future;
      try {
        var v = f.apply(this, arguments);
        resultFuture.return(v);
      } catch (e) {
        resultFuture.throw(e);
      }
    }
    return resultFuture.wait();
  };
};

// Create the singleton RemoteCollectionDriver only on demand, so we
// only require Mongo configuration if it's actually used (eg, not if
// you're only trying to receive data from a remote DDP server.)
Neo4jInternals.defaultRemoteCollectionDriver = meteorOnce(function () {
  var neo4jUrl = process.env.NEO4J_URL;
  var connectionOptions = {};

  if (!neo4jUrl) {
    neo4jUrl = Meteor.settings.neo4jUrl;
  }
  if (!neo4jUrl) {
    neo4jUrl = '127.0.0.1:7474';
    Meteor._debug("Defaulting NEO4J_URL to " + neo4jUrl);
  }

  return new Neo4jInternals.RemoteCollectionDriver(neo4jUrl, connectionOptions);
});
