/*global require,module,process*/

var AWS = require('aws-sdk');
var winston = require('winston');

var S3DocumentStore = function(options) {
  this.expire = options.expire;
  this.bucket = options.s3bucket;
  this.key = options.s3key;
  this.keyID = options.s3keyID;
  this.endpoint = options.s3endpoint;
  this.client = new AWS.S3({
      accessKeyId: this.keyID,
      secretAccessKey: this.key,
      endpoint: this.endpoint,
      signatureVersion: 'v4',
      s3ForcePathStyle: true
  });
};

S3DocumentStore.prototype.get = function(key, callback, skipExpire) {
  var _this = this;

  var req = {
    Bucket: _this.bucket,
    Key: key
  };

  _this.client.getObject(req, function(err, data) {
    if(err) {
      winston.info(err.message);
      winston.debug(err.stack);
      callback(false);
    }
    else {
      callback(data.Body.toString('utf-8'));
      if (_this.expire && !skipExpire) {
        winston.warn('s3 store cannot set expirations on keys');
      }
    }
  });
}

S3DocumentStore.prototype.set = function(key, data, callback, skipExpire) {
  var _this = this;

  var req = {
    Bucket: _this.bucket,
    Key: key,
    Body: data,
    ContentType: 'text/plain'
  };

  _this.client.putObject(req, function(err, data) {
    if (err) {
      callback(false);
    }
    else {
      callback(true);
      if (_this.expire && !skipExpire) {
        winston.warn('s3 store cannot set expirations on keys');
      }
    }
  });
}

module.exports = S3DocumentStore;
