var util = require('util');

var AbstractError = function (msg, constr) {
  Error.captureStackTrace(this, constr || this);
  this.message = msg || 'Error';
};
util.inherits(AbstractError, Error);
AbstractError.prototype.name = 'Abstract Error';

var InvalidRequest = function (msg) {
  InvalidRequest.super_.call(this, msg, this.constructor);
};
util.inherits(InvalidRequest, AbstractError);
module.exports.InvalidRequest= InvalidRequest;

var ValueError = function (msg) {
  ValueError.super_.call(this, msg, this.constructor);
};
util.inherits(ValueError, AbstractError);
ValueError.prototype.message = 'Value Error';
module.exports.ValueError= ValueError;

var NoData = function (msg) {
  NoData.super_.call(this, msg, this.constructor);
};
util.inherits(NoData, AbstractError);
NoData.prototype.message = 'No Data';
module.exports.NoData= NoData;

var NodeDeleted = function (msg) {
  NodeDeleted.super_.call(this, msg, this.constructor);
};
util.inherits(NodeDeleted, AbstractError);
NodeDeleted.prototype.message = 'Node Deleted';
module.exports.NodeDeleted= NodeDeleted;

var NodeNotFound = function (msg) {
  NodeNotFound.super_.call(this, msg, this.constructor);
};
util.inherits(NodeNotFound, AbstractError);
NodeNotFound.prototype.message = 'Node Not Found';
module.exports.NodeNotFound= NodeNotFound;

