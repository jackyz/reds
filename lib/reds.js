
/*!
 * reds
 * Copyright(c) 2011 TJ Holowaychuk <tj@vision-media.ca>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var natural = require('natural')
  , _ =	require('underscore')
  , metaphone = natural.Metaphone.process
  , stem = natural.PorterStemmer.stem
  , stopwords = require('./stopwords')
  , redis = require('redis')
  , mmseg = require('mmseg')
  , noop = function(){};

/**
 * Library version.
 */

exports.version = '0.1.1';

/**
 * Expose `Search`.
 */

exports.Search = Search;

/**
 * Expose `Query`.
 */

exports.Query = Query;

/**
 * Search types.
 */

var types = {
    intersect: 'sinter'
  , union: 'sunion'
  , and: 'sinter'
  , or: 'sunion'
};

/**
 * Create a redis client, override to
 * provide your own behaviour.
 *
 * @return {RedisClient}
 * @api public
 */

exports.createClient = function(){
  return exports.client
    || (exports.client = redis.createClient());
};

/**
 * Return a new reds `Search` with the given `key`.
 *
 * @param {String} key
 * @return {Search}
 * @api public
 */

exports.createSearch = function(key){
  if (!key) throw new Error('createSearch() requires a redis key for namespacing');
  return new Search(key);
};

/**
 * Return the words in `str`.
 *
 * @param {String} str
 * @return {Array}
 * @api private
 */

exports.words = function(object){
  var q = mmseg.open('/usr/local/etc/');
  var obj = {word:[], other:[]}; // 用来建立索引的数据
  for(var sort in object){
    if(sort == "text" && object[sort] != ""){
      if(object[sort]){
	var s = q.segmentSync(object[sort]);
	// 要先进行编码，在metaphoneMap和metaphoneKeys中加上word前缀
	obj.word = _.without(_.union(s, [])," ");
      }
    }else if ((sort == "tag" && object[sort].length >0 ) || (( sort == "city" || sort == "device" || sort == "user" || sort == "public" || sort == "originality") && object[sort] !="" && object[sort] != undefined)){
      // 直接加上索引类型前缀
      if(sort == "tag"){
	_.each(object[sort], function(e){
	  obj.other.push("tag:"+e);
	});
      }else{
	obj.other.push(sort+":"+object[sort]);
      }
    }
  }
  return obj;
};

/**
 * Stem the given `words`.
 *
 * @param {Array} words
 * @return {Array}
 * @api private
 */

exports.stem = function(object){
  var ret = [];
  for(var v in object){
    if(v == "word"){
      var words = object.word;
      for (var i = 0, len = words.length; i < len; ++i) {
	ret.push(stem(words[i]));
      }
    }
    object.word = ret;
  }
  return object;
};

/**
 * Strip stop words in `words`.
 *
 * @param {Array} words
 * @return {Array}
 * @api private
 */

exports.stripStopWords = function(object){
  var ret = [];
  for(var v in object){
    if( v == "word"){
      var words = object[v];
      for (var i = 0, len = words.length; i < len; ++i) {
	if (~stopwords.indexOf(words[i])) continue;
	ret.push(words[i]);
      }
      object.word = ret;
    }
  }
  return object;
};

/**
 * Return the given `words` mapped to the metaphone constant.
 *
 * Examples:
 *
 *    metaphone(['tobi', 'wants', '4', 'dollars'])
 *    // => { '4': '4', tobi: 'TB', wants: 'WNTS', dollars: 'TLRS' }
 *
 * @param {Array} words
 * @return {Object}
 * @api private
 */

exports.metaphoneMap = function(obj){
  var ret = {};
  for(var v in obj){
    if(v == "word"){
      var words = obj.word;
      for (var i = 0, len = words.length; i < len; ++i) {
	ret[words[i]] = "wrod:"+metaphone(words[i]);
      }
    }
    obj.word = ret;
  }
  return obj;
};

/**
 * Return an array of metaphone constants in `words`.
 *
 * Examples:
 *
 *    metaphone(['tobi', 'wants', '4', 'dollars'])
 *    // => ['4', 'TB', 'WNTS', 'TLRS']
 *
 * @param {Array} words
 * @return {Array}
 * @api private
 */

exports.metaphoneArray = function(words){
  var arr = []
    , constant;
  for (var i = 0, len = words.length; i < len; ++i) {
    constant = metaphone(words[i]);
    if (!~arr.indexOf(constant)) arr.push(constant);
  }
  return arr;
};

/**
 * Return a map of metaphone constant redis keys for `words`
 * and the given `key`.
 *
 * @param {String} key
 * @param {Array} words
 * @return {Array}
 * @api private
 */

exports.metaphoneKeys = function(key, obj){
  var keys = [];
  var k = Object.keys(obj), v;
  for(var i=0; i<k.length,v=k[i]; i++){
    switch(v){
      case "word":
	var words = obj.word;
	keys = _.union(keys, exports.metaphoneArray(words).map(function(c){
	  return key +":word:"+ c;
	}));
	break;
      case "other":
	var other = obj.other;
	keys = _.union(keys, other.map(function(c){
	  return key +":"+ c;
	}));
	break;
    }
  }
  return keys;
};

/**
 * Initialize a new `Query` with the given `str`
 * and `search` instance.
 *
 * @param {String} str
 * @param {Search} search
 * @api public
 */

function Query(str, search) {
  this.str = str;
  this.type('and');
  this.search = search;
}

/**
 * Set `type` to "union" or "intersect", aliased as
 * "or" and "and".
 *
 * @param {String} type
 * @return {Query} for chaining
 * @api public
 */

Query.prototype.type = function(type){
  this._type = types[type];
  return this;
};

/**
 * Perform the query and callback `fn(err, ids)`.
 *
 * @param {Function} fn
 * @return {Query} for chaining
 * @api public
 */

Query.prototype.end = function(fn){
  var key = this.search.key
    , db = this.search.client
    , query = this.str
    , words = exports.stem(exports.stripStopWords(exports.words(query)))
    , keys = exports.metaphoneKeys(key, words)
    , type = this._type;
  if (!keys.length) return fn(null, []);
  db[type](keys, fn);

  return this;
};

/**
 * Initialize a new `Search` with the given `key`.
 *
 * @param {String} key
 * @api public
 */

function Search(key) {
  this.key = key;
  this.client = exports.createClient();
}

/**
 * Index the given `str` mapped to `id`.
 *
 * @param {String} str
 * @param {Number|String} id
 * @param {Function} fn
 * @api public
 */

Search.prototype.index = function(str, id, fn){
  var key = this.key
    , db = this.client
    , words = exports.stem(exports.stripStopWords(exports.words(str)))
    , map = exports.metaphoneMap(words)
    , keys = Object.keys(map)
    , len = keys.length;
  var multi = db.multi();
  keys.forEach(function(k){
    switch(k){
      case "word":
	var words = map.word;
	var wk = Object.keys(map.word);
	wk.forEach(function(word){
	  multi.sadd(key + ':' + words[word], id); // 直接给定key值前半部分
	  multi.sadd('index:object:' + id, words[word]); // 删除时候的依据
	});
	break;
      case "other":
	map.other.forEach(function(o){
	  console.log(key + ":" +o );
	  multi.sadd(key + ':' + o, id); // 直接给定key值前半部分
	  multi.sadd('index:object:' + id, o);
	});
	break;
    }
  });
  multi.exec(fn || noop);

  return this;
};

/**
 * Remove occurrences of `id` from the index.
 *
 * @param {Number|String} id
 * @api public
 */

Search.prototype.remove = function(id, fn){
  fn = fn || noop;
  var key = this.key
    , db = this.client;
  db.smembers(key + ':object:' + id, function(err, constants){
    if (err) return fn(err);
    var multi = db.multi().del(key + ':object:' + id);
    constants.forEach(function(c){
      multi.srem(key + ':' + c, id);
    });
    multi.exec(fn);
  });
  return this;
};

/**
 * Perform a search on the given `query` returning
 * a `Query` instance.
 *
 * @param {String} query
 * @param {Query}
 * @api public
 */

Search.prototype.query = function(query){
  return new Query(query, this);
};
