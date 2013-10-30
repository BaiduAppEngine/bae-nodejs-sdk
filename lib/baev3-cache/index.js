/*
 * memcache for bae nodejs
 *
 */
var _ = require('underscore')._;
var manager = require('jackpot');
var Socket = require('net').Socket;
var nshead = require('node-nshead');
var mcpack = require('node-mcpack');

var CACHE_MAX_KEY_LEN = 180;
var CACHE_MAX_VALUE_LEN = 1048576;
var CACHE_MAX_QUERY_NUM = 64;

/*
 * @constructor
 * @param {String} cacheId
 * @param {String} memcacheAddr
 * @param {String} user
 * @param {String} password
 *
 */

function Memcache (cacheId,memcacheAddr,user,password){
  var self = this;

  self.CACHE_MAX_KEY_LEN = 180;
  self.CACHE_MAX_VALUE_LEN = 1048576;
  self.CACHE_MAX_QUERY_NUM = 64;

  self.pool_size = 10;
  self.timeout = 5000;
  self.nshead_len = 36;
  var serverTokens = [];
  
  this.cacheId = cacheId;
  this.memcacheAddr = memcacheAddr;
  this.user = user;
  this.password = password;
  var server = this.memcacheAddr;
  if(typeof server === 'string'){
    // check server
    serverTokens = /(.*):(\d+){1,}$/.exec(server).reverse();
    // server into  [port, host]
    if(Array.isArray(serverTokens))
      serverTokens.pop();
  }else {
    // error
    console.log('')
    throw new Error('');
  }

  // create connectionpool
  self.pool = new manager(self.pool_size);
  self.pool.retries = 1;
  self.pool.factory(function(){
    var s = new Socket();
    var manager = this;
    s.setTimeout(self.timeout);
    s.metaQuery = [];
    s.responesBuf = new Buffer(0);
    s.bufferArray = [];
    s.tokens = [].concat(serverTokens);


    //add the event listener
    s.on('close', function(){
      manager.remove(this);
    });
    s.on('data', function(buffer_stream){
      s.responesBuf = Buffer.concat([s.responesBuf, buffer_stream]);

      //handle multi responses
      while(s.responesBuf.length > self.nshead_len){
        var nshead_buf = s.responesBuf.slice(0, self.nshead_len);
        var unpacked_nshead = nshead.unpack(nshead_buf);
        var body_len = unpacked_nshead.body_len;
        var single_response_len = self.nshead_len + body_len;
        if(s.responesBuf.length < single_response_len){
          break;
        }
        var body = s.responesBuf.slice(self.nshead_len, single_response_len);
        s.responesBuf = s.responesBuf.slice(single_response_len);
        rawDataReceived(s, body);
      }

    });
    s.on('timeout', function(){
      //console.log('timeout');
      manager.remove(this);
    });
    s.on('end', function(){
      s.end();
    });


    s.connect.apply(s, s.tokens);
    return s;
  });

}

function rawDataReceived (sock, body){
  var meta_data = sock.metaQuery.shift();
  //console.log('meta_data:', meta_data);
  if(meta_data && meta_data.callback){
    try {
      var result = mcpack.pack2json(body);
    }catch(e){
      console.log('error pack2json');
      throw new Error('Parse result to json type failed: ' + e);
    }
    if(meta_data.cmd === 'get' || meta_data.cmd === 'incr' ||
      meta_data.cmd === 'decr'){
      var err_ret = handlErr(result);
      if(err_ret){
        meta_data.callback(err_ret);
      }else {
         var value;
         try {
           value = JSON.parse(result['content']['result0']['value']);
         } catch(e){
           //console.log('parse value to object error', e);
           //throw e;
           value = result['content']['result0']['value'];
         }
        meta_data.callback(null, value);
      }
    }else if(meta_data.cmd === 'set' || meta_data.cmd === 'add' ||
      meta_data.cmd === 'replace' || meta_data.cmd === 'delete'){
      var err_ret = handlErr(result);
      if(err_ret){
        meta_data.callback(err_ret);
      }else {
        meta_data.callback(null);
      }
    }else{
      // handle getMulti cmd
      var result_value = [];
      if(result['err_no'] !== 0){
        meta_data.callback(result['error']);
      }else {
        _.each(result['content'], function(value, key){
          if(value['err_no'] === 0){
            try {
              var get_result = JSON.parse(JSON.stringify(value['value']));
           } catch(e){
             console.log('parse value to object error', e);
             throw e;
           }
           result_value.push(get_result);
         }else{
          result_value.push(value['error']);
        }
      });
        meta_data.callback(null, result_value);
      }
    }
  }
}

/*
 * @private
 * @param {String} cmd
 * @param {Object} content
 * @return {Buffer}
 */

Memcache.prototype._createReqQuery = function (cmd, content){
  var self = this;

  var bae_ak = this.user;
  var bae_sk = this.password;
  var log_id = 0; 
  var app_id = this.cacheId;

  var query = {};
  query['cmd'] = cmd;
  query['pname'] = bae_ak;
  query['token'] = bae_sk;
  query['logid'] = log_id;
  query['appid'] = app_id;
  query['content'] = {};
  var query_num = (function (obj){
                     var count = 0;
                     for(var i in obj){
                       if(obj.hasOwnProperty(i)){
                         count++;
                       }
                     }
                     return count;
                   })(content);
  if(query_num > self.CACHE_MAX_QUERY_NUM){
    console.log('error: current query numbers is ' + query_num + ' it exceed max query num 60');
    throw new Error('query numbers is exceed max query numbers');
  }

  query['content'].query_num = query_num;
  var qi;

  for(var i = 0, l = content.length; i < l; i++){
    qi = 'query' + i;
    query['content'][qi] = {};
    query['content'][qi].key = content[i].key;
    if(content[i].hasOwnProperty('value')){
      query['content'][qi].value = content[i].value;
    }
    if(content[i].hasOwnProperty('delay_time')){
      query['content'][qi].delay_time = content[i].delay_time;
    }
  }

  //console.dir(query);

  try {
    var mcpacked_query = mcpack.json2pack(query);
    var nshead_info = {'log_id': query['logid'], 'body_len': mcpacked_query.length};
    var nshead_packed = nshead.pack(nshead_info);
  } catch(e){
    console.log('mcpacked error: ', e);
    throw new Error('mcpack error: ', e);
  }

  var buf = Buffer.concat([nshead_packed, mcpacked_query]);
  return buf;

};


/*
 * @private
 */


Memcache.prototype._transBuf = function (cmd, buf, cb){
  var self = this;
  var nshead_len = 36;
  self.pool.pull(function(err, sock){
    if(!sock || err || sock.readyState !== 'open'){
      cb && cb(new Error('socket error'));
      return;
    }
    sock.metaQuery.push({'cmd': cmd, 'callback': cb});
    sock.write(buf);
  });
};

/*
 *
 * @private
 */

function handlErr(result){
  var error = null;
  if(result['err_no'] !== 0){
    error = JSON.stringify(result['error']);
  }else {
    if(result['content']['result0']['err_no'] !== 0){
      error = JSON.stringify(result['content']['result0']['error']);
    }
  }
  return error;
}

function checkInput(key, value){
  if(key.length > CACHE_MAX_KEY_LEN || (value && value.length > CACHE_MAX_VALUE_LEN)){
    console.log('invalid params, please check your key/value');
    throw new Error('error occured in params: key or value');
  }
}

/*
 * @public
 * @param {String} key
 * @cb {function} cb(err, value)
 */

Memcache.prototype.get = function (key, cb){
  var self = this;

   if(arguments.length < 1){
    throw new Error('arguments error: arguments length must >= 1');
    return;
  }
  if(typeof key !== 'string'){
    throw new Error('arguments error: "key" must be string type');
    return;
  }
  if(arguments.length >= 2 && typeof cb !== 'function'){
    throw new Error('arguments error: "callback" must be function type');
    return;

  }
  checkInput(key);
  var buf = self._createReqQuery('get', [{'key': key}]);
  self._transBuf('get', buf, cb);
};

/*
Memcache.prototype.getMulti = function(keys, cb){
  var self = this;

  if(arguments.length < 1){
    throw new Error('arguments error: arguments length must >= 1');
    return;
  }
  if(Object.prototype.toString.call(keys) !== '[object Array]'){
    throw new Error('arguments error: "keys" must be Array type');
    return;
  }
  if(arguments.length >= 2 && typeof cb !== 'function'){
    throw new Error('arguments error: "callback" must be function type');
    return;
  }
  var query = [];
  for(var i = 0; i < keys.length; i++){
    if(typeof keys[i] !== 'string'){
      throw new Error('arguments error: "element in keys" must be String type');
      return;
    }
    checkInput(keys[i]);
    query.push({'key': keys[i]});
  }

  var buf = self._createReqQuery('get', query);
  self._transBuf('getMulti', buf, cb);
}
*/

/*
 * @public
 * @param {String} key
 * @param {Object} value
 * @param {Number} lifetime
 * @param {Function} cb(err)
 */

Memcache.prototype.set = function (key, value, lifetime, cb){
  var self = this;

  if(arguments.length < 2){
    throw new Error('arguments error: arguments length must >= 2');
    return;
  }
  if(typeof key !== 'string'){
    throw new Error('arguments error: "key" must be string type');
    return;
  }
  if(arguments.length === 3){
    if(typeof lifetime === 'function'){
      cb = lifetime;
      lifetime = undefined;
    }else if(typeof lifetime !== 'number'){
      throw new Error('arguments error: "lifetime" must be number type');
      return;
    }
  }
  if(arguments.length > 3){
    if(typeof lifetime !== 'number' || typeof cb != 'function'){
      throw new Error('arguments error: "lifetime" or "callback" type error');
      return;
    }
  }

  // TODO add function judge
  var delay_time = lifetime || 0;
  if(typeof value !== 'string'){
    value = JSON.stringify(value);
  }
  checkInput(key, value);
  var buf = self._createReqQuery('set', [{'key':key, 'value':value, 'delay_time':delay_time*1000}]);
  self._transBuf('set', buf, cb);
};

Memcache.prototype.delete = function (key, lifetime, cb){
  var self = this;

  if(arguments.length < 1){
    throw new Error('arguments error: arguments length must >= 1');
    return;
  }
  if(typeof key !== 'string'){
    throw new Error('arguments error: "key" must be string type');
    return;
  }
  if(arguments.length === 2){
    if(typeof lifetime === 'function'){
      cb = lifetime;
      lifetime = undefined;
    }else if(typeof lifetime !== 'number'){
      throw new Error('arguments error: "delaytime" must be number type');
      return;
    }
  }
  if(arguments.length > 2){
    if(typeof lifetime !== 'number' || typeof cb != 'function'){
      throw new Error('arguments error: "lifetime" or "callback" type error');
      return;
    }
  }

  var delay_time = lifetime || 0;
  checkInput(key);
  var buf = self._createReqQuery('delete', [{'key': key, 'delay_time': delay_time* 1000}]);
  self._transBuf('delete', buf, cb);
}

Memcache.prototype.add = function (key, value, lifetime, cb){
  var self = this;

  if(arguments.length < 2){
    throw new Error('arguments error: arguments length must >= 2');
    return;
  }
  if(typeof key !== 'string'){
    throw new Error('arguments error: "key" must be string type');
    return;
  }
  if(arguments.length === 3){
    if(typeof lifetime === 'function'){
      cb = lifetime;
      lifetime = undefined;
    }else if(typeof lifetime !== 'number'){
      throw new Error('arguments error: "lifetime" must be number type');
      return;
    }
  }
  if(arguments.length > 3){
    if(typeof lifetime !== 'number' || typeof cb != 'function'){
      throw new Error('arguments error: "lifetime" or "callback" type error');
      return;
    }
  }


  // TODO add function judge
  var delay_time = lifetime || 0;
  if(typeof value !== 'string'){
    value = JSON.stringify(value);
  }
  checkInput(key, value);
  var buf = self._createReqQuery('add', [{'key':key, 'value':value, 'delay_time':delay_time*1000}]);
  self._transBuf('add', buf, cb);
}

Memcache.prototype.replace = function (key, value, lifetime, cb){
  var self = this;

  if(arguments.length < 2){
    throw new Error('arguments error: arguments length must >= 2');
    return;
  }
  if(typeof key !== 'string'){
    throw new Error('arguments error: "key" must be string type');
    return;
  }
  if(arguments.length === 3){
    if(typeof lifetime === 'function'){
      cb = lifetime;
      lifetime = undefined;
    }else if(typeof lifetime !== 'number'){
      throw new Error('arguments error: "lifetime" must be number type');
      return;
    }
  }
  if(arguments.length > 3){
    if(typeof lifetime !== 'number' || typeof cb != 'function'){
      throw new Error('arguments error: "lifetime" or "callback" type error');
      return;
    }
  }

  // TODO add function judge
  var delay_time = lifetime || 0;
  if(typeof value !== 'string'){
    value = JSON.stringify(value);
  }
  checkInput(key, value);
  var buf = self._createReqQuery('replace', [{'key':key, 'value':value, 'delay_time':delay_time*1000}]);
  self._transBuf('replace', buf, cb);
}


Memcache.prototype.incr = function (key, delta, cb){
  var self =  this;

  if(arguments.length < 1){
    throw new Error('arguments error: arguments length must >= 1');
    return;
  }
  if(typeof key !== 'string'){
    throw new Error('arguments error: "key" must be string type');
    return;
  }
  if(arguments.length === 2){
    if(typeof delta === 'function'){
      cb = delta;
      delta = undefined;
    }else if(typeof delta !== 'number'){
      throw new Error('arguments error: "delta" must be number type');
      return;
    }
  }
  if(arguments.length > 2){
    if(typeof delta !== 'number' || typeof cb != 'function'){
      throw new Error('arguments error: "delta" or "callback" type error');
      return;
    }
  }

  var value = delta || 1;
  value = JSON.stringify(value);
  checkInput(key, value);
  var buf = self._createReqQuery('increment', [{'key':key, 'value':value, 'delay_time':0}]);
  self._transBuf('incr', buf, cb);
}

Memcache.prototype.decr = function (key, delta, cb){
  var self = this;

  if(arguments.length < 1){
    throw new Error('arguments error: arguments length must >= 1');
    return;
  }
  if(typeof key !== 'string'){
    throw new Error('arguments error: "key" must be string type');
    return;
  }
  if(arguments.length === 2){
    if(typeof delta === 'function'){
      cb = delta;
      delta = undefined;
    }else if(typeof delta !== 'number'){
      throw new Error('arguments error: "delta" must be number type');
      return;
    }
  }
  if(arguments.length > 2){
    if(typeof delta !== 'number' || typeof cb != 'function'){
      throw new Error('arguments error: "delta" or "callback" type error');
      return;
    }
  }

  var value = delta || 1;
  value = JSON.stringify(value);
  checkInput(key, value);
  var buf = self._createReqQuery('decrement', [{'key':key, 'value':value, 'delay_time':0}]);
  self._transBuf('decr', buf, cb);
}

module.exports = Memcache;
