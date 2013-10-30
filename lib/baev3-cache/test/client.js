var Memcached = require('../index.js');
var server = '10.213.98.19:40000';
var client = new Memcached();
var fs = require('fs');

var a = {b:1}
var key1 = 'key00';
    var value1 = '123abc';
    var key2 = 'key22';
    var value2 = 222;
    var key3 = 'key33';
    var value3 = 'ajgflgj';
    var key4 = 'key44';
/*
client.getMulti(['first'], function(err, result){
  if(err){console.log(err);}else{
    result.forEach(function(ele){
      console.log(ele);
    });
  }
});
*/

var str = fs.readFileSync('./0.5M.txt').toString();
//
str = str.slice(0)+ str.slice(0);
console.log(str.length);
client.set('first', a , function(err){
  if(!err){
    client.get('first', function(err, result){
      console.log("get: ", result);
    })
  }else{
    console.log('set  failed');
  }
});
/*
var key = 'add_replace_dele';
var value = 'add_value';
client.add(key, value, function(err){
  if(!err){
    console.log('add ok');
    client.replace(key, 'replace_value', function(err){
      if(!err){
        console.log('replace ok');
        client.delete(key, function(err){
          if(!err){
            console.log('delete ok');
          }else{
            console.log('delete failed');
          }
        });
      }else{
        console.log('replace failed');
      }
    });
  }else{
    console.log('add failed');
  }
});


client.set('incr', 5, function(err){
 if(!err){
    client.incr('incr', 4, function(err, result){
    if(!err){
         console.log("incr: ", result);
         client.decr('incr', 3, function(err, result){
           if(!err){
             console.log('decr:', result);
           }
         });
     }else{
         console.log(err);
     }
    });
 }
}); 


client.set('no_number', 'fdad', function(err){
  if(!err){
    client.incr('no_number', 4, function(err, result){
    if(!err){
         console.log("incr: ", result);
         client.decr('no_number', 100, function(err, result){
           if(!err){
             console.log('decr:', result);
           }
         });
     }else{
         console.log(err);
     }
   });
 }
});


*/

//client.replace();
//client.add();
//client.delete();

//arguments test

