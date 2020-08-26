var request = require('request');
var options = {
  'method': 'POST',
  'url': 'https://mb-kafka-demo.herokuapp.com/muletest',
  'headers': {
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({"payload":"Inbound call from test.js (localhost)" })

};
request(options, function (error, response) { 
  if (error) throw new Error(error);
  console.log(response.body);
});

