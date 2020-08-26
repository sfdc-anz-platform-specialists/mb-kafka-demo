//modified to support a Heroku Kafka instance

var express = require('express');
var router = express.Router();
var Promise = require("bluebird");
var nforce = require('nforce');
var org = require('../lib/connection');

//KAFKA might have a prefix 
const TOPIC=(process.env.KAFKA_PREFIX||'')+'clickstream';

var Kafka = require('no-kafka');
var producer = new Kafka.Producer();
var consumer = new Kafka.SimpleConsumer();

// this function is the data handler for the Kafka consumer
var dataHandler = function (messageSet, topic, partition) {
  messageSet.forEach(function (m) {
    console.log('Kafka Subscriber returned:', topic, partition, m.offset, m.message.value.toString('utf8'));
    // we persist the message in a SFDC custom object -- so actually this method behaves like a Kafka "connector"
    var ks = nforce.createSObject('KafkaSink__c');
    ks.set('Key__c', m.message.key.toString('utf8'));
    ks.set('Value__c', m.message.value.toString('utf8'));  
    ks.set('Clicks__c', 1);  
    org.insert({ sobject: ks });  
  });
};

// initialise and subscribe the Kafka consumer
consumer.init().then(function () {
consumer.subscribe(TOPIC, dataHandler);  // all partitions
});

//initialise the Kafka producer
producer.init().then(function () {
console.log('Producer initialised');
});

// THE ROUTER ENDPOINTS

// home page
router.get('/', function(req, res, next) { 
  
  mykafkaproducer(TOPIC,'/','Home Page');
  org.query({ query: "Select Id, Name, Type, Industry, Rating From Account Order By LastModifiedDate DESC" })
    .then(function(results){
      res.render('index', { records: results.records });
    });
});

// page to show previous Kafka messages, persisted in SFDC
router.get('/kafka', function(req, res, next) {
  
  mykafkaproducer(TOPIC,'/kafka','Kafka (this page)'); //yep, we publish this page visit to kafka also
  org.query({ query: "select key__c,value__c, count(id) c from kafkasink__c group by key__c,value__c" })
    .then(function(results){
      res.render('kafka', { records: results.records });
      console.log(results.records);
    });

});

/* Display new account form */
router.get('/new', function(req, res, next) {
  
  mykafkaproducer(TOPIC,'/new','New Account'); // publish to Kafka
  res.render('new');
});

/* Creates a new the record */
router.post('/', function(req, res, next) {

  var acc = nforce.createSObject('Account');
  acc.set('Name', req.body.name);
  acc.set('Industry', req.body.industry);
  acc.set('Type', req.body.type);
  acc.set('AccountNumber', req.body.accountNumber);
  acc.set('Description', req.body.description);
  org.insert({ sobject: acc })
    .then(function(account){
      res.redirect('/' + account.id);
    })
});

/* Inbound Mulesoft POST */
router.post('/muletest', function(req, res, next) {
  
  var payload=req.body.payload;
  console.log('Recived payload',req.body);
  mykafkaproducer(TOPIC,'/muletest',payload); //publish to Kafka
  res.sendStatus(200); 
});



/* Record detail page */
router.get('/:id', function(req, res, next) {
  
  mykafkaproducer(TOPIC,'/:id','Record Detail'); //publish to Kafka
  // query for record, contacts and opportunities
  Promise.join(
    org.getRecord({ type: 'account', id: req.params.id }),
    org.query({ query: "Select Id, Name, Email, Title, Phone From Contact where AccountId = '" + req.params.id + "'"}),
    org.query({ query: "Select Id, Name, StageName, Amount, Probability From Opportunity where AccountId = '" + req.params.id + "'"}),
    org.query({ query: "SELECT Id, Name, CreatedDate, ContentType From Attachment where ParentId = '" + req.params.id + "'"}),
    org.query({ query: "SELECT Id, Name, CreatedDate, ContentType From Attachment where ParentId IN (Select Id FROM Contact WHERE AccountId = '" + req.params.id + "')"}),
    function(account, contacts, opportunities, attAcc, attCon) {
        res.render('show', { record: account, contacts: contacts.records, opps: opportunities.records, atts: Object.assign({}, attAcc.records, attCon.records)});
    });
});

/* display attachment in browser if supported, otherwise download */
router.get('/attachment/:id', function(req, res){
  Promise.join(
    org.getRecord({type: 'Attachment', id: req.params.id, fields: ['Id', 'ContentType', 'Name'], raw: true}),
    org.getBody({id: req.params.id, type: 'Attachment'}),
    function(attachment, body){
      res.set('Content-Type', attachment.ContentType);
      res.set('Content-Disposition', "inline; filename='" + attachment.Name + "'");
      res.send(body);
    });
});

/* Display record update form */
router.get('/:id/edit', function(req, res, next) {

  mykafkaproducer(TOPIC,'/:id/edit','Edit Record'); //publish to kafka
  org.getRecord({ id: req.params.id, type: 'Account'})
    .then(function(account){
      res.render('edit', { record: account });
    });
});

/* Display record update form */
router.get('/:id/delete', function(req, res, next) {

  mykafkaproducer(TOPIC,'/:id/delete','Delete Record'); //publish to kafka
  var acc = nforce.createSObject('Account');
  acc.set('Id', req.params.id);
  org.delete({ sobject: acc })
    .then(function(account){
      res.redirect('/');
    });
});

/* Updates the record */
router.post('/:id', function(req, res, next) {

  var acc = nforce.createSObject('Account');
  acc.set('Id', req.params.id);
  acc.set('Name', req.body.name);
  acc.set('Industry', req.body.industry);
  acc.set('Type', req.body.type);
  acc.set('AccountNumber', req.body.accountNumber);
  acc.set('Description', req.body.description);
  org.update({ sobject: acc })
    .then(function(){
      res.redirect('/' + req.params.id);
    })
});


//helper function to publish messages to Kafka
function mykafkaproducer(t,k,v){

producer.send({topic: t, message: {key:k,value: v} })
   .then (function (result) {
  console.log('The Kafka producer returned the following result:',result);   
});
return;

}
module.exports = router;
