var aws = require('aws-sdk');
var moment = require('moment');
var request = require('request');
var limit = require("simple-rate-limiter");
var propertiesReader = require('properties-reader');

var properties = propertiesReader('properties.file');

// PROPERTIES
var AWS_ACCESS_KEY_ID = properties.get('aws.access.key');
var AWS_SECRET_ACCESS_KEY = properties.get('aws.secret.key');
var FACEBOOK_APP_ID = properties.get('facebook.app.id');
var FACEBOOK_ACCESS_TOKEN = properties.get('facebook.access.token');

// VARIABLES
var MAX_QUERY_TRIES = 3; // MAX NUMBER OF RETRIES WHEN SERVER ERROR
var QUERY_RETRY_TIMEOUT = 2000; // WAIT 'QUERY_RETRY_TIMEOUT' SECONDS TO RETRY

// COUNTERS
var findEventsCounter = 0;
var getEventDetailsCounter = 0;
var saveEventCounter = 0;

//Set AWS configuration for future requests.
aws.config.update({"accessKeyId": AWS_ACCESS_KEY_ID, "secretAccessKey": AWS_SECRET_ACCESS_KEY, "region": "eu-west-1"});
aws.config.apiVersions = {
  dynamodb: '2012-08-10'
};

var dynamodb = new aws.DynamoDB();
var now = moment();

var facebookPageEventsAPI = limit(function(pageId, callback) {
    var url = "https://graph.facebook.com/v2.3/" + pageId + "/events?access_token=" + FACEBOOK_APP_ID + "|" + FACEBOOK_ACCESS_TOKEN;
    request(url, callback);
}).to(1).per(1000);

var facebookPageEventDetailsAPI = limit(function(eventId, callback) {
    var url = "https://graph.facebook.com/v2.3/" + eventId + "?access_token=" + FACEBOOK_APP_ID + "|" + FACEBOOK_ACCESS_TOKEN;
    request(url, callback);
}).to(1).per(1000);

// scan table
function scanTable(lastKey) {
	if (lastKey === null) {
		dynamodb.scan({
	        "TableName": "Clubs"
    	}, function (err, data) {
		    if (err) {
		    	console.log(err, err.stack);
		    	process.exit();
		    } else {
		    	processItems(data);
		    	if (data.LastEvaluatedKey != null) {
		    		scanTable(data.LastEvaluatedKey);
		    	}
		    }
		});
	} else {
		dynamodb.scan({
        	"TableName": "Clubs",
        	"ExclusiveStartKey": lastKey
	    }, function (err, data) {
		    if (err) {
		    	console.log(err, err.stack);
		    	process.exit();
		    } else {
		    	processItems(data);
		    	if (data.LastEvaluatedKey != null) {
		    		scanTable(data.LastEvaluatedKey);
		    	}
		    }
		});
	}
}

// check FacebookId attribute and request Facebook events
function processItems(data) {
	for (var ii in data.Items) {
        item = data.Items[ii];

        if (item.FacebookId != null) {
        	findEvents(item.Id, item.FacebookId, 1);
        }
	}
}

function findEvents(clubId, pageId, tryNumber) {
	findEventsCounter++;

	facebookPageEventsAPI(pageId, function(error, response, body) {
		if (!error && response.statusCode === 200) {
			var json = JSON.parse(body.toString());
			json.data.forEach(function(item) {
				var start = moment().zone(item.start_time);
				if (now.isBefore(start)) {
					getEventDetails(clubId, item.id, 1);
				}
			});
			findEventsCounter--;
		} else {
			if (response !== undefined && response.statusCode === 500 && tryNumber <= MAX_QUERY_TRIES) {
				tryNumber++;
				setTimeout(function() {
					findEvents(pageId, tryNumber);
				}, QUERY_RETRY_TIMEOUT);
			} else {
				if (response !== undefined) {
					if (error !== null && error !== undefined) {
						console.log("findEvents, " + error + ", STATUS CODE: " + response.statusCode);
					} else {
						console.log("findEvents, STATUS CODE: " + response.statusCode);
					}
				} else {
					if (response !== undefined) {
						if (error !== null && error !== undefined) {
							console.log("findEvents, " + error + ", STATUS CODE: " + response.statusCode);
						} else {
							console.log("findEvents, STATUS CODE: " + response.statusCode);
						}
					} else {
						if (error !== null && error !== undefined) {
							console.log("findEvents, " + error);
						} else {
							console.log("findEvents, something failed but no error and response objects");
						}
					}
				}
				findEventsCounter--;
			}
		}
	});
}

function getEventDetails(clubId, eventId, tryNumber) {
	getEventDetailsCounter++;

	facebookPageEventDetailsAPI(eventId, function(error, response, body) {
		if (!error && response.statusCode === 200) {
			saveEvent(clubId, JSON.parse(body.toString()));
			getEventDetailsCounter--;
		} else {
			if (response !== undefined && response.statusCode === 500 && tryNumber <= MAX_QUERY_TRIES) {
				tryNumber++;
				setTimeout(function() {
					getEventDetails(pageId, tryNumber);
				}, QUERY_RETRY_TIMEOUT);
			} else {
				if (response !== undefined) {
					if (error !== null && error !== undefined) {
						console.log("getEventDetails, " + error + ", STATUS CODE: " + response.statusCode);
					} else {
						console.log("getEventDetails, STATUS CODE: " + response.statusCode);
					}
				} else {
					if (response !== undefined) {
						if (error !== null && error !== undefined) {
							console.log("getEventDetails, " + error + ", STATUS CODE: " + response.statusCode);
						} else {
							console.log("getEventDetails, STATUS CODE: " + response.statusCode);
						}
					} else {
						if (error !== null && error !== undefined) {
							console.log("getEventDetails, " + error);
						} else {
							console.log("getEventDetails, something failed but no error and response objects");
						}
					}
				}
				getEventDetailsCounter--;
			}
		}
	});
}

function saveEvent(clubId, event) {
	saveEventCounter++;

	var clubEvent = new Object();
	clubEvent.id = event.id;
	clubEvent.clubId = clubId;
	clubEvent.name = event.name;
	clubEvent.description = event.description;

	var start = moment().zone(event.start_time).valueOf();
	var updatedAt = moment().zone(event.updated_time).valueOf();
	clubEvent.startsAt = start;
	clubEvent.updatedAt = updatedAt;

	if (event.cover != null) {
		clubEvent.pictureLink = event.cover.source;
	}
	if (event.ticket_uri != null) {
		clubEvent.ticketsLink = event.ticket_uri;
	}
	if (event.end_time != null) {
		var endsAt = moment().zone(event.end_time).valueOf();
		clubEvent.endsAt = endsAt;
	}
	saveEventApi(clubEvent);
}

// update item
var saveEventApi = limit(function(clubEvent) {
	var item = {
		"Id":{"S":clubEvent.id.toString()},
		"ClubId":{"S":clubEvent.clubId},
		"Name":{"S":clubEvent.name},
		"Description":{"S":clubEvent.description},
		"StartsAt":{"S":clubEvent.startsAt.toString()},
		"UpdatedAt":{"S":clubEvent.updatedAt.toString()}
	}

	if (clubEvent.pictureLink != null) {
		item.PictureLink = {"S":clubEvent.pictureLink};
	}
	if (clubEvent.ticketsLink != null) {
		item.ticketsLink = {"S":clubEvent.ticketsLink};
	}
	if (clubEvent.endsAt != null) {
		item.endsAt = {"S":clubEvent.endsAt.toString()};
	}

	var params = {
		TableName: "Clubs.Events",
	    Item: item
	};

	dynamodb.putItem(params, function(err, data) {
		if (err) {
			console.log("Save club event to dynamodb failed: " + err);
		}
		saveEventCounter--;
	});
}).to(2).per(1000);

// start
scanTable(null);

var interval = setInterval(function() {
	console.log("findEventsCounter: " + findEventsCounter);
	console.log("getEventDetailsCounter: " + getEventDetailsCounter);
	console.log("saveEventCounter: " + saveEventCounter);
}, 30000);