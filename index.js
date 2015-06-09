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
var totalClubsCounter = 0;
var totalClubsWithFacebookIdCounter = 0;
var findEventsCounter = 0;
var getEventDetailsCounter = 0;
var saveEventCounter = 0;

//Set AWS configuration for future requests.
aws.config.update({"accessKeyId": AWS_ACCESS_KEY_ID, "secretAccessKey": AWS_SECRET_ACCESS_KEY, "region": "eu-west-1"});
aws.config.apiVersions = {
  dynamodb: '2012-08-10'
};

var dynamodb = new aws.DynamoDB();
var now = moment(new Date(new Date().getTime() + 12 * 60 * 60 * 1000));

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
		    	totalClubsCounter = data["Count"];
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
		    	totalClubsCounter = totalClubsCounter + data["Count"];
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
        	totalClubsWithFacebookIdCounter++;
        	findEvents(item.Id.S, item.FacebookId.S, 1);
        }
	}
}

function findEvents(clubId, pageId, tryNumber) {
	findEventsCounter++;

	facebookPageEventsAPI(pageId, function(error, response, body) {
		if (!error && response.statusCode === 200) {
			var json = JSON.parse(body.toString());
			json.data.forEach(function(item) {
				var start = moment(item.start_time.substring(0, 16)).utcOffset(item.start_time);
				if (now.isBefore(start)) {
					getEventDetails(clubId, item.id, 1);
				} else {
					console.log("Event too old: " + start);
					console.log("Event id: " + item.id);
				}
			});
			findEventsCounter--;
		} else {
			if (response !== undefined && response.statusCode === 500 && tryNumber <= MAX_QUERY_TRIES) {
				tryNumber++;
				setTimeout(function() {
					findEvents(pageId, tryNumber);
				}, QUERY_RETRY_TIMEOUT);
			} else if (response !== undefined && response.statusCode === 400) {
				var json = JSON.parse(body.toString());
				if (json.error != null && json.error.code != null && json.error.code === 21) {
					// expired club facebook id (time to replace it)
					var newFacebookId = json.error.message.split(" ")[8].replace(".", "");
					console.log("Saving new facebook ID: " + newFacebookId);
					updateClubApi(clubId, newFacebookId);
				}
				findEventsCounter--;
			} else {
				if (response !== undefined) {
					if (error !== null && error !== undefined) {
						console.log("findEvents, " + error + ", STATUS CODE: " + response.statusCode);
						console.log("clubId: " + clubId + ", pageId: " + pageId);
					} else {
						console.log("findEvents, STATUS CODE: " + response.statusCode);
						console.log("clubId: " + clubId + ", pageId: " + pageId);
					}
				} else {
					if (error !== null && error !== undefined) {
						console.log("findEvents, " + error);
						console.log("clubId: " + clubId + ", pageId: " + pageId);
					} else {
						console.log("findEvents, something failed but no error and response objects");
						console.log("clubId: " + clubId + ", pageId: " + pageId);
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
						console.log("clubId: " + clubId + ", eventId: " + eventId);
					} else {
						console.log("getEventDetails, STATUS CODE: " + response.statusCode);
						console.log("clubId: " + clubId + ", eventId: " + eventId);
					}
				} else {
					if (error !== null && error !== undefined) {
						console.log("getEventDetails, " + error);
						console.log("clubId: " + clubId + ", eventId: " + eventId);
					} else {
						console.log("getEventDetails, something failed but no error and response objects");
						console.log("clubId: " + clubId + ", eventId: " + eventId);
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

	var start = moment(event.start_time.substring(0, 16)).utcOffset(event.start_time);
	var updated = moment(event.updated_time.substring(0, 16)).utcOffset(event.updated_time);
	clubEvent.startsAt = start.valueOf().toString();
	clubEvent.updatedAt = updated.valueOf().toString();

	if (event.cover != null) {
		clubEvent.pictureLink = event.cover.source;
	}
	if (event.ticket_uri != null) {
		clubEvent.ticketsLink = event.ticket_uri;
	}
	if (event.end_time != null) {
		var ends = moment(event.end_time.substring(0, 16)).utcOffset(event.end_time);
		clubEvent.endsAt = ends.valueOf().toString();

		// do not save events that last more than 3 days
		if (start.diff(ends,'days') > 3) {
			console.log("Returning without saving, event lasts " + start.diff(ends,'days') + " days");
			return;
		}
	}
	saveEventApi(clubEvent);
}

// save item
var saveEventApi = limit(function(clubEvent) {
	var item = {
		"Id":{"S":clubEvent.id.toString()},
		"ClubId":{"S":clubEvent.clubId},
		"Name":{"S":clubEvent.name},
		"StartsAt":{"S":clubEvent.startsAt},
		"UpdatedAt":{"S":clubEvent.updatedAt}
	}

	if (clubEvent.description != null) {
		item.Description = {"S":clubEvent.description};
	}
	if (clubEvent.pictureLink != null) {
		item.PictureLink = {"S":clubEvent.pictureLink};
	}
	if (clubEvent.ticketsLink != null) {
		item.TicketsLink = {"S":clubEvent.ticketsLink};
	}
	if (clubEvent.endsAt != null) {
		item.EndsAt = {"S":clubEvent.endsAt};
	}

	var params = {
		TableName: "Clubs.Events",
	    Item: item
	};

	dynamodb.putItem(params, function(err, data) {
		if (err) {
			console.log("Save club event to dynamodb failed: " + err);
			console.log(JSON.stringify(clubEvent));
			process.exit();
		}
		saveEventCounter--;
	});
}).to(2).per(1000);

// update item
var updateClubApi = limit(function(id, facebookId) {
	dynamodb.updateItem({
    	"Key": {
    		"Id": {"S":id}
    	},
	    "TableName": "Clubs",
        "UpdateExpression": "SET FacebookId = :a",
	    "ExpressionAttributeValues" : {
	    	":a" : {"S":facebookId}
	    }
	}, function(err, data) {
	  	if (err) {
	  		console.log(err, err.stack);
		    process.exit();
	  	} else {
	  		findEvents(id, facebookId, 1);
	  	}
	});
}).to(5).per(1000);

// start
scanTable(null);

var interval = setInterval(function() {
	console.log("totalClubsCounter: " + totalClubsCounter);
	console.log("totalClubsWithFacebookIdCounter: " + totalClubsWithFacebookIdCounter);
	console.log("findEventsCounter: " + findEventsCounter);
	console.log("getEventDetailsCounter: " + getEventDetailsCounter);
	console.log("saveEventCounter: " + saveEventCounter);

	if (findEventsCounter == 0 && getEventDetailsCounter == 0 && saveEventCounter == 0) {
		clearInterval(interval);
	}
}, 30000);