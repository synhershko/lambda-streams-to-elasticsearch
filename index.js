/*
 AWS Streams to Elasticsearch

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

var debug = process.env['DEBUG_ENABLED'] || false;

var pjson = require('./package.json');
var setRegion = process.env['AWS_REGION'];
var deagg = require('aws-kpl-deagg');
var async = require('async');

var elasticsearch = require('elasticsearch');

var aws = require('aws-sdk');

var kinesis;
exports.kinesis = kinesis;
var online = false;

require('./constants');

/* Configure transform utility */
var transform = require('./transformer');
/*
 * create the transformer instance - change this to be regexToDelimter, or your
 * own new function
 */
var useTransformer = transform.jsonToStringTransformer.bind(undefined);

/*
 * Configure destination router. By default all records route to the configured
 * stream
 */
var router = require('./router');

/*
 * create the routing rule reference that you want to use. This shows the
 * default router
 */
var useRouter = router.defaultRouting.bind(undefined);
// example for using routing based on messages attributes
// var attributeMap = {
// "binaryValue" : {
// "true" : "TestRouting-route-A",
// "false" : "TestRouting-route-B"
// }
// };
// var useRouter = router.routeByAttributeMapping.bind(undefined, attributeMap);

// should KPL checksums be calculated?
var computeChecksums = true;

/*
 * If the source Kinesis Stream's tags or DynamoDB Stream Name don't resolve to
 * an existing Firehose, allow usage of a default delivery stream, or fail with
 * an error.
 */
var USE_DEFAULT_DELIVERY_STREAMS = false;
/*
 * Delivery stream mappings can be specified here to overwrite values provided
 * by Kinesis Stream tags or DynamoDB stream name. (Helpful for debugging)
 * Format: DDBStreamName: deliveryStreamName Or: FORWARD_TO_ELASTICSEARCH_DNS tag
 * value: deliveryStreamName
 */
var elasticsearchDestinations = {
    'DEFAULT' : 'http://localhost:9200' // Yeah, this will error
};

function init() {
    if (!online) {
        if (!setRegion || setRegion === null || setRegion === "") {
            setRegion = "us-east-1";
            console.warn("Warning: Setting default region " + setRegion);
        }

        if (debug) {
            console.log("AWS Streams to Firehose Forwarder v" + pjson.version + " in " + setRegion);
        }

        aws.config.update({
            region : setRegion
        });

        // configure a new connection to kinesis streams, if one has not been
        // provided
        if (!exports.kinesis) {
            if (debug) {
                console.log("Connecting to Amazon Kinesis Streams in " + setRegion);
            }
            exports.kinesis = new aws.Kinesis({
                apiVersion : '2013-12-02',
                region : setRegion
            });
        }

        online = true;
    }
}

/** function to extract the kinesis stream name from a kinesis stream ARN */
function getStreamName(arn) {
    try {
        var eventSourceARNTokens = arn.split(":");
        return eventSourceARNTokens[5].split("/")[1];
    } catch (e) {
        console.log("Malformed Kinesis Stream ARN");
    }
}
exports.getStreamName = getStreamName;

function onCompletion(context, event, err, status, message) {
    console.log("Processing Complete");

    if (err) {
        console.log(err);
    }

    // log the event if we've failed
    if (status !== OK) {
        if (message) {
            console.log(message);
        }

        // ensure that Lambda doesn't checkpoint to kinesis on error
        context.done(status, JSON.stringify(message));
    } else {
        context.done(null, message);
    }
}
exports.onCompletion = onCompletion;

/** AWS Lambda event handler */
function handler(event, context) {
    // add the context and event to the function that closes the lambda
    // invocation
    var finish = onCompletion.bind(undefined, context, event);

    /** End Runtime Functions */
    if (debug) {
        console.log(JSON.stringify(event));
    }

    // fail the function if the wrong event source type is being sent, or if
    // there is no data, etc
    var noProcessStatus = ERROR;
    var noProcessReason;

    if (!event.Records || event.Records.length === 0) {
        noProcessReason = "Event contains no Data";
        // not fatal - just got an empty event
        noProcessStatus = OK;
    } else {
        // there are records in this event
        var serviceName;
        if (event.Records[0].eventSource === KINESIS_SERVICE_NAME) {
            serviceName = event.Records[0].eventSource;
        } else {
            noProcessReason = "Invalid Event Source " + event.Records[0].eventSource;
        }

        // currently hard coded around the 1.0 kinesis event schema
        if (event.Records[0].kinesis && event.Records[0].kinesis.kinesisSchemaVersion !== "1.0") {
            noProcessReason = "Unsupported Kinesis Event Schema Version " + event.Records[0].kinesis.kinesisSchemaVersion;
        }
    }

    if (noProcessReason) {
        // terminate if there were any non process reasons
        finish(noProcessStatus, ERROR, noProcessReason);
    } else {
        init();

        // parse the stream name out of the event
        var streamName = exports.getStreamName(event.Records[0].eventSourceARN);

        // create the processor to handle each record
        var processor = exports.processEvent.bind(undefined, event, serviceName, streamName, function(err) {
            if (err) {
                finish(err, ERROR, "Error Processing Records");
            } else {
                finish(undefined, OK);
            }
        });

        if (elasticsearchDestinations.length === 0 || !elasticsearchDestinations[streamName]) {
            // no delivery stream cached so far, so add this stream's tag value
            // to the delivery map, and continue with processEvent
            exports.buildDeliveryMap(streamName, serviceName, context, event, processor);
        } else {
            // delivery stream is cached so just invoke the processor
            processor();
        }
    }
}
exports.handler = handler;

/**
 * Function which resolves the destination delivery stream for a given Kinesis
 * stream. If no delivery stream is found to deliver to, then we will cache the
 * default delivery stream
 *
 * @param streamName
 * @param shouldFailbackToDefaultDeliveryStream
 * @param event
 * @param callback
 * @returns
 */
function verifyDeliveryStreamMapping(streamName, shouldFailbackToDefaultDeliveryStream, context, event, callback) {
    if (debug) {
        console.log('Verifying delivery stream');
    }
    if (!elasticsearchDestinations[streamName]) {
        if (shouldFailbackToDefaultDeliveryStream) {
            /*
             * No delivery stream has been specified, probably as it's not
             * configured in stream tags. Using default delivery stream. To
             * prevent accidental forwarding of streams to a firehose set
             * USE_DEFAULT_DELIVERY_STREAMS = false.
             */
            elasticsearchDestinations[streamName] = elasticsearchDestinations['DEFAULT'];
        } else {
            /*
             * Fail as no delivery stream mapping has been specified and we have
             * not configured to use a default. Kinesis Streams should be tagged
             * with ForwardToFirehoseStream = <DeliveryStreamName>
             */
            exports.onCompletion(context, event, undefined, ERROR, "Warning: Kinesis Stream " + streamName + " not tagged for Firehose delivery with Tag name " + FORWARD_TO_FIREHOSE_STREAM);
        }
    }

    if (!exports.es_client) {
        exports.es_client = new elasticsearch.Client({
            host: elasticsearchDestinations[streamName]
        });
    }

    if (!exports.es_client) {
        exports.onCompletion(context, event, undefined, ERROR, "No es_client defined, quitting");
        return;
    }

    exports.es_client.ping({
        requestTimeout: 1000
    }, function (error) {
        if (error) {
            console.error('Elasticsearch cluster is inaccessible '  + elasticsearchDestinations[streamName]);
            exports.onCompletion(context, event, undefined, ERROR, 'Elasticsearch cluster is inaccessible '  + elasticsearchDestinations[streamName]);
        } else {
            console.log('Connected to Elasticsearch on ' + elasticsearchDestinations[streamName]);

            // call the specified callback - should have
            // already
            // been prepared by the calling function
            callback();
        }
    });
}
exports.verifyDeliveryStreamMapping = verifyDeliveryStreamMapping;

/**
 * Function which resolves the destination delivery stream from the specified
 * Kinesis Stream Name, using Tags attached to the Kinesis Stream
 */
function buildDeliveryMap(streamName, serviceName, context, event, callback) {
    if (debug) {
        console.log('Building delivery stream mapping');
    }
    if (elasticsearchDestinations[streamName]) {
        // A delivery stream has already been specified in configuration
        // This could be indicative of debug usage.
        exports.verifyDeliveryStreamMapping(streamName, false, event, callback);
    } else {
        console.log('Trying to get list of tags of stream ' + streamName);
        // get the delivery stream name from Kinesis tag
        exports.kinesis.listTagsForStream({
            StreamName : streamName
        }, function(err, data) {
            shouldFailbackToDefaultDeliveryStream = USE_DEFAULT_DELIVERY_STREAMS;
            if (err) {
                exports.onCompletion(context, event, err, ERROR, "Unable to List Tags for Stream");
            } else {
                console.log('Got tags of stream ' + streamName);
                // grab the tag value if it's the foreward_to_elasticsearch
                // name item
                data.Tags.map(function(item) {
                    if (item.Key === FORWARD_TO_ELASTICSEARCH_DNS) {
                        /*
                         * Disable fallback to a default delivery stream as a
                         * FORWARD_TO_ELASTICSEARCH_DNS has been specifically set.
                         */
                        shouldFailbackToDefaultDeliveryStream = false;
                        elasticsearchDestinations[streamName] = item.Value;
                    }
                });

                exports.verifyDeliveryStreamMapping(streamName, shouldFailbackToDefaultDeliveryStream, context, event, callback);
            }
        });
    }
}
exports.buildDeliveryMap = buildDeliveryMap;

/**
 * Convenience function which generates the batch set with low and high offsets
 * for pushing data to Elasticsearch in blocks of FIREHOSE_MAX_BATCH_COUNT
 * Batch ranges are calculated to be compatible with the array.slice() function which uses a
 * non-inclusive upper bound
 */
function getBatchRanges(records) {
    var batches = [];
    var currentLowOffset = 0;
    var batchCurrentBytes = 0;
    var batchCurrentCount = 0;
    var recordSize;

    for (var i = 0; i < records.length; i++) {
        // need to calculate the total record size for the call to Firehose on
        // the basis of of non-base64 encoded values
        recordSize = Buffer.byteLength(records[i].toString(targetEncoding), targetEncoding);

        // batch always has 1 entry, so add it first
        batchCurrentBytes += recordSize;
        batchCurrentCount += 1;

        // generate a new batch marker every 4MB or 500 records, whichever comes
        // first
        if (batchCurrentCount === ELASTICSEARCH_MAX_BATCH_COUNT || i === records.length - 1) {
            batches.push({
                lowOffset : currentLowOffset,
                // annoying special case handling for record sets of size 1
                highOffset : i + 1,
                sizeBytes : batchCurrentBytes
            });
            // reset accumulators
            currentLowOffset = i + 1;
            batchCurrentBytes = 0;
            batchCurrentCount = 0;
        }
    }

    return batches;
}
exports.getBatchRanges = getBatchRanges;

/**
 * Function to process a stream event and generate requests to forward the
 * embedded records to Kinesis Firehose. Before delivery, the user specified
 * transformer will be invoked, and the messages will be passed through a router
 * which can determine the delivery stream dynamically if needed
 */
function processEvent(event, serviceName, streamName, callback) {
    if (debug) {
        console.log('Processing event');
    }
    // look up the delivery stream name of the mapping cache
    var deliveryStreamName = elasticsearchDestinations[streamName];

    if (debug) {
        console.log("Forwarding " + event.Records.length + " " + serviceName + " records to Delivery Stream " + deliveryStreamName);
    }

    async.map(event.Records, function(record, recordCallback) {
        // resolve the record data based on the service
        if (serviceName === KINESIS_SERVICE_NAME) {
            // run the record through the KPL deaggregator
            deagg.deaggregateSync(record.kinesis, computeChecksums, function(err, userRecords) {
                // userRecords now has all the deaggregated user records, or
                // just the original record if no KPL aggregation is in use
                if (err) {
                    recordCallback(err);
                } else {
                    recordCallback(null, userRecords);
                }
            });
        }
    }, function(err, extractedUserRecords) {
        if (err) {
            callback(err);
        } else {
            // extractedUserRecords will be array[array[Object]], so
            // flatten to array[Object]
            var userRecords = [].concat.apply([], extractedUserRecords);

            // transform the user records
            transform.transformRecords(serviceName, useTransformer, userRecords, function(err, transformed) {
                // apply the routing function that has been configured
                router.routeToDestination(deliveryStreamName, transformed, useRouter, function(err, routingDestinationMap) {
                    if (err) {
                        // we are still going to route to the default stream
                        // here, as a bug in routing implementation cannot
                        // result in lost data!
                        console.log(err);

                        // discard the delivery map we might have received
                        routingDestinationMap[deliveryStreamName] = transformed;
                    }
                    // send the routed records to the delivery processor
                    async.map(Object.keys(routingDestinationMap), function(destinationStream, asyncCallback) {
                        var records = routingDestinationMap[destinationStream];

                        processFinalRecords(records, streamName, destinationStream, asyncCallback);
                    }, function(err, results) {
                        if (err) {
                            callback(err);
                        } else {
                            if (debug) {
                                results.map(function(item) {
                                    console.log(JSON.stringify(item));
                                });
                            }
                            callback();
                        }
                    });

                });
            });
        }
    });
}
exports.processEvent = processEvent;

function writeToElasticsearch(items, streamName, deliveryStreamName, callback) {
    var startTime = new Date();
    var indexName = 'logstash-' + startTime.toISOString().slice(0,10).replace('-', '.'); // TODO configurable index prefix name

    var body = [];
    items.map(function(item) {
        body.push({ index:  { _index: indexName, _type: 'impressions' } });
        body.push(item); // item is assumed to be JSON
    });

    if (debug) {
        console.log('Writing to Elasticsearch');
        console.log(JSON.stringify(items));
    }

    // bulk API docs at https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/api-reference.html
    exports.es_client.bulk({
        body: body
    }, function (err, resp) {
        if (err) {
            console.log(JSON.stringify(err));
            callback(err);
        } else {
            // TODO verify resp and no batch failures
            if (debug) {
                var elapsedMs = new Date().getTime() - startTime.getTime();
                console.log("Successfully wrote " + items.length + " records to Elasticsearch in " + elapsedMs + " ms");
            }
            callback();
        }
    });
}
exports.writeToElasticsearch = writeToElasticsearch;

/**
 * function which handles the output of the defined transformation on each
 * record.
 */
function processFinalRecords(records, streamName, deliveryStreamName, callback) {
    if (debug) {
        console.log('Delivering records to destination Streams');
    }
    // get the set of batch offsets based on the transformed record sizes
    var batches = exports.getBatchRanges(records);

    if (debug) {
        console.log(JSON.stringify(batches));
    }

    // push to Firehose using PutRecords API at max record count or size.
    // This uses the async reduce method so that records from Kinesis will
    // appear in the Firehose PutRecords request in the same order as they
    // were received by this function
    async.reduce(batches, 0, function(successCount, item, reduceCallback) {
        if (debug) {
            console.log("Forwarding records " + item.lowOffset + ":" + item.highOffset + " - " + item.sizeBytes + " Bytes");
        }

        // grab subset of the records assigned for this batch and push to
        // firehose
        var processRecords = records.slice(item.lowOffset, item.highOffset);

        exports.writeToElasticsearch(processRecords, streamName, deliveryStreamName, function(err) {
            if (err) {
                reduceCallback(err, successCount);
            } else {
                reduceCallback(null, successCount + 1);
            }
        });
    }, function(err, successfulBatches) {
        if (err) {
            console.log("Forwarding failure after " + successfulBatches + " successful batches");
            callback(err);
        } else {
            console.log("Event forwarding complete. Forwarded " + successfulBatches + " batches comprising " + records.length + " records to Firehose " + deliveryStreamName);
            callback(null, {
                "deliveryStreamName" : deliveryStreamName,
                "batchesDelivered" : successfulBatches,
                "recordCount" : records.length
            });
        }
    });
}
exports.processFinalRecords = processFinalRecords;