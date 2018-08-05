const MongoClient = require('mongodb').MongoClient;
var ObjectId = require("mongodb").ObjectId;
var elasticsearch = require('elasticsearch');



var localDbUrl = "mongodb://data_root:xyz1$3^nhy7@104.197.218.69:27017/admin";
// var liveDbUrl = "mongodb://data_root:xyz1$3^nhy7@104.197.218.69:27017/admin";
var liveDbUrl = "mongodb://dp_root:xyz1236tgbnhy7@107.21.99.225:27017/admin";

var returnMessage = [];

var country_wise_database = {
    "us": {
        projects_database: "data_us",
        projects_collection: "leads_with_url",
        business_database: "data_us",
        business_collection: "leads_without_url"
    },
    "gb": {
        projects_database: "data_gb",
        projects_collection: "leads_with_url",
        business_database: "data_gb",
        business_collection: "leads_without_url"
    },
    "au": {
        projects_database: "data_au",
        projects_collection: "leads_with_url",
        business_database: "data_au",
        business_collection: "leads_without_url"
    },
    "nz": {
        projects_database: "data_nz",
        projects_collection: "leads_with_url",
        business_database: "data_nz",
        business_collection: "leads_without_url"
    },
    "in": {
        projects_database: "data_in",
        projects_collection: "leads_with_url",
        business_database: "data_in",
        business_collection: "leads_without_url"
    }
}

var MongoSettings = {
    // staging_database: "data",
    // staging_collection: "dataproc_lac",
    // live_database: "data",
    // live_collection: "dataproc_lac",
    // staging: {
    //     projects_database: "data",
    //     projects_collection: "dataproc_lac",
    //     business_database: "data_us",
    //     business_collection: "leads_without_url"
    // },
    live: {
        projects_database: "ds",
        projects_collection: "ds_projects",
        business_database: "dp_business",
        business_collection: "business"
    }
};

var ElasticSetting = {
    Host: "107.21.99.225:9200",
    requestTimeout: 50000000,
    index: "dp_projects,dp_business",
    type: "projects,business"
}

var field_mapping = {
    "dp_projects": "leads_with_url",
    "dp_business": "leads_without_url"
}

var elasticClient = new elasticsearch.Client({
    host: ElasticSetting.Host,
    requestTimeout: ElasticSetting.requestTimeout
});

module.exports = async function (req, res) {

    var request = req.body;

    try {
        console.log('Before findDoc');
        var findDoc = await findDocumentInFP(request);
        if (findDoc === 0) {
            let result = {
                LiveElastic: {
                    status: 0,
                    message: "No Listing Found in Live Elastic"
                }
            }
            res.json(result);
        }
        request._index = findDoc._index;
        request._type = findDoc._type;
        if (findDoc._source.address.country_code == null || findDoc._source.address.country_code == 'undefined') {
            returnMessage.status = 500;
            returnMessage.message = "This listing doesn't have Country code";
            return returnMessage;
        }
        request.country_code = findDoc._source.address.country_code;
        var respose = await Promise.all([
            updateStagingMongo(request),
            deleteFromLiveMongo(request),
            deleteElasticDoc(request)
        ]).then(function (response) {
            res.json(response);
        });
    } catch (E) {
        // console.log(E.message);
        res.json(E.message``);
    }
}


// Find listing in FP

function findDocumentInFP(doc) {

    // console.log("inside document");
    // console.log(doc)

    return new Promise(async function (resolve, reject) {
        try {
            // console.log(doc.fp_id);
            let findDoc = await elasticClient.search({
                index: ElasticSetting.index,
                type: ElasticSetting.type,
                body: {
                    query: {
                        filtered: {
                            filter: {
                                and: [
                                    { term: { _id: doc.fp_id } }
                                ]
                            }
                        }
                    }
                }
            });

            console.log(findDoc.hits.total);
            console.log('findDoc');
            if (findDoc.hasOwnProperty("hits")) {
                if (findDoc.hits.hasOwnProperty("total") && findDoc.hits.total >= 1) {
                    resolve(findDoc.hits.hits[0]);
                } else
                    resolve(findDoc.hits.total);
            }
            else
                resolve(0);
        } catch (E) {
            reject(E.message);
        }
    });

}


// Document delete from Live Elastic

function deleteElasticDoc(doc) {
    return new Promise(async function (resolve, reject) {
        try {
            var deleteDoc = await elasticClient.delete({
                index: doc._index,
                type: doc._type,
                id: doc.fp_id
            });
            if (deleteDoc.hasOwnProperty("found")) {
                let result = {
                        type: "LiveElastic",
                        status: 1,
                        message: "Delete listing from Live Elastic"
                }
                resolve(result);
            }
            else {
                let result = {
                        type: "LiveElastic",
                        status: 0,
                        message: "Listing is not deleted from Live Elastic"
                }
                resolve(result);
            }

        } catch (E) {
            let result = {
                    type: "LiveElastic",
                    status: 0,
                    message: E.message
            }
            reject(result);
        }

    })
}

// Delete listing from Live Mongo

function deleteFromLiveMongo(request) {
    return new Promise(async function (resolve, reject) {
        try {
            var database = country_wise_database[request.country_code];
            var FP_ID = new ObjectId(request.fp_id);
            var local_database = await MongoClient.connect(liveDbUrl, { connectTimeoutMS: 90000, socketTimeoutMS: 90000 });

            let D_B = (request._index == 'dp_projects') ? MongoSettings.live.projects_database : MongoSettings.live.business_database;
            let collection = (request._type == 'projects') ? MongoSettings.live.projects_collection : MongoSettings.live.business_collection;

            var dbo = local_database.db(D_B);
            var query = { _id: FP_ID };

            var res = await dbo.collection(collection).remove(query);

            if (res.hasOwnProperty('result')) {
                //res.result.n
                let result = {
                        type: "LiveMongo",
                        status: 1,
                        message: "Delete listing from Live Mongo"
                }
                resolve(result);

            } else {
                let result = {
                        type: "LiveMongo",
                        status: 0,
                        message: "Listing not deleted Live Mongo"
                }
                resolve(result);
            }

        } catch (E) {
            let result = {
                type: "LiveMongo",
                status: 0,
                message: E.message
            }
            reject(result);
        }

    });
}

// Update Document status in local Mongo

function updateStagingMongo(request) {
    return new Promise(async function (resolve, reject) {
        try {
            var database = country_wise_database[request.country_code];

            var FP_ID = new ObjectId(request.fp_id);
            var status_code = request.status_code;
            var local_database = await MongoClient.connect(localDbUrl, { connectTimeoutMS: 90000, socketTimeoutMS: 90000 });
            let D_B = (request._index == 'dp_projects') ? database.projects_database : database.business_database;
            let collection = (request._type == 'projects') ? database.projects_collection : database.business_collection;

            var dbo = local_database.db(D_B);
            var query = { _id: FP_ID };
            var res = await dbo.collection(collection).update(query, { $set: { "domain_data.valid": status_code } });
            if (res.hasOwnProperty('result')) {
                // res.result.nModified
                let result = {
                    type: "Staging Mongo",
                    status: 1,
                    message: "Listing has been updated in staging Server"
                }
                resolve(result);
            } else {
                let result = {
                    type: "Staging Mongo",
                    status: 0,
                    message: "Listing not updated in staging Server"
                }
                resolve(result);
            }

        } catch (E) {
            let result = {
                type: "Staging Mongo",
                status: 0,
                message: E.message
            }
            reject(result);
        }

    });

}

