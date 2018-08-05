/*
 * Description : This file will move mongo data from Local to Live mongo
 * Created : April 23 2018
 * Author : Balu G  & Tulsiram
 * Email : balu@buzzboard.com
 * 
 */

const MongoClient = require('mongodb').MongoClient;
var ObjectId = require("mongodb").ObjectId;
const cluster = require('cluster');
var formatPhoneNumbers = require('./format_phonenumber.js');
const ElasticLib = require("../bin/elastic.lib.js");

var localDbUrl = "mongodb://data_root:xyz1$3^nhy7@104.197.218.69:27017/admin";
// var liveDbUrl = "mongodb://data_root:xyz1$3^nhy7@104.197.218.69:27017/admin";
// var liveDbUrl = "mongodb://dp_root:xyz1236tgbnhy7@107.21.99.225:27017/admin";
var liveDbUrl = "mongodb://dp_root:xyz1236tgbnhy7@107.22.170.45:27017/admin"; // Demo 45 server

const NUMBER_CHILD_PROCESS = 1; // Don't change this value untill you discuss with Author of this file

const ELASTIC_BATCH_SIZE = 1000;

const ElasticSetting = {
    host: "107.22.170.45:9200",
    index: "dp_projects_nodejs",
    type: "project"
}

var MongoSettings = {
    local: {
        projects_database: "data",
        projects_collection: "leads_with_url_with_lac",
        business_database: "data_us",
        business_collection: "leads_without_url"
    },
    live: {
        projects_database: "ds",
        projects_collection: "ds_projects_sample",
        business_database: "dp_business",
        business_collection: "business_sample"
    }
};

const limit = 5;

/************* unset fields ********/

const exclude_objects_updated = {

    "_parent_id": 0,
    "_duplicate": 0,
    "directory_listing": 0,
    "additional_info": 0,
    "_verified": 0,
    "yp_data": 0,
    "_status": 0,
    "page_data": 0,
    "log": 0,
    "google_places_api": 0,
    "domain_data_log": 0,
    "business.categories": 0,
    "business.category_name1": 0,
    "business.contact_name": 0,
    "business.contact_designation": 0,
    "business.contact_details": 0,
    "business.contacts_count": 0,
    "business.contact_person": 0,
    "business.correctly_placed": 0,
    "business.databridge_contacts_count": 0,
    "business.databridge": 0,
    "business.dl_category": 0,
    "business.dl_category_labels": 0,
    "business.dl_category_name": 0,
    "business.elst_category_ids": 0,
    "business.elst_category_names": 0,
    "business.elst_category_score": 0,
    "business.primary_phone": 0,
    "changes": 0,
    "_domain_data_log": 0,
    "data": 0,
    "data_bridge": 0,
    "_Adwords_log": 0,
    "additional_information": 0,
    "data_info.additional_information": 0,
    "data_info._Adwords_log": 0,
    "data_info.address": 0,
    "data_info.bing_data": 0,
    "data_info.bm": 0,
    "data_info.business": 0,
    "data_info.contact_info": 0,
    "data_info.data_source_old": 0,
    "data_info.domain_data": 0,
    "data_info.dates": 0,
    "data_info.dp_moved": 0,
    "data_info.elastic_index_failed": 0,
    "data_info.facutal_id": 0,
    "data_info.fp_score": 0,
    "data_info.hostname": 0,
    "data_info.location": 0,
    "data_info.neustar_mongo_id": 0,
    "data_info.page_analysis": 0,
    "data_info.social": 0,
    "data_info.technologies": 0,
    "databridge": 0,
    "domain_data.data_source": 0,
    "elastic_index_failed": 0,
    "elastic_update_index_failed": 0,
    "es_multiloc_missing": 0,
    "found": 0,
    "google_places": 0,
    "http_status_codes": 0,
    "neustar": 0,
    "page_analysis.technologies": 0,
    "page_shoppingcart": 0,
    "track": 0,
    "preferred_canvasser": 0,
    "sensis_categories": 0,
    "update_phone": 0,
    "verified": 0,
    "add_info": 0,
    "duplicate_business_ran": 0,
    "local_presence": 0,
    "yslow_page_performance": 0,
    "_source": 0,
    "di_domain_name_lower": 0,
    "raw_data": 0,
    "analysis_scripts": 0,
    "_fp_log": 0,
    "_log_new": 0,
    "geometry": 0,
    "tags": 0,
    "_messages": 0,
    "address_old": 0,
    "duplicate": 0,
    "messages": 0,
    "page_analysis.google_pagerank": 0,
    "additional_information.neustar_category_ids": 0,
    "additional_information.neustar_category_name": 0
};


function formatfields(doc) {
    try {
        /**adwords_data */
        function adwords_data(adwords_data) {
            if (adwords_data) {
                /**semrush */
                if (adwords_data['semrush']) {

                    /**adwords_cost */
                    if (adwords_data['semrush'].hasOwnProperty('adwords_cost')) {
                        adwords_data['semrush']['AdwordsCost'] = adwords_data['semrush']['adwords_cost'];
                        delete adwords_data['semrush']['adwords_cost'];
                    }
                    /**adwords_keywords */
                    if (adwords_data['semrush'].hasOwnProperty('adwords_keywords')) {
                        adwords_data['semrush']['AdwordsKeywords'] = adwords_data['semrush']['adwords_keywords'];
                        delete adwords_data['semrush']['adwords_keywords'];
                    }

                    /**adwords_traffic */
                    if (adwords_data['semrush'].hasOwnProperty('adwords_traffic')) {
                        adwords_data['semrush']['AdwordsTraffic'] = adwords_data['semrush']['adwords_traffic'];
                        delete adwords_data['semrush']['adwords_traffic'];
                    }

                    /**organic_cost */
                    if (adwords_data['semrush'].hasOwnProperty('organic_cost')) {
                        adwords_data['semrush']['OrganicCost'] = adwords_data['semrush']['organic_cost'];
                        delete adwords_data['semrush']['organic_cost'];
                    }
                    /**organic_keywords */
                    if (adwords_data['semrush'].hasOwnProperty('organic_keywords')) {
                        adwords_data['semrush']['OrganicKeywords'] = adwords_data['semrush']['organic_keywords'];
                        delete adwords_data['semrush']['organic_keywords'];
                    }
                    /**organic_traffic */
                    if (adwords_data['semrush'].hasOwnProperty('organic_traffic')) {
                        adwords_data['semrush']['OrganicTraffic'] = adwords_data['semrush']['organic_traffic'];
                        delete adwords_data['semrush']['organic_traffic'];
                    }
                }
                return adwords_data;
            }
        }


        /** domain_data */
        function domain_data(domain_data) {
            if (domain_data) {
                if (domain_data.hasOwnProperty('http_redirects') &&
                    domain_data['http_redirects'].constructor === Array &&
                    domain_data['http_redirects'].length === 0) {
                    delete domain_data['http_redirects'];
                }
                return domain_data;
            }
        }

        /**bing_data */
        function bing_data(bing_data) {

            if (bing_data) {

                /**semrush */
                if (bing_data['semrush']) {

                    /**adwords_cost */
                    if (bing_data['semrush'].hasOwnProperty('adwords_cost')) {
                        bing_data['semrush']['AdwordsCost'] = bing_data['semrush']['adwords_cost'];
                        delete bing_data['semrush']['adwords_cost'];
                    }
                    /**adwords_keywords */
                    if (bing_data['semrush'].hasOwnProperty('adwords_keywords')) {
                        bing_data['semrush']['AdwordsKeywords'] = bing_data['semrush']['adwords_keywords'];
                        delete bing_data['semrush']['adwords_keywords'];
                    }

                    /**adwords_traffic */
                    if (bing_data['semrush'].hasOwnProperty('adwords_traffic')) {
                        bing_data['semrush']['AdwordsTraffic'] = bing_data['semrush']['adwords_traffic'];
                        delete bing_data['semrush']['adwords_traffic'];
                    }

                    /**organic_cost */
                    if (bing_data['semrush'].hasOwnProperty('organic_cost')) {
                        bing_data['semrush']['OrganicCost'] = bing_data['semrush']['organic_cost'];
                        delete bing_data['semrush']['organic_cost'];
                    }
                    /**organic_keywords */
                    if (bing_data['semrush'].hasOwnProperty('organic_keywords')) {
                        bing_data['semrush']['OrganicKeywords'] = bing_data['semrush']['organic_keywords'];
                        delete bing_data['semrush']['organic_keywords'];
                    }
                    /**organic_traffic */
                    if (bing_data['semrush'].hasOwnProperty('organic_traffic')) {
                        bing_data['semrush']['OrganicTraffic'] = bing_data['semrush']['organic_traffic'];
                        delete bing_data['semrush']['organic_traffic'];
                    }


                }
                return bing_data;
            }

        }

        /** business */
        function business(business) {

            if (business) {

                /**email */
                if (business['email']) {

                    if (business['emails'] && business['emails'].constructor === Array) {
                        Array.prototype.push.apply(business['emails'], business['email']);
                    } else {
                        business['emails'] = [];
                        business['emails'] = business['email'];
                    }
                    delete business['email'];
                }

                /**fax */
                if (business['fax']) {
                    if (business['fax'] && business['fax'].constructor === Array) {
                        Array.prototype.push.apply(business['fax_numbers'], business['fax']);
                    } else {
                        business['fax_numbers'] = [];
                        business['fax_numbers'] = business['fax'];
                    }
                    delete business['fax'];
                }

                /**phone numbers => local*/
                if (business['phone_numbers'] &&
                    business['phone_numbers']['local'] &&
                    business['phone_numbers']['local'].constructor === Array) {


                    if (business['phone_numbers']['local'].length === 0) {
                        delete business['phone_numbers']['local'];
                    } else {

                        business['phone_numbers_local'] ?
                            Array.prototype.push.apply(business['phone_numbers_local'], business['phone_numbers']['local']) :
                            business['phone_numbers_local'] = business['phone_numbers']['local'];

                        delete business['phone_numbers']['local'];
                    }

                }

                /**phone numbers => toll_free*/
                if (business['phone_numbers'] &&
                    business['phone_numbers']['toll_free'] &&
                    business['phone_numbers']['toll_free'].constructor === Array) {

                    business['phone_numbers_toll_free'] ?
                        Array.prototype.push.apply(business['phone_numbers_toll_free'], business['phone_numbers']['toll_free']) :
                        business['phone_numbers_toll_free'] = business['phone_numbers']['toll_free'];

                    delete business['phone_numbers']['toll_free'];
                }

                /**phone numbers */
                if (business['phone_numbers']) {
                    business['phone_numbers'] = formatPhoneNumbers(business['phone_numbers']);
                }



                /**phone numbers_local */
                if (business['phone_numbers_local']) {
                    business['phone_numbers_local'] = formatPhoneNumbers(business['phone_numbers_local']);
                }


                if (business.hasOwnProperty('total_adspend') &&
                    business['total_adspend'] == 0) {
                    delete business['total_adspend'];
                }



                /**category_labels count 0 */
                if (business['category_labels'] &&
                    business['category_labels'].constructor === Array &&
                    business['category_labels'].length === 0) {
                    delete business['category_labels'];
                }



                /**phone numnbers count 0 */
                if (business['phone_numbers'] &&
                    business['phone_numbers'].constructor === Array &&
                    business['phone_numbers'].length === 0) {
                    delete business['phone_numbers'];
                }

                /**emails count 0 */
                if (business['emails'] &&
                    business['emails'].constructor === Array &&
                    business['emails'].length === 0) {
                    delete business['emails'];
                }

                /**listing_source count 0 */
                if (business['listing_source'] &&
                    business['listing_source'].constructor === Array &&
                    business['listing_source'].length === 0) {
                    delete business['listing_source'];
                }

                return business;
            }
        }

        /** address */
        function address(address) {

            if (address) {

                if (address.hasOwnProperty('region')) {
                    address['region'] = address['region'].toLocaleUpperCase();
                }
                return address;
            }
        }

        /**data_info */
        function data_info(data_info) {

            if (data_info) {

                if (data_info.hasOwnProperty('source')) {
                    data_info['data_source'] = data_info['source'];
                    delete data_info['source'];
                }

                if (data_info.hasOwnProperty('data_source') && data_info['data_source'].constructor === Array) {
                    data_info['data_source'] = data_info['data_source'][0].toString();
                }
                return data_info;
            }
        }

        /**social */
        function social(social) {

            if (social) {

                if (social['facebook'] &&
                    social['facebook'].constructor === Array &&
                    social['facebook'].length === 0) {
                    delete social['facebook'];
                }

                if (social['facebook'] &&
                    social['facebook'][0].hasOwnProperty('last_engagement') &&
                    social['facebook'][0]['last_engagement'].constructor === Date) {
                    var temp = social['facebook'][0]['last_engagement'].getTime();
                    social['facebook'][0]['last_engagement'] = "";
                    social['facebook'][0]['last_engagement'] = temp;

                    // echo date('Y-m-dH:i:s',$params['body']['social']['facebook']['last_engagement']->sec);
                }

                if (social['facebook'] &&
                    social['facebook'][0] &&
                    social['facebook'][0].hasOwnProperty('posts_count') &&
                    social['facebook'][0]['posts_count'].constructor === Boolean) {
                    delete social['facebook'][0]['posts_count'];
                }

                if (social['blogs'] &&
                    social['blogs'].constructor === Array &&
                    social['blogs'].length === 0) {
                    delete social['blogs'];
                }


                if (social['flickr'] &&
                    social['flickr'].constructor === Array &&
                    social['flickr'].length === 0) {
                    delete social['flickr'];
                }
                if (social['foursquare'] &&
                    social['foursquare'].constructor === Array &&
                    social['foursquare'].length === 0) {
                    delete social['foursquare'];
                }
                if (social['google_plus'] &&
                    social['google_plus'].constructor === Array &&
                    social['google_plus'].length === 0) {
                    delete social['google_plus'];
                }
                if (social['linkedin'] &&
                    social['linkedin'].constructor === Array &&
                    social['linkedin'].length === 0) {
                    delete social['linkedin'];
                }
                if (social['pinterest'] &&
                    social['pinterest'].constructor === Array &&
                    social['pinterest'].length === 0) {
                    delete social['pinterest'];
                }
                if (social['twitter'] &&
                    social['twitter'].constructor === Array &&
                    social['twitter'].length === 0) {
                    delete social['twitter'];
                }
                if (social['youtube'] &&
                    social['youtube'].constructor === Array &&
                    social['youtube'].length === 0) {
                    delete social['youtube'];
                }


                return social;
            }
        }

        /**page_analysis */
        function page_analysis(page_analysis) {

            if (page_analysis) {
                if (page_analysis.hasOwnProperty('headings_data') &&
                    page_analysis['headings_data'].constructor !== Array) {
                    delete page_analysis['headings_data'];
                }
                return page_analysis;
            }
        }

        /******************************MODIFICATIONS STARTS *********************************** */


        doc['adwords_data'] ? doc['adwords_data'] = adwords_data(doc['adwords_data']) : "";

        doc['domain_data'] ? doc['domain_data'] = domain_data(doc['domain_data']) : "";

        doc['bing_data'] ? doc['bing_data'] = bing_data(doc['bing_data']) : "";

        doc['business'] ? doc['business'] = business(doc['business']) : "";

        doc['address'] ? doc['address'] = address(doc['address']) : "";

        doc['data_info'] ? doc['data_info'] = data_info(doc['data_info']) : "";

        doc['social'] ? doc['social'] = social(doc['social']) : "";

        doc['page_analysis'] ? doc['page_analysis'] = page_analysis(doc['page_analysis']) : "";


        if (doc['_log']) {
            if (doc['_log'].hasOwnProperty("added_date") && doc['_log']['added_date']) {
                if (doc['data_info']) {
                    doc['data_info']['added_date'] = doc['_log']['added_date'];
                }
            }
            delete doc['_log'];
        }

        /******************************MODIFICATIONS ENDS *********************************** */
        return doc;

    } catch (E) {
        console.log(`Error in formatting ${doc._id}=======`, E);
        return { error: E }
    }
}

// init
// if (cluster.isMaster) {
//     // var numWorkers = require('os').cpus().length;
//     for (var i = 0; i < NUMBER_CHILD_PROCESS; i++) {
//         cluster.fork(); //creating child process
//     }
//     cluster.on('exit', function (worker, code, signal) {
//         console.log('Worker ' + worker.process.pid + ' died with code: ' + code + ', and signal: ' + signal);
//         // console.log('Starting a new worker');

//         if (code !== 3) {
//             cluster.fork();
//         }

//     });
// } else {
//     console.log("Intialize the Code");
//     iterateBatch.call();
// }

iterateBatch.call();

function requiredFields(obj) {

    if (!obj.hasOwnProperty('location') && !obj.hasOwnProperty('geometry')) {
        throw new Error("Geo Location is not defined");
    }

    else if (!obj['location'].hasOwnProperty('lat') || !obj['location'].hasOwnProperty('lon')) {
        throw new Error("Geo Location don't have lat or lon");
    }

    else if (!obj['page_analysis'].hasOwnProperty('form')) {
        throw new Error("Form element is not defined");
    }

    else if (!obj['address'].hasOwnProperty('postal_code')) {
        throw new Error("Postal Code element is not defined");
    }
    else {
        return true;
    }

}

function bulkIndex(bulkData, formatFailureIDS) {
    return new Promise(async (resolve, reject) => {
        console.log('=========bulkData===========');
        // let datamovedresult = await ElasticLib.bulkMove(bulkData);
        console.log(JSON.stringify(bulkData));
        // console.log(formatFailureIDS);
        // docArr
        // Elastic update
        // mongo update
    })
}


function iterateBatch() {
    (async function () {

        var local_connection = "";
        var live_connection = "";
        let local_db = "";
        let local_collection = "";

        let live_db = "";
        let live_collection = "";

        var local_dbo = "";
        var live_dbo = "";
        var cursor = "";
        var counter = 1;
        var batchData = [];
        var errorIDS = [];
        var sucessIDS = [];


        var startTime = Date.now();

        local_connection = await MongoClient.connect(localDbUrl, { connectTimeoutMS: 90000, socketTimeoutMS: 90000 });
        // live_connection = await MongoClient.connect(liveDbUrl, { connectTimeoutMS: 90000, socketTimeoutMS: 90000 });

        local_db = MongoSettings.local.projects_database;
        local_collection = MongoSettings.local.projects_collection;

        // live_db = MongoSettings.live.projects_database;
        // live_collection = MongoSettings.live.projects_collection;

        local_dbo = local_connection.db(local_db);
        // live_dbo = live_connection.db(live_db);


        // var query = {
        //     "domain_data.valid": 1, $or: [{ "dates.dp_moved": { $exists: true } }, { "dates.dp_moved": { $gt: new Date("2018-04-30").toISOString() } }]
        // };

        var query = {
            //"_id": ObjectId("58351b263f238de27baf781c")
        }

        cursor = await local_dbo.collection(local_collection).find(query, exclude_objects_updated).limit(10).batchSize(1000);
        var cursor_count = await cursor.count();
        console.log("cursor_count ======", cursor_count);

        if (cursor_count === 0) {
            console.log("stopping process...All listings are processed");
            // process.exit(3);
        }

        while (await cursor.hasNext()) {
            let doc = await cursor.next();
            // console.log(doc)
            try {
                let formatted = formatfields(doc);
                let required = false;
                let data = {}
                if (!formatted.error) {
                    required = await requiredFields(formatted);
                    console.log("------------      -----------");

                    // console.log(formatted);
                    if (required) {
                        // let indexDoc = {
                        //     _index: ElasticSetting.index,
                        //     _type: ElasticSetting.type,
                        //     _id: formatted._id.toString()
                        // }
                        let indexDoc = {
                            index:{
                                _index: ElasticSetting.index,
                                _type: ElasticSetting.type,
                                _id: formatted._id.toString()
                            }
                        }

                        delete formatted._id;
                        batchData.push(indexDoc);
                        batchData.push(formatted);
                    //    console.log((counter % 10 === 0));
                        
                        
                        console.log("counter....",counter);
                    }
                }
                // console.log(doc);
            } catch (Error) {
                // console.log("---------",doc._id.toString())
                errorIDS.push(doc._id);
                // console.log(Error);
            }
            if (counter % 10 === 0) {
                let SucessIDS = Array.from(batchData);
                let FailurIDS = Array.from(errorIDS);
                bulkIndex(SucessIDS,FailurIDS);
                batchData = [];
                errorIDS = [];
                counter = 0;
            }
            counter++;
        }
    })();
}


/*
* 1 Fetch mongo data
* 2 prepare a batch 
* 3 batch data move to FP elastic
* 4 error log

*/

