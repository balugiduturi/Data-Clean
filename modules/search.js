// var elasticsearch = require('elasticsearch');
var request = require('request');
const hmacsha1 = require('./sha1.js');
var aes256 = require('./aes256.js');
const config = require(`${__dataClean_root}/bin/config.js`);

// const ElasticSetting = config.ElasticSetting;


// var elasticClient = new elasticsearch.Client({
//     host: ElasticSetting.Host,
//     requestTimeout: ElasticSetting.requestTimeout
// });

const public_key = config.api_keys.public_key;
const secret_key = config.api_keys.secret_key;
const headers = config.headers;
const FPLiveRoot = config.FPLiveAPi;

module.exports = async function (req, res) {

    var requestBody = req.body;
    
    console.log(requestBody);

    var query = `{"_query":{},"_params":{"listing_source":"dp_public","partner_id":152,"lang_name":"language_en","user_id":14279,"demo":"true","limit":100,"lang":"language_en","page":1,"keyword":"${requestBody.key}","sort":{"by":"business_name","order":"asc"}}}`;
    
    var encryptedData = new aes256().encrypt(query, secret_key)
    var signature = hmacsha1(encryptedData, secret_key);

    var options = {
        method: 'POST',
        url: `${FPLiveRoot}/prospects/search.json`,
        timeout: 10000,
        form: {
            signature: signature,
            key: public_key,
            data: encryptedData
        },
        headers: headers
    };

    // console.log(options);

    request(options, function (error, response, data) {
        if (!error) {
            // console.log(data);
            res.send(data);
        } else {
            console.log(error);
        }

    });


}


