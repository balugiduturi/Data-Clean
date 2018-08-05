
var ElasticSetting = {
    Host: "107.21.99.225:9200",
    requestTimeout: 50000000,
    index: "dp_projects,dp_business",
    type: "projects,business"
}

const api_keys = {
    public_key : "d705cc6602fe1d255e24271b3e409cf2a9a3ec15",
    secret_key : "f2a02af9e453e1521009fdc565cc198a"
}

var headers = {
    'Content-Type': "application/json",
    'Access-Control-Allow-Methods': 'POST',
    "Access-Control-Allow-Origin": "*",
    'Access-Control-Max-Age': '1000'
};

const apiVersion = 'v4';
const FPLiveAPi = `https://api.discover-prospects.com/${apiVersion}`;



module.exports.ElasticSetting = ElasticSetting;
module.exports.api_keys = api_keys;
module.exports.headers = headers;
module.exports.FPLiveAPi = FPLiveAPi;


