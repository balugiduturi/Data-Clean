const MongoClient = require('mongodb').MongoClient;
var ObjectId = require("mongodb").ObjectId;
var database = "dataclean_application";
let collection = 'users';

var localDbUrl = "mongodb://data_root:xyz1$3^nhy7@104.197.218.69:27017/admin";

module.exports = async function (req, res) {
    var request = req.body;
    try {
            

    } catch (E) {
        console.log(E.message);
    }
}