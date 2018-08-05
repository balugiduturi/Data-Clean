const MongoClient = require('mongodb').MongoClient;
var ObjectId = require("mongodb").ObjectId;
var database = "dataclean_application";
let collection = 'users';

var localDbUrl = "mongodb://data_root:xyz1$3^nhy7@104.197.218.69:27017/admin";

module.exports = async function (req, res) {

    var request = req.body;

    // console.log(request);

    try {
        var checkExistensy = await checkUserDetails(request);
        console.log(checkExistensy);
        if (checkExistensy.length > 0) {
            let message = {
                status: 1,
                message: "user sucessfully loged In",
                data: checkExistensy[0]
            }
            res.json({ message })
        } else {
            let message = {
                status: 0,
                message: "Check username and password"
            }
            res.json(message);
        }
    } catch (E) {
        let message = {
            status: 0,
            message: E.message
        }
        res.json(message);
    }

    function checkUserDetails(request) {
        return new Promise(async function (resolve, reject) {
            try {
                var local_database = await MongoClient.connect(localDbUrl, { connectTimeoutMS: 90000, socketTimeoutMS: 90000 });
                var dbo = local_database.db(database);
                let res = await dbo.collection(collection).find({ email: request.email, password:request.password },{password:0}).toArray();
                resolve(res);
            } catch (E) {
                reject(E.message)
            }
        });
    }
}