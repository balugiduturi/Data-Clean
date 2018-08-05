const MongoClient = require('mongodb').MongoClient;
var ObjectId = require("mongodb").ObjectId;
var database = "dataclean_application";
let collection = 'users';

var localDbUrl = "mongodb://data_root:xyz1$3^nhy7@104.197.218.69:27017/admin";

module.exports = async function (req, res) {

    var request = req.body;

    // console.log(request);

    try {
        var checkExistensy = await checkUserExistency(request.email);
        console.log(checkExistensy);
        if (checkExistensy > 0) {
            let message = {
                status: 0,
                message: "user already exists with this email id"
            }
            res.json({ message })
        } else {
            var insert = await MongoUserInsert(request);
            res.json(insert);
        }
    } catch (E) {
        let message = {
            status: 0,
            message: E.message
        }
        res.json(message);
    }




    function checkUserExistency(user_email) {
        return new Promise(async function (resolve, reject) {
            try {
                var local_database = await MongoClient.connect(localDbUrl, { connectTimeoutMS: 90000, socketTimeoutMS: 90000 });
                var dbo = local_database.db(database);
                let res = await dbo.collection(collection).count({ email: user_email });
                resolve(res);
            } catch (E) {
                reject(E.message)
            }
        });
    }

    function MongoUserInsert(request) {
        return new Promise(async function (resolve, reject) {
            try {

                var first_name = request.name.firstName;
                var last_name = request.name.lastName;
                var email = request.email;
                var password = request.password;

                var local_database = await MongoClient.connect(localDbUrl, { connectTimeoutMS: 90000, socketTimeoutMS: 90000 });

                let InsertDocument = {
                    first_name: first_name,
                    last_name: last_name,
                    email: email,
                    password: password
                }
                // db.collection(<collection_name>).insertOne(<document>, <callback_function>)

                var dbo = local_database.db(database);
                var res = await dbo.collection(collection).insertOne(InsertDocument);
                if (res.hasOwnProperty('result')) {
                    // res.result.nModified
                    let result = {
                        type: "Staging Mongo",
                        status: 1,
                        message: "Listing has been Inserted Server"
                    }
                    resolve(result);
                } else {
                    let result = {
                        type: "Staging Mongo",
                        status: 0,
                        message: "Listing has been not Inserted Server"
                    }
                    resolve(result);
                }

            } catch (E) {
                let result = {
                    type: "Listing has been not Inserted Server",
                    status: 0,
                    message: E.message
                }
                reject(result);
            }

        });

    }

}