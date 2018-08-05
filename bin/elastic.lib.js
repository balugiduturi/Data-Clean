

var elasticsearch = require('elasticsearch');
var elasticClientInstance = null;
const _ip = "107.22.170.45:9200";


// var elasticClient_old = function (_ip) {
//     if (!_ip)
//         throw new Error("Host is not defined");
//     if (elasticClientInstance === null) {
//         console.log("Create new Elastic connection");
//         elasticClientInstance = elasticsearch.Client({
//             hosts: [_ip],
//             //            keepAlive: true,
//             requestTimeout: 50000000
//         });
//     }
//     return elasticClientInstance;
// }


function elasticClient(_ip) {

    return new Promise(function (resolve, reject) {
        if (!_ip)
            reject(new Error("Host is not defined"));
        if (elasticClientInstance === null) {
            console.log("Create new Elastic connection");
            elasticClientInstance = elasticsearch.Client({
                hosts: [_ip],
                //            keepAlive: true,
                requestTimeout: 50000000
            });
            resolve(elasticClientInstance);
        } else {
            resolve(elasticClientInstance)
        }
    })
}

async function bulkMove(bulkDocsArr) {
    console.log("inside bulk .....");
    // console.log(bulkDocsArr);
    try {
        let connection = await elasticClient(_ip);
        let res = await connection.bulk({ body: bulkDocsArr });
        return res;
    } catch (error) {
        console.log(error);
    }

    // let connection = await elasticClient(_ip);
    // connection.bulk({
    //     body: bulkDocsArr
    // }, function (err, res) {
    //     // console.log(res)
    //     console.log('err........',err);
    // })


}

module.exports.bulkMove = bulkMove;

