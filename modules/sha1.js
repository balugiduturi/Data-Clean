var CryptoJS = require("crypto-js");

module.exports = function (data, key) {
    var hash = CryptoJS.HmacSHA1(data, key).toString()
    var dataBuf = new Buffer(hash);
    var base64Encoded = dataBuf.toString('base64');
    return base64Encoded.toString()
}
