var CryptoJS = require("crypto-js");

module.exports = class AES256 {

    //Encrypts the data with secret key
    encrypt(data, key) {

        var cryptkey = CryptoJS.enc.Utf8.parse(key);
        //var randomstring = require("randomstring");
        var ivData = '1234567891234567'; //randomstring.generate(16)

        var iv = CryptoJS.enc.Utf8.parse(ivData);
        var encrypted = CryptoJS.AES.encrypt(CryptoJS.enc.Utf8.parse(data), cryptkey,
            {
                keySize: 256 / 8,
                iv: iv,
                mode: CryptoJS.mode.CBC,
                padding: CryptoJS.pad.Pkcs7
            });

        // Base64 decode then convert it to hex
        var encryptedBuf = new Buffer(encrypted.toString(), 'base64')
        var base64DecodedInHex = encryptedBuf.toString('hex');

        //IV data to hex
        var ivBuf = new Buffer(ivData.toString())
        var ivInHex = ivBuf.toString('hex');

        //Append IV and encrypterd data
        var dataInHex = ivInHex + base64DecodedInHex

        // Base64 encode
        var dataBuf = new Buffer(dataInHex, 'hex');
        var base64Encoded = dataBuf.toString('base64');

        return base64Encoded
    }


    //Decrypts the data with secret key
    decrypt(data, key) {
        //Decode base64
        var encryptedBuf = new Buffer(data.toString(), 'base64')
        var base64DecodedInHex = encryptedBuf.toString('hex');

        //Separate 16 bytes(32 here because it is hex) that is the iv data rest is encrypted data
        var iv = base64DecodedInHex.substring(0, 32)
        var encryptedData = base64DecodedInHex.substring(32)

        const cryptkey = CryptoJS.enc.Utf8.parse(key);

        var bufft = new Buffer(encryptedData, 'hex')

        const crypted = CryptoJS.enc.Base64.parse(bufft.toString('base64'));

        var decrypt = CryptoJS.AES.decrypt({ ciphertext: crypted }, cryptkey, {
            keySize: 256 / 8,
            iv: CryptoJS.enc.Hex.parse(iv),
            mode: CryptoJS.mode.CBC,
            padding: CryptoJS.pad.Pkcs7
        });

        return decrypt.toString(CryptoJS.enc.Utf8)

    }
}