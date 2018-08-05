global.__base = __filename;
global.__dataClean_root = __dirname;

const express = require('express');
const multer = require('multer');
const path = require('path');
const bodyParser = require('body-parser')

const storageFolder = "./uploads/";

const app = express();
const route = require("./routes/route.js")
var storage = multer.diskStorage({
    destination: storageFolder,
    filename: function (req, file, cb) {
        cb(null, file.originalname.replace(path.extname(file.originalname)) + '-' + Date.now() + path.extname(file.originalname));

    }

});
var upload = multer({ storage: storage });

app.use(bodyParser.json())
app.use(function (req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    return next();
});

app.get('/', (req, res) => res.send('index'));
// app.post('/upload',  upload.single('uploadFile'), function (req, res) {
//     console.log("csvMongoExportupload ",req.file.path);
//     res.send("recived upload sdfas..");
// })

app.use("/", route)

const port = 2222;
app.listen(port, () => console.log(`server has been running on port ${port}`));


