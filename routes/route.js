const express = require('express');
const router = express.Router();

const mongoStatusUpdate = require('../modules/mongoStatusUpdate.js');
const signup = require('../modules/signup.js');
const loginCheck = require('../modules/loginCheck.js');
const search = require('../modules/search.js');
const editListing = require('../modules/editListing.js');
const test = require('../modules/test.js');

router.post('/mongoStatusChange',  mongoStatusUpdate);
router.post('/signup',  signup);
router.post('/loginCheck',  loginCheck);
router.post('/search',  search);
router.post('/editlisting',  editListing);
// router.post('/test',  test);
console.log(test);


module.exports=router;