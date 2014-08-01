//https://github.com/nathanpeck/s3-upload-stream
var Uploader = require('s3-upload-stream').Uploader;

exports.create = function (filename, callback) {

	var uploadStream = null;

    var UploadStreamObject = new Uploader(
            { 
                "accessKeyId": process.env.AWS_ACCESS_KEY_ID,
                "secretAccessKey": process.env.AWS_SECURE_ACCESS_KEY,
            },
            {
                "Bucket": process.env.AWS_ARRIVALS_ARCHIVE_BUCKET_NAME,
                "Key": filename,
                "ACL": 'public-read',
                "StorageClass": 'REDUCED_REDUNDANCY',
            },
            function (err, newUploadStream) {
                if (err) {
                    callback(err);
                } else {
                	uploadStream = newUploadStream;
                    callback(null, uploadStream);
                }
            }
        );

};
