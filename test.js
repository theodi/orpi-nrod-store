var es = require('event-stream'),
	s3 = require('./s3-writer');

s3.create("foobar.csv", function (err, outStream) {
	es.readArray(["1","2","3"]).pipe(outStream);
	es.readArray(["4","5","6"]).pipe(outStream);
});
