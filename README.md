Open Rail Performance Index
===========================

##orpi-nrod-store

The objective of the orpi-nrod-store is to save for reference all data from the [Network Rail Open Data (NROD) feeds](http://www.networkrail.co.uk/data-feeds/) that is relevant to calculate a rail services performance index (to be defined).

At the moment of writing we are relying exclusively on the "train movement" messages that are part of the "TRAIN_MVT_ALL_TOC" feed. The original data feed is a stream of individual JSON files that we store as flattened CSV files for convenience, one file per hour. The columns are explained on the Network Rail Open Data wiki [here](http://nrodwiki.rockshore.net/index.php/Train_Movement). By flattening the JSON structure, what in the original feed is, for example, the *body.event_type* field becomes the "body.event_type" column.

###Licence

The trains schedule and arrival data are sourced from the [Network Rail "Data Feeds" service](https://datafeeds.networkrail.co.uk). As a result, the data produced by *orpi-nrod-store* contains information of Network Rail Infrastructure Limited licensed under the following licence [http://www.networkrail.co.uk/data-feeds/terms-and-conditions/](http://www.networkrail.co.uk/data-feeds/terms-and-conditions/)_.

![Creative Commons License](http://i.creativecommons.org/l/by/4.0/88x31.png "Creative Commons License") This work, but for what is specified differently above, is licensed under a [Creative Commons Attribution 4.0 International](https://creativecommons.org/licenses/by/4.0/). 