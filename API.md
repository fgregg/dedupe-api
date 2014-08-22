# Dedupe API endpoints

There are a few processes which can be used via the API. 

### Review Task

**``/session-list/``**

Required params: ``api_key``

Get a list of sessions that your group has access to. Responds with a list of
sessions with their name and session id.

Sample Response:

``` javascript 
{

    "status": "ok",
    "message": "",
    "objects": [
        {
            "name": "csv_example_messy_input.csv",
            "id": "5906d8ff-e417-48c4-b097-c4728aaf67c5"
        }
    ]

}
```

**/get-review-cluster/<session_id>/** 

Required params: ``api_key``

Get a list of records that were clustered together during the training process.

Sample response

``` javascript 
{

    "status": "ok",
    "message": "",
    "objects": [
        {
            "confidence": 0.861913084983826,
            "Zip": "60629",
            "Site name": "Ada S. McKinley\nCommunity Services Albany Location",
            "Phone": "7377810",
            "Address": "5954 S Albany",
            "record_id": 1908,
            "group_id": 472
        },
        {
            "confidence": 0.861913084983826,
            "Zip": "60629",
            "Site name": "ADA S. MCKINLEY COMMUNITY SERVICES ALBANY LOCATION",
            "Phone": "7377810",
            "Address": "5954 S ALBANY",
            "record_id": 1389,
            "group_id": 472
        }
    ],
    "review_remainder": 707,
    "total_clusters": 756

}
```

**/mark-cluster/<session_id>/** 

Required params: 

``api_key``
``group_id`` 
``action`` Either ``yes`` or ``no``

Send a descision about a cluster.

Sample response

``` javascript 

{
    "status": "ok", 
    "action": "yes", 
    "message": "", 
    "group_id": "472", 
    "session_id": "5906d8ff-e417-48c4-b097-c4728aaf67c5"
}

```




