# plywood-druid-requester

This is the [Druid](http://druid.io/) requester making abstraction layer for [plywood](https://github.com/implyio/plywood).

Given a Druid query and an optional context it return a Q promise that resolves to the Druid data.

## Installation

To install run:

```
npm install plywood-druid-requester
```

## Usage

In the raw you could use this library like so:

```
druidRequesterGenerator = require('plywood-druid-requester').druidRequester

druidRequester = druidRequesterGenerator({
  host: 'my.druid.host' // This better be a link to a bard
})

druidRequester({
  query: {
    "queryType": "maxTime",
    "dataSource": 'wikipedia_editstream'
  }
})
  .then(function(res) {
    console.log("The maxTime is:", res[0].result)
  })
  .done()
```

Although usually you would just pass `druidRequester` into the Druid driver that is part of plywood.

## Druid API extension

### Removing GETs

In Druid most requests are made using POST.

For inconvenient reasons there are some requests that are made using GET.
Specifically the request to list data sources in the cluster (`GET http://$host/druid/v2/`) and the request
to list the dimensions and metrics of a data source (`GET http://$host/druid/v2/$datasource`). Both of these are
'fixed' here by introducing two new query types:

```json
{
  "queryType": "introspect",
  "dataSource": "some_data_source"
}
```

and

```json
{
  "queryType": "sourceList"
}
```

By introducing these pseudo query types it allows this requester to abstract away the idea of POST and GET and just
pretend (as far as the user is concerned that Druid simply (asynchronously) exchanges one blob of JSON for another.

### Is the data really there?

In Druid certain queries (like the timeseries) will return `[]` both if there is no data for the given filter and if
the data source outright does not exist. This can be very inconvenient if you want to show meaningful errors to users
down the line.

To counteract that this requester will explicitly make an introspection query to find out if the datasource is listed
or not whenever the query returned `[]`.

There is currently no way to disable this check (that makes an extra query). If you have a use case for why this check
should not exist please submit an issue.

## Tests

This is a bit of an incomplete part of this project. Currently the tests run against an actual Druid cluster.
In fact they run against the Metamarkets demo cluster (that is maintained by Druid team members `<3` ).
The data source being queried is the `wikipedia_editstream` data source that is the subject of
[one of the Druid tutorials](http://druid.io/docs/0.6.171/Tutorial:-Loading-Your-Data-Part-1.html).
For now, if you are interested in contributing, you will have to set up such a Druid node yourself.

It is intended to make this part of the process easier somehow (ether by mocking the Druid cluster or otherwise).
