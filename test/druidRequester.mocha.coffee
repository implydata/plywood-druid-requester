{ expect } = require("chai")

{ druidRequesterFactory } = require('../build/druidRequester')

info = require('./info')

druidRequester = druidRequesterFactory({
  host: info.druidHost
})

describe "Druid requester", ->
  @timeout(5 * 1000)

  describe "error", ->
    it "throws if there is not host or locator", ->
      expect(->
        druidRequesterFactory({})
      ).to.throw('must have a `host` or a `locator`')


    it "correct error for bad datasource", (testComplete) ->
      druidRequester({
        query: {
          "queryType": "maxTime",
          "dataSource": 'wikipedia_editstream_borat'
        }
      })
      .then(-> throw new Error('DID_NOT_ERROR'))
      .then(null, (err) ->
        expect(err.message).to.equal("No such datasource")
        testComplete()
      )
      .done()


    it "correct error for bad datasource (on introspect)", (testComplete) ->
      druidRequester({
        query: {
          "queryType": "introspect",
          "dataSource": 'wikipedia_editstream_borat'
        }
      })
      .then(-> throw new Error('DID_NOT_ERROR'))
      .then(null, (err) ->
        expect(err.message).to.equal("No such datasource")
        testComplete()
      )
      .done()


  describe "introspection", ->
    it "introspects single data sources", (testComplete) ->
      druidRequester({
        query: {
          "queryType": "sourceList"
        }
      })
      .then((res) ->
        expect(res).be.an('Array')
        expect(res.indexOf('wikipedia_editstream') > -1).to.equal(true)
        testComplete()
      )
      .done()


    it "introspects single dataSource", (testComplete) ->
      druidRequester({
        query: {
          "queryType": "introspect",
          "dataSource": 'wikipedia_editstream'
        }
      })
      .then((res) ->
        expect(res.dimensions).be.an('Array')
        expect(res.metrics).be.an('Array')
        testComplete()
      )
      .done()


    it "introspects multi dataSource", (testComplete) ->
      druidRequester({
        query: {
          "queryType": "introspect",
          "dataSource": {
            "type": "union"
            "dataSources": ['wikipedia_editstream', 'wikipedia_editstream']
          }
        }
      })
      .then((res) ->
        expect(res.dimensions).be.an('Array')
        expect(res.metrics).be.an('Array')
        testComplete()
      )
      .done()


  describe "basic working", ->
    it "gets max time", (testComplete) ->
      druidRequester({
        query: {
          "queryType": "maxTime",
          "dataSource": 'wikipedia_editstream'
        }
      })
      .then((res) ->
        expect(res.length).to.equal(1)
        expect(isNaN(new Date(res[0].result))).to.be.false
        testComplete()
      )
      .done()


    it "works with regular time series", (testComplete) ->
      druidRequester({
        query: {
          "queryType": "timeseries",
          "dataSource": "wikipedia_editstream",
          "granularity": "hour",
          "aggregations": [
            { "type": "count", "name": "Count" }
          ],
          "intervals": [ "2014-01-01T00:00:00.000/2014-01-02T00:00:00.000" ]
        }
      })
      .then((res) ->
        expect(res.length).to.equal(24)
        testComplete()
      )
      .done()


    it "works with regular time series in the far future", (testComplete) ->
      druidRequester({
        query: {
          "queryType": "timeseries",
          "dataSource": "wikipedia_editstream",
          "granularity": "hour",
          "aggregations": [
            { "type": "count", "name": "Count" }
          ],
          "intervals": [ "2045-01-01T00:00:00.000/2045-01-02T00:00:00.000" ]
        }
      })
      .then((res) ->
        expect(res.length).to.equal(0)
        testComplete()
      )
      .done()


    it "works with regular time series in the far future with invalid data source", (testComplete) ->
      druidRequester({
        query: {
          "queryType": "timeseries",
          "dataSource": "wikipedia_editstream_borat",
          "granularity": "hour",
          "aggregations": [
            { "type": "count", "name": "Count" }
          ],
          "intervals": [ "2045-01-01T00:00:00.000/2045-01-02T00:00:00.000" ]
        }
      })
      .then(-> throw new Error('DID_NOT_ERROR'))
      .then(null, (err) ->
        expect(err.message).to.equal("No such datasource")
        testComplete()
      )
      .done()



  describe "timeout", ->
    it "works in simple case", (testComplete) ->
      timeoutDruidRequester = druidRequesterFactory({
        host: '10.69.20.5'
        timeout: 50
      })

      timeoutDruidRequester({
        query: {
          "context": {
            #"timeout": 50
            "useCache": false
          }
          "queryType": "timeseries",
          "dataSource": "mmx_metrics",
          "granularity": "hour",
          "aggregations": [
            { "type": "count", "name": "Count" }
          ],
          "intervals": [ "2014-01-01T00:00:00.000/2014-01-02T00:00:00.000" ]
        }
      })
      .then(-> throw new Error('DID_NOT_ERROR'))
      .then(null, (err) ->
        expect(err.message).to.equal("timeout")
        testComplete()
      )
      .done()
