const ApiBuilder = require('claudia-api-builder')
const AWS = require('aws-sdk')
const ZwiftAccount = require('zwift-mobile-api')
const mysql = require('promise-mysql')
const kms = new AWS.KMS()
const account = decryptEnv(process.env.ZWIFTPW).then(pw => new ZwiftAccount(process.env.ZWIFTUSER, pw))
const dbConnection = decryptEnv(process.env.DBPW).then(pw => mysql.createConnection({
  host: process.env.DBHOST,
  user: process.env.DBUSER,
  database: process.env.DBNAME,
  password: pw,
  charset: 'utf8mb4',
  timezone: 'Z'
}))
const api = new ApiBuilder()

module.exports = api

function getEventSubgroupResults(eventSubgroupId) {
  return account.then((account) => {
    return account.getWorld().segmentResults(eventSubgroupId).then((results) => {
      results.sort((a, b) => {
        return a.elapsedMs - b.elapsedMs
      })
      return results
    })
  })
}

function getFurthestProgressList(eventSubgroupId) {
  return dbConnection.then((conn) => {
    return conn.query('SELECT riderid, lineid, meters, msec FROM live_results WHERE grp = ?', [eventSubgroupId])
  }).then((rows) => {
    let results = {}
    for (let row of rows) {
      if (!(row.riderid in results) || row.meters > results[row.riderid].meters) {
        results[row.riderid] = row
      }
    }
    let resultList = Object.keys(results).map(key => results[key])
    resultList.sort((a, b) => {
      if (a.lineid != b.lineid) {
        return b.meters - a.meters
      } else {
        if (Math.abs(a.meters - b.meters) < 1000) {
          return a.msec - b.msec
        } else {
          return b.meters - a.meters
        }
      }
    })
    //console.log(`getFurthestProgressList(${eventSubgroupId}) = ${resultList}`)
    return resultList
  })
}

function determineDNFs (eventSubgroupId, results, progress) {
  let dnfs = []
  let finishers = []
  for (let result of results) {
    finishers[result.riderId] = result
  }
  for (let p of progress) {
    if (!(p.riderid in finishers)) {
      dnfs.push({
        id: 0,
        dnf: true,
        riderId: p.riderid,
        eventSubgroupId: eventSubgroupId,
        lastLineCrossed: p.lineid,
        lastCrossingTime: p.msec,
        distanceRidden: p.meters,
      })
    }
  }
  return dnfs
}
function getDNFRiders(eventSubgroupId) {
}

api.get('/segment-results', (request) => {
  return getEventSubgroupResults(request.queryString.eventSubgroupId)
}, {apiKeyRequired: true})

api.get('/dnfs', (request) => {
  const eventSubgroupId = request.queryString.eventSubgroupId
  return Promise.all([getFurthestProgressList(eventSubgroupId), getEventSubgroupResults(eventSubgroupId)])
    .then(values => {
      let [progress, results] = values
      return determineDNFs(eventSubgroupId, results, progress)
    })
}, {apiKeyRequired: true})

api.get('/fullResults', (request) => {
  const eventSubgroupId = request.queryString.eventSubgroupId
  return Promise.all([getFurthestProgressList(eventSubgroupId), getEventSubgroupResults(eventSubgroupId)])
    .then(values => {
      let [progress, results] = values
      return results.concat(determineDNFs(eventSubgroupId, results, progress))
    })
}, {apiKeyRequired: true})

function decryptEnv(encrypted) {
  if (encrypted) {
    return kms.decrypt({CiphertextBlob: new Buffer(encrypted, 'base64')}).promise().then((data) => {
      return data.Plaintext.toString('ascii')
    })
  } else {
    return Promise.resolve(null)
  }
}

/*
api.get('zwiftId', (request) => {
  const username = request.queryString.username
  const pw = request.queryString.pw
  const account = new ZwiftAccount(username, pw)
  return account.getProfile().profile().then((profile) => {
    return profile.id
  })
})
*/