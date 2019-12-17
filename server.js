const startDate = new Date()
const mongodb = require('mongodb')
const async = require('async')
const path = require('path')
const fs = require('fs')

const url = 'mongodb://localhost:27017'

const mergeCustomerData = () => {
  return new Promise(resolve => {
    const path1 = path.join(__dirname, 'source', 'm3-customer-data.json')
    const path2 = path.join(__dirname, 'source', 'm3-customer-address-data.json')
    const dataArray1 = JSON.parse(fs.readFileSync(path1))
    const dataArray2 = JSON.parse(fs.readFileSync(path2))
    const mergedArray = dataArray1.map((elem, i, arr)=>{
      return {...dataArray1[i], ...dataArray2[i]}
    })
    resolve(mergedArray)
  })
}

const generateTasksArray = (dataArray, dbCollection) => {
  return dataArray.map((e, i, a) => {
    return (callback) => {
      dbCollection.insertOne(e)
      .then(res => callback(null, i))
      .catch(err => callback(err))
    }
  })
}

const validateInputArg = () => {
  let input = process.argv[2]
  if (!isNaN(input) & input > 0 ){
    return input
  }
  throw Error("Invalid input CLI argument: please provide a number with value > 0.")
}

mongodb.MongoClient.connect(url)
  .then((client)=>{
    const db = client.db('edx-course-db')
    db.dropCollection('mongodb-migration-node-script').catch(e=>console.log('No need to drop collection.')) // Drop collection for lab purpose.
    const collection = db.collection('mongodb-migration-node-script')
    mergeCustomerData()
      .then((dataArray)=>{
        return generateTasksArray(dataArray, collection)
      })
      .then(tasksArray => async.parallelLimit(tasksArray, validateInputArg()))
      .then(res => console.log(res.length))
      .catch(err => console.error(err))
      .finally(()=>{
        client.close()
        console.log("Disconnected from the database.")
        console.log('Time taken for execution: ' + (new Date()-startDate))
      })
  })
    .catch(err => console.error(err))

// - Think of reading file through data stream
