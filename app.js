const axios = require('axios');

const url = "https://ap-southeast-1.aws.data.mongodb-api.com/app/data-gpdby/endpoint/data/v1/action/aggregate";

const payload = {
    "collection": "submissions",
    "database": "canvas",
    "dataSource": "octopus-insights",
    "pipeline": [
        {
            "$match": { "assignment.courseId": "345" }
        },
        {
            "$project": {"_id":0}
        }
    ]
};

const headers = {
    'Content-Type': 'application/json',
    'Access-Control-Request-Headers': '*',
    'api-key': 'gVADxZ5U7bMC7POiaShQxg5YfMS5TJuqCrFP51xNyTJJkLLYkEkshWnIP4gNNXxB',
    'Accept': 'application/json'
};

const executeSingleRequest = async () => {
    try {
        const response = await axios.post(url, payload, { headers });
        return response.data;
    } catch (error) {
        // Handle errors as needed
        console.error(error.message);
        return null;
    }
};

const getDocumentCount = async (results) => {
        const documents = results.map(result => result ? result.documents.length : 0);
       return documents.reduce((acc, count) => acc + count, 0);
}

const executeParallelRequests = async () => {
    const numberOfRequests = 3000;
    const requests = Array.from({ length: numberOfRequests }, () => executeSingleRequest());

    try {
        const results = await Promise.all(requests);
        console.time('Looptime')
        console.log("Promises length", results.length);
        getDocumentCount(results).then(count => {
            console.timeEnd('Looptime')
            console.log("Total Document Count:", count);
            console.timeEnd('Execution time')
        })
    } catch (error) {
        // Handle errors as needed
        console.error(error.message);
    }
};

// Call the function to execute parallel requests

console.time('Execution time')
executeParallelRequests();