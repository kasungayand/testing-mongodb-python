import requests
import json
import datetime
import asyncio
import aiohttp
from aiohttp import ClientSession
from aiohttp_retry import RetryClient,ExponentialRetry
from pandas import *
import concurrent.futures
#from multiprocessing import Pool




class MyLogger:
    def debug(self, *args, **kwargs):
        # print('[debug]:', *args, **kwargs)
        pass

data = read_csv("courses.csv")



course_list_new = data['courses'].tolist()


url = "https://ap-southeast-1.aws.data.mongodb-api.com/app/data-gpdby/endpoint/data/v1/action/aggregate"

payload = {
    "collection": "submissions",
    "database": "canvas",
    "dataSource": "octopus-insights",
    "pipeline": [
      {
        "$match": { "assignment.courseId": "345" }
      },
    ]

}
headers = {
  'Content-Type': 'application/json',
  'Access-Control-Request-Headers': '*',
  'api-key': 'gVADxZ5U7bMC7POiaShQxg5YfMS5TJuqCrFP51xNyTJJkLLYkEkshWnIP4gNNXxB',
  'Accept': 'application/json'
}


payload_json = json.dumps(payload)

#async def closeconnection(session):
    #await session.close()


    
async def fn(retry_client,course_id):

    payloadnew = {
        "collection": "submissions",
        "database": "canvas",
        "dataSource": "octopus-insights",
        "filter": {
        "assignment.courseId":str(course_id)
        }

    }


    payload_json_new = json.dumps(payloadnew)
    #session=aiohttp.ClientSession()

    async with retry_client.request(method="POST", url=url, headers=headers,data=payload_json, retry_options=ExponentialRetry(attempts=25, statuses=[403, 500, 502, 503, 504],start_timeout=0.2, factor=4,retry_all_server_errors=True)) as response:


        response.raise_for_status()
        response_json = await response.json()
        #print("Response content:", response_text)  # Print out the response content for debugging
        
        #if response.status == 200:
            #response_json = await response.json()
        return response_json
        #else:
            # Handle error response
            #return None
    #return await fetch(url,session,token,query,variables,client,schoolname)



def process_result(result):
    return result.get('documents', [])


async def by_aiohttp_concurrency(course_list):
    
    Start = datetime.datetime.now()
    #Submissions=list()
    async with ClientSession(headers=headers, raise_for_status=False) as session:
        retry_client = RetryClient(client_session=session, logger=MyLogger())
        tasks = []
        for course_id in course_list:
            tasks.append(asyncio.create_task(fn(retry_client,course_id)))

        original_result = await asyncio.gather(*tasks)
        
        #for res in original_result:
            #print(res)
            #print(len(res['documents']))
            #Submissions=Submissions+res['documents']
    End = datetime.datetime.now()
    print("Time take for data fetching: ", End-Start)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        submissions = list(executor.map(process_result, original_result))
        
    return [doc for sublist in submissions for doc in sublist]
        #for res in original_result:
            #Submissions.extend(res.get('documents', []))
        
    
    #return submissions





Start = datetime.datetime.now()
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
#session=aiohttp.ClientSession()
loop = asyncio.get_event_loop()
data=loop.run_until_complete(by_aiohttp_concurrency(course_list_new))

#num_workers = 4

#with Pool(processes=num_workers) as pool:
    #print(original_result)
            # Use map_async for asynchronous execution (optional)
    #results = pool.map_async(process_data, data).get()


#print(data)
#asyncio.run(closeconnection(session))
End = datetime.datetime.now()
print("Time Taken: ",End-Start)
print("Data length:",len(data))








#Start = datetime.datetime.now()
#response = requests.request("POST", url, headers=headers, data=payload)
#End = datetime.datetime.now()

#print("Time Taken:", End-Start)
#print(response.text)