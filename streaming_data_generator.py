import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from random import randrange, random, choice
from datetime import datetime
import json
import time

tenant = '1'
num_devices = 100
hub_name = 'tenant' + tenant

async def run():
    x = 1
    while x<10000:
        try:
            msg = {}

            user_id = randrange(1, num_devices)
            xstart =  randrange(1, 6)
            ystart =  randrange(1, 6)
            xend = randrange(1, 6)
            yend = randrange(1, 6)
            #pressure = randrange(0, 500) + random()
            ts = datetime.now().isoformat()
            msg["user_id"] = user_id
            msg["xstart"] = xstart
            msg["ystart"] = ystart
            msg["xend"] = xend
            msg["yend"] = yend

            if xstart == xend and ystart==yend:
                continue
            msg["ts"] = ts
            msg["source"] = "eventhub"

            #print(json.dumps(msg))
            
            producer = EventHubProducerClient.from_connection_string(
                conn_str="<connection-string>",
                eventhub_name="<Eventhub name>",
            )

            async with producer:

                event_data_batch = await producer.create_batch()
                
                event_data_batch.add(EventData(str(json.dumps(msg))))

                await producer.send_batch(event_data_batch)

            time.sleep(0.1)
        except Exception as e:
            print(e) 

        x=x+1

loop = asyncio.get_event_loop()
loop.run_until_complete(run())