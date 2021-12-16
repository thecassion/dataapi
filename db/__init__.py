import motor.motor_asyncio
import os
client = motor.motor_asyncio.AsyncIOMotorClient(os.environ.get('MONGODB_URI'))
db = client["unops"]