import logging
import requests
import argparse
import time
from pymongo import MongoClient
from sys import stdout
# from random import choice
import numpy as np
# import asyncio
import pdb
from os import environ


# logging.basicConfig(
#     level=logging.DEBUG,
#     # format="[%(levelname)-8s] %(asctime)s.%(msecs)-3d %(filename)-25s %(funcName)-15s %(lineno)-4s %(thread)d %(message)s",
#     format="[%(levelname)-8s] %(asctime)s.%(msecs)-3d %(filename)-25s %(funcName)-15s %(lineno)-4s %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S",
#     stream=stdout,
# )

# log = logging.getLogger("user-simulator")
# log.addHandler(logging.FileHandler("user-simulator.log"))

# LOG_LEVEL = logging.DEBUG
LOG_LEVEL = logging.INFO

log = logging.getLogger("user-simulator")
log.setLevel(LOG_LEVEL)
console = logging.StreamHandler(stdout)
log_format="[%(levelname)-8s] %(asctime)s.%(msecs)-3d %(funcName)-25s %(lineno)-4s %(message)s"
format = logging.Formatter(log_format, datefmt="%Y-%m-%d %H:%M:%S")
console.setFormatter(format)
console.setLevel(LOG_LEVEL)
console.set_name("user-simulator")
log.addHandler(console)

parser = argparse.ArgumentParser(
                    prog = 'simulate-user',
                    description = 'Program to simulate user actions',
                    epilog = 'What do I write here?')

# Generating random data from json-generator.com

class User:

    def __init__(self, *args, **kwargs) -> None:
        assert kwargs.get("rd_data_url", None), "Random data generator url is required"
        assert kwargs.get("mongo_uri", None), "Mongo url is required"
        self._client = MongoClient(mongo_uri)
        self._current_user_data = []
        self._data_pointer = 0 # This just points to the current data being used
        self._collection = self._client["users"]["m_user"]
        self.__truncate = kwargs.get("truncate", False)
        self.rd_api_token = kwargs.get("rd_api_token", None)
        self.rd_data_url = kwargs.get("rd_data_url", None)
        if self.__truncate:
            log.warning("Truncating collection")
            self._collection.delete_many({})

    def get_random_user_data(self):
        log.debug("Getting random user data")
        headers = {"Authorization": f"Bearer {self.rd_api_token}"}
        # try:
        #     async with session.get(self.rd_data_url, headers=headers) as response:
        # self._current_user_data = response.json()
        response = requests.get(self.rd_data_url, headers=headers)
        self._current_user_data = response.json()
        self._data_pointer = 0
        log.info("Got random user data")

    def insert_user_data(self):
        log.debug("Inserting user data")
        self._collection.insert_one(self._current_user_data[self._data_pointer])
        self._data_pointer += 1
        log.info("Inserted user data")

    def update_user_data(self):
        log.debug("Update user data")
        # pdb.set_trace()
        _id = self._collection.aggregate([{"$sample": {"size": 1}}], maxTimeMS=300)
        _id = _id.next()["_id"]
        log.debug(f"Updating user data with id: {_id}")
        log.debug(f"Updating user data with data: {self._current_user_data[self._data_pointer]}")
        response = self._collection.update_one({"_id": _id},{"$set":self._current_user_data[self._data_pointer]})
        log.debug(f"Number of documents matched and updated updated: {response.matched_count}|{response.modified_count}")
        if(response.matched_count == 0):
            log.error("No document matched the query")
            return
        self._data_pointer += 1
        log.info("Updated user data")

    def delete_user_data(self):
        log.debug("Delete user data")
        _id = self._collection.aggregate([{"$sample": {"size": 1}}], maxTimeMS=300)
        _id = _id.next()["_id"]
        log.debug(f"Deleting user data with id: {_id}")
        response = self._collection.delete_one({"_id": _id})
        log.debug(f"Number of documents deleted: {response.deleted_count}")
        if(response.deleted_count == 0):
            log.error("No document matched the query")
            return
        # self._collection.insert_one(self._current_user_data[self._data_pointer])
        self._data_pointer += 1
        log.info("Deleted user data")

    def has_more_data(self):
        log.debug("Checking if there is more data")
        log.debug(f"Ratio: {self._data_pointer/len(self._current_user_data)}")
        if(self._current_user_data and self._data_pointer/len(self._current_user_data) > 0.8):
            log.debug("Getting more data")
            self.get_random_user_data()
        # asyncio.sleep(0.1)
        # await self.get_random_user_data()
        return self._data_pointer < len(self._current_user_data)



def simulate_user_profile(time_interval, truncate=False):
    """
    This can be insert update and delete operations
    """
    current_data = None
    next_data = None
    s_user.get_random_user_data() # Make this an async call
    while True:
        try:
            # while s_user.has_more_data(refresh=True):
            while s_user.has_more_data():
                operation = np.random.choice([s_user.insert_user_data, s_user.update_user_data, s_user.delete_user_data], p=[0.45, 0.45, 0.1])
                operation()
                # s_user.insert_user_data()
                log.debug("Sleeping for {} seconds".format(time_interval))
                time.sleep(time_interval)
        except KeyboardInterrupt as e:
            print("Exiting...")
            break


if __name__ == "__main__":
    parser.add_argument('time_interval', type=float, help="Time interval of events in seconds")
    parser.add_argument('--truncate', action="store_true", help="Truncate the mongodb collection")
    # parser.add_argument('-v', '--verbose', action='store_true')  # on/off flag
    args = parser.parse_args()
    log.info("Starting user simulator")
    log.debug(f"Time interval: {args.time_interval}")
    log.debug(f"Truncate interval: {args.truncate}")
    mongo_uri = environ.get("MONGO_URI", None)
    if mongo_uri:
        log.debug(f"Mongo URI: {mongo_uri}")
    else:
        log.error("MONGO_URI env var not set")
        exit(0)
    json_gen_url = environ.get("JSON_GEN_URL", None)
    if json_gen_url:
        log.debug(f"JSON_GEN_URL: {json_gen_url}")
    else:
        log.error("JSON_GEN_URL env var not set")
        exit(0)
    rd_api_token = environ.get("RD_API_TOKEN", None)
    if rd_api_token:
        log.debug(f"RD_API_TOKEN: {rd_api_token}")
    else:
        log.error("RD_API_TOKEN env var not set")
        exit(0)
    # asyncio.run(simulate_user_profile(args.time_interval, truncate=args.truncate))
    s_user = User(mongo_uri=mongo_uri, rd_data_url=json_gen_url, rd_api_token=rd_api_token, truncate=args.truncate)
    simulate_user_profile(args.time_interval)
