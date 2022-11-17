#!/usr/bin/python3

"""Analyze user activity"""

import json
import sys

if __name__ == "__main__":
    with open(sys.argv[1], encoding='utf-8') as file_:
        data = json.load(file_)
    users = {x["id"]: x for x in data["users"]}
    for user in users.values():
        user["tweets"] = 0
        user["convos"] = set()
    for tweet in data["tweets"]:
        user = users[tweet["author_id"]]
        user["tweets"] += 1
        user["convos"].add(tweet["conversation_id"])
    print("@%-18s %-28s %8s %8s" % ("username", "name", "tweets", "convos"))
    for user in sorted(users.values(), key=lambda x:-x["tweets"]):
        print("@%-18s %-28s %8d %8d" % (
            user["username"], user["name"], user["tweets"], len(user["convos"])
        ))
