#!/usr/bin/python3

"""Download and manage tweets and threads for one or more users.

Uses a JSON DB to store progress and avoid re-doing work."""

import argparse
import json
import logging
import os
import tempfile
import time

import requests
import tweepy

logging.basicConfig(level=logging.INFO)

TWEET_PARAMS = {
    "user_auth": True,
    "tweet_fields": ','.join([
        "attachments",
        "author_id",
        "conversation_id",
        "created_at",
        "in_reply_to_user_id",
        "possibly_sensitive",
        "referenced_tweets",
    ]),
}
USER_PARAMS = {
    "user_auth": True,
    "user_fields": ','.join([
        "created_at",
        "description",
        "location",
        "pinned_tweet_id",
        "profile_image_url",
        "url",
    ]),
}

def new_client(keys_file):
    """Get a new Client with appropriate keys

    The keys_file should contain a JSON dict with the following:
    consumer_key - API key for the app, from the Dev Portal
    consumer_secret - API key secret, DO NOT SHARE
    bearer_token - Authenticates app, semi-secret
    access_token - OAuth user access token, from the Dev Portal
    access_token_secret - OAuth user access secret, DO NOT SHARE

    Primarily user auth is used, not the bearer_token.

    Because the file contains sensitive keys, it should have appropriate
    permissions. (Not world-readable.)
    """
    with open(keys_file, encoding="utf-8") as key_file:
        keys = json.load(key_file)
    keys.pop("bearer_token", None)
    return tweepy.Client(return_type=dict, wait_on_rate_limit=True, **keys)

def read_db(filename):
    """Read the current JSON db from filename"""
    try:
        with open(filename, encoding="utf-8") as file_:
            return json.load(file_)
    except FileNotFoundError:
        return {}

def write_db(filename, json_db):
    """Write the current JSON db to filename, atomically"""
    with tempfile.NamedTemporaryFile(delete=False,
                                     prefix="download_db",
                                     dir=os.path.dirname(filename),
                                     mode="w") as file_:
        tempname = file_.name
        json.dump(json_db, file_, ensure_ascii=False, check_circular=False,
                  allow_nan=False, indent=2)
    os.replace(tempname, filename)

def get_user_ids(user_db, names, client):
    """Resolve all usernames to user_ids"""
    if not names:
        return []
    result = []
    need = {}
    for name in names:
        if name.isdigit():
            # Already an id
            result.append(name)
            continue
        # Strip @ if needed. Also gives a way to force username lookup for
        # number usernames.
        if name[0] == "@":
            name = name[1:]
        name_cf = name.casefold()
        for db_item in user_db:
            if db_item["username"].casefold() == name_cf:
                result.append(db_item["id"])
                break
        else:
            need[name.casefold()] = len(result)
            result.append(None)
    if need:
        got = client.get_users(usernames=need.keys(), **USER_PARAMS)
        if "errors" in got:
            raise RuntimeError(f"Couldn't look up users: {got['errors']}")
        for blob in got["data"]:
            user_db.append(blob)
            result[need[blob["username"].casefold()]] = blob["id"]
        logging.info("Looked up %d usernames", len(need))
    return result

def get_user_info(data, client):
    """Get user info for unknown user ids"""
    known = {x["id"] for x in data["users"]}
    seen = {x["author_id"] for x in data["tweets"]}
    to_fetch = list(seen.difference(known))
    for chunk in range(0, len(to_fetch), 100):
        batch = to_fetch[chunk:chunk+100]
        got = client.get_users(ids=batch, **USER_PARAMS)
        for blob in got.get("errors", ()):
            user = {
                "id": blob["value"],
                "name": blob["title"],
                "description": blob["detail"],
                "username": None,
            }
            data["users"].append(user)
        data["users"].extend(got["data"])
        logging.info("Looked up %d usernames", len(batch))

def get_known(data):
    """Build the set of known ids"""
    known = {x["id"] for x in data.get("tweets", ())}
    known.update(err["resource_id"] for err in data.get("errors", ()))
    # These aren't valid ids, don't try to look them up
    known.update(["couldnt_scrape", "tweet_was_deleted"])
    return known

def fetch_tweets_by_id(data, tweet_ids, client):
    """Fetch by id"""
    tweet_ids = set(tweet_ids).difference(get_known(data))
    if tweet_ids:
        tweets = data.setdefault("tweets", [])
        got = client.get_tweets(tweet_ids, **TWEET_PARAMS)
        tweets.extend(got.get("data", []))
        data.setdefault("errors", []).extend(got.get("errors", []))
        logging.info("Fetched %d cmdline tweets", len(tweet_ids))

def fetch_user_tweets(tweets, user_ids, client):
    """Update DB for users by fetching from the timeline"""
    for user_id in user_ids:
        since_id = max((int(x["id"]) for x in tweets if x["author_id"] == user_id),
                       default=0)
        old_size = len(tweets)
        tweets.extend(tweepy.Paginator(client.get_users_tweets, user_id,
                                       max_results=100, since_id=since_id,
                                       **TWEET_PARAMS)
                      .flatten())
        logging.info("Fetched %d timeline tweets for user_id %s",
                     len(tweets) - old_size, user_id)

def parse_timeline_item(content, result):
    """helper"""
    item_content = content["itemContent"]
    if item_content["itemType"] != "TimelineTweet":
        return
    inner = item_content["tweet_results"]["result"]
    if inner["__typename"] != "Tweet":
        return
    result.append(inner["rest_id"])

def parse_entry_content(content, result):
    """Parse tweet ids from scraped content, and accumulate in result"""
    match content["entryType"]:
        case "TimelineTimelineItem":
            parse_timeline_item(content, result)
        case "TimelineTimelineModule":
            for item in content["items"]:
                parse_timeline_item(item["item"], result)
        case unknown:
            raise RuntimeError(f"Unknown entryType {unknown} in {content}")

BEARER_AUTH = ("Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xn"
               "Zz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA")

def get_guest_token(client):
    """Get a fresh guest token"""
    response = client.session.post(
        "https://api.twitter.com/1.1/guest/activate.json",
        data=b'',
        headers={
            "accept": "*/*",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "en-US,en;q=0.9",
            "authorization": BEARER_AUTH,
            "referer": "https://twitter.com/"})
    response.raise_for_status()
    token = response.json()["guest_token"]
    logging.info("Guest token is %s", token)
    return token

def get_related_tweets(tweet, guest_token, client):
    """Use scraping to get the ids of all related tweets.

    This includes retweets and replies, and this is the only way to find them.
    """
    query_id = "BoHLKeBvibdYDiJON1oqTg"
    url = f"https://twitter.com/i/api/graphql/{query_id}/TweetDetail"
    params = {
        "variables": {
            "focalTweetId": tweet,
            "with_rux_injections": False,
            "includePromotedContent": False,
            "withCommunity": True,
            "withQuickPromoteEligibilityTweetFields": False,
            "withBirdwatchNotes": False,
            "withSuperFollowsUserFields": False,
            "withDownvotePerspective": False,
            "withReactionsMetadata": False,
            "withReactionsPerspective": False,
            "withSuperFollowsTweetFields": False,
            "withVoice": True,
            "withV2Timeline": True,
        },
        "features": {
            "responsive_web_twitter_blue_verified_badge_is_enabled": True,
            "verified_phone_label_enabled": False,
            "responsive_web_graphql_timeline_navigation_enabled": True,
            "unified_cards_ad_metadata_container_dynamic_card_content_query_enabled": True,
            "tweetypie_unmention_optimization_enabled": True,
            "responsive_web_uc_gql_enabled": True,
            "vibe_api_enabled": True,
            "responsive_web_edit_tweet_api_enabled": True,
            "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
            "standardized_nudges_misinfo": True,
            "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": False,
            "interactive_text_enabled": True,
            "responsive_web_text_conversations_enabled": False,
            "responsive_web_enhance_cards_enabled": True,
        },
    }
    params = {k: json.dumps(v, separators=(',',':'))
              for k, v in params.items()}
    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "en-US,en;q=0.9",
        "authorization": BEARER_AUTH,
        "content-type": "application/json",
        "cookie": "; ".join([
            "guest_id_marketing=v1%3A166856715467666900",
            "guest_id_ads=v1%3A166856715467666900",
            'personalization_id="v1_hV6te5/6PItvp10SCWb8dw=="',
            "guest_id=v1%3A166856715467666900",
            "ct0=6b2cb3a2c07b2ec4a562f2c407f75af9",
            f"gt={guest_token}"]),
        "dnt": "1",
        "referer": f"https://twitter.com/guhdong/status/{tweet}",
        "sec-ch-ua": '"Google Chrome";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "Windows",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/107.0.0.0 Safari/537.36'),
        "x-csrf-token": "6b2cb3a2c07b2ec4a562f2c407f75af9",
        "x-guest-token": guest_token,
        "x-twitter-active-user": "yes",
        "x-twitter-client-language": "en",
    }
    while True:
        req = requests.Request("GET", url, params=params, headers=headers).prepare()
        response = client.session.send(req)
        if response.status_code != 429:
            break
        # Rate limited
        resume = float(response.headers["x-rate-limit-reset"])
        wait_time = resume - time.time()
        logging.warning("Rate limited, sleeping for %.1f seconds", wait_time)
        time.sleep(wait_time)
    response.raise_for_status()
    if response.status_code != 200:
        raise RuntimeError(
            f"Got a non-200 status: {response.status_code} {response.reason}")
    result = []
    response_json = response.json()
    if not response_json["data"]:
        # This happens when we *have* the tweet in our DB, but it no longer
        # exists when we try to scrape it. I.e. it's been deleted in the
        # in-between time.
        # Mark a synthetic scraped ref so we don't try to re-crawl.
        return ["tweet_was_deleted"]
    try:
        for entry in (response_json["data"]
                      ["threaded_conversation_with_injections_v2"]
                      ["instructions"][0]
                      ["entries"]):
            parse_entry_content(entry["content"], result)
    except Exception as ex:
        raise RuntimeError(f"context: {response_json}") from ex
    if not result:
        # This happens when the tweet is age-restricted, or can't be shown to
        # logged-out users for some other reason.
        # Mark a synthetic scraped ref with a *different* value.
        return ["couldnt_scrape"]
    return result

def do_reply_closure(data, expand_ids, write_fn, client):
    """Find and fetch unknown tweets based on reply chains"""
    tweets = data.setdefault("tweets", [])
    errors = data.setdefault("errors", [])
    known = get_known(data)
    guest_token = None

    for tweet in tweets:
        for ref in tweet.get("scraped_refs", ()):
            known.add(ref)
    batch = []
    pos = 0
    while True:
        for tweet in tweets[pos:]:
            scraped = tweet.get("scraped_refs", [])
            if not scraped and tweet["author_id"] in expand_ids:
                logging.info("Scraping related tweets for %s", tweet["id"])
                if not guest_token:
                    guest_token = get_guest_token(client)
                try:
                    scraped = get_related_tweets(tweet["id"], guest_token, client)
                except KeyboardInterrupt:
                    write_fn()
                    raise
                tweet["scraped_refs"] = scraped
            all_refs = {ref["id"] for ref in tweet.get("referenced_tweets", ())}
            all_refs.update(scraped)
            all_refs.difference_update(known)
            batch.extend(all_refs)
            known.update(all_refs)
            write_fn()
        if not batch:
            break
        pos = len(tweets)
        logging.info("Chasing tweets: Processing batch of len %d", len(batch))
        for chunk in range(0, len(batch), 100):
            got = client.get_tweets(batch[chunk:chunk+100], **TWEET_PARAMS)
            tweets.extend(got.get("data", []))
            errors.extend(got.get("errors", []))
            write_fn()
        batch = []

def main():
    """It's main"""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('json_db', help="JSON database name")
    parser.add_argument("-k", '--keys', default="keys.json", help="""
        Filename containing the API and user secret keys, in JSON format""")
    parser.add_argument("-u", '--user', action="append", help="""Scrape tweets
        for <user>. Up to 3200 tweets will be grabbed. Can be specified
        multiple times to scrape multiple users.""")
    parser.add_argument("-e", '--expand_user', action="append", help="""
        Expand replies for <user>. All tweets will be followed up threads
        (towards the base post) no matter what, but for users in this list,
        tweets will be expanded to look for replies/retweets/quote-retweets.
        The resulting tweets will always be fetched, even if they aren't in
        this filtered list. This is a slow operation, since it requires one
        call per tweet. Can be specified multiple times to scrape multiple
        users.""")
    parser.add_argument("-t", "--tweet_id", action="append", help="""
        Fetch a tweet by id. Can be used with -e to fetch an entire thread,
        possibly the entire conversation tree. Can be specified multiple
        times.""")
    args = parser.parse_args()

    client = new_client(args.keys)
    data = read_db(args.json_db)
    ids = get_user_ids(data.setdefault("users", []), args.user, client)
    expand_ids = set(get_user_ids(data["users"], args.expand_user, client))
    write_db(args.json_db, data)

    fetch_tweets_by_id(data, args.tweet_id or (), client)
    write_db(args.json_db, data)

    get_user_info(data, client)
    write_db(args.json_db, data)

    fetch_user_tweets(data.setdefault("tweets", []), ids, client)
    write_db(args.json_db, data)

    def write_gen():
        last = time.time()
        while True:
            now = time.time()
            if now - last > 10:
                logging.info("Writing JSON db...")
                write_db(args.json_db, data)
                last = now
            yield
    gen_obj = write_gen()

    do_reply_closure(data, expand_ids, lambda:next(gen_obj), client)

    get_user_info(data, client)
    write_db(args.json_db, data)

if __name__ == "__main__":
    main()
