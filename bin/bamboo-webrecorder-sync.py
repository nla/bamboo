#!/usr/bin/env python3
#
# Required keycloak roles: realm:panadmin, client:realm-management:query-users, client:realm-management:view-users
#
import logging
import os
import redis
import requests

logging.basicConfig(level=logging.DEBUG)

bamboo_url = os.environ["BAMBOO_URL"].rstrip("/")
oidc_url = os.environ["OIDC_URL"].rstrip("/")
oidc_admin_url = oidc_url.replace("/realms/", "/admin/realms/")

def authenticate():
    # /auth/realms/pandas -> /auth/admin/realms/pandas
    oidc_config = requests.get(oidc_url + "/.well-known/openid-configuration").json()
    response = requests.post(oidc_config['token_endpoint'],
                         data={"grant_type": "client_credentials", "client_id": (os.environ["OIDC_CLIENT_ID"]),
                               "client_secret": (os.environ["OIDC_CLIENT_SECRET"])})
    response.raise_for_status()
    return response.json()['access_token']


session = requests.Session()
session.headers['Authorization'] = 'bearer ' + authenticate()


def get_agency_id_for_user(username):
    response = session.get(oidc_admin_url + "/users", params={"search": username})
    response.raise_for_status()
    # keycloak doesn't do an exact match, so we have to filter
    users = [user for user in response.json() if user['username'] == username]
    if not users: return None
    return users[0]['attributes']['agencyId'][0]

def get_or_create_webrecorder_series(agency_id, name):
    response = session.get(bamboo_url + "/api/series/search/findByAgencyIdAndName",
                           params={"agencyId": agency_id, "name": name})
    response.raise_for_status()
    series_list = response.json()['_embedded']['series']
    if series_list:
        return series_list[0]
    else:
        response = session.post(bamboo_url + "/api/series",
                                json={"agencyId": agency_id,
                                      "name": name,
                                      "description": "Created automatically by bamboo-webrecorder-sync.py. Do not rename."})
        response.raise_for_status()
        return response.json()

def main():
    db = redis.Redis(db=2, decode_responses=True)
    for key in db.scan_iter("c:*:info", 1000):
        collid = key.split(":")[1]

        info = db.hgetall("c:" + collid + ":info")

        agency_id = get_agency_id_for_user(info['owner'])
        if agency_id is None:
            logging.warning("User '%s' has no agencyId attribute in keycloak! Ignoring.", info['owner'])
            continue

        series = get_or_create_webrecorder_series(agency_id, "Webrecorder")

        print(info['owner'], agency_id, series['id'])

        crawl_data = {
            "webrecorderCollectionId": collid,
            "created": int(info["created_at"]) * 1000,
            "creator": info["owner"],
            "description": info["desc"],
            "name": info["title"],
            "crawlSeriesId": series["id"],
        }

        response = session.get(bamboo_url + "/api/crawls/search/findByWebrecorderCollectionId", params={"webrecorderCollectionId": collid})
        if response.status_code == 404:
            crawl_response = session.post(bamboo_url + "/api/crawls", json=crawl_data)
            crawl_response.raise_for_status()
            crawl = crawl_response.json()
        else:
            response.raise_for_status()
            crawl = response.json()

            changes = {key:value for (key, value) in crawl_data.items() if crawl[key] != value and key != 'created'}
            if changes:
                session.patch(crawl['_links']['self']['href'], json=changes).raise_for_status()

        for key, url in db.hgetall("c:" + collid + ":warc").items():
            path = url.replace('http://nginx:6090/', '/opt/webrecorder/')
            response = session.get(bamboo_url + "/api/warcs/search/findByFilename", params={"filename": key})
            if response.status_code == 404:
                session.post(bamboo_url + "/crawls/" + str(crawl["id"]) + "/warcs/upload",
                             files={"warcFile": (key, open(path, "rb"))}).raise_for_status()
            else:
                response.raise_for_status()

if __name__ == '__main__':
    try:
        main()
    except requests.HTTPError as e:
        logging.error(e)
        logging.error(e.response.json())

