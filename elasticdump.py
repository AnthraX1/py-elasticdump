import argparse
import requests
import os
import sys
import time
import orjson as json
import urllib3
import hashlib
from queue import Empty
from urllib.parse import urlparse, quote_plus
from multiprocessing import Process, Queue, Value, Event

# ES default to 24 hours max
TIMEOUT = "1d"
session = requests.Session()
COMPRESSION_HEADER = {
    "Accept-Encoding": "deflate, compress, gzip",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0",
}

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def ES21scroll(sid, session):
    headers = {"Content-Type": "application/json"}
    if args.C:
        headers.update(COMPRESSION_HEADER)
    return json.loads(
        session.post(
            "{}/_search/scroll?scroll={}".format(args.host, TIMEOUT),
            data=sid,
            verify=False,
            headers=headers,
            auth=(args.username, args.password) if args.password else None,
        ).text
    )


def ESscroll(sid, session):
    headers = {"Content-Type": "application/json"}
    if args.C:
        headers.update(COMPRESSION_HEADER)
    if args.kibana_esapi:
        headers.update({"kbn-xsrf": "true", "osd-xsrf": "true"})
        url = "{}/elasticsearch/_search/scroll".format(args.host)
    else:
        url = "{}/_search/scroll".format(args.host)
    return json.loads(
        session.post(
            url,
            data=json.dumps({"scroll": TIMEOUT, "scroll_id": sid}),
            verify=False,
            auth=(args.username, args.password) if args.password else None,
            headers=headers,
        ).text
    )


def getVersion():
    url = "{}/".format(args.host)
    if args.kibana_esapi:
        url = "{}/elasticsearch/".format(args.host)
    r = requests.get(
        url,
        verify=False,
        allow_redirects=args.follow_redirect,
        auth=(args.username, args.password) if args.password else None,
    )
    clusterinfo = r.json()
    if args.debug:
        display(r.text)
    varr = clusterinfo["version"]["number"].split(".")
    vv = ".".join(varr[0:-1])
    return float(vv)


def getVersionKibana():
    headers = {
        "Content-Type": "application/json",
        "kbn-xsrf": "true",
        "osd-xsrf": "true",
    }
    r = requests.post(
        "{}/api/console/proxy?method=GET&path={}".format(args.host, quote_plus("/")),
        verify=False,
        headers=headers,
        allow_redirects=args.follow_redirect,
        auth=(args.username, args.password) if args.password else None,
    )
    if args.debug:
        display(r.text)
    clusterinfo = r.json()
    varr = clusterinfo["version"]["number"].split(".")
    vv = ".".join(varr[0:-1])
    return float(vv)


def kibanaScroll(sid, session):
    headers = {
        "Content-Type": "application/json",
        "kbn-xsrf": "true",
        "osd-xsrf": "true",
    }
    scroll_url = "{}/api/console/proxy?method=POST&path={}".format(
        args.host, quote_plus("/_search/scroll")
    )
    if args.C:
        headers.update(COMPRESSION_HEADER)
    return json.loads(
        session.post(
            scroll_url,
            data=json.dumps({"scroll": TIMEOUT, "scroll_id": sid}),
            verify=False,
            auth=(args.username, args.password) if args.password else None,
            headers=headers,
        ).text
    )


def display(msg, end="\n"):
    print(msg, file=sys.stderr, end=end)


def get_index_shard_count():
    r = session.get(
        "{}/_cat/shards/{}?format=json".format(args.host, args.index),
        verify=False,
        auth=(args.username, args.password) if args.password else None,
    )
    return len(r.json())


def delete_search_tasks():
    headers = {"Content-Type": "application/json"}
    if args.C:
        headers.update(COMPRESSION_HEADER)
    r = session.post(
        "{}/_tasks/_cancel?actions=*search*".format(args.host),
        verify=False,
        auth=(args.username, args.password) if args.password else None,
        headers=COMPRESSION_HEADER
    )
    if args.debug:
        display(r.text)


def get_kibana_index_shard_count():
    headers = {
        "Content-Type": "application/json",
        "kbn-xsrf": "true",
        "kbn-xsrf": "true",
    }
    url = "{}/api/console/proxy?method=GET&path={}".format(
        args.host, quote_plus("_cat/shards/{}?format=json".format(args.index))
    )
    r = session.post(
        url,
        verify=False,
        auth=(args.username, args.password) if args.password else None,
        headers=headers,
    )
    if args.debug:
        display(r.text)
    return len(r.json())


def search_after_dump(outq, alldone):
    if args.kibana:
        esversion = getVersionKibana()
    else:
        esversion = getVersion()
    if esversion < 2.1:
        alldone.set()
        exit("search_after is not supported before ES version 2.1")
    headers = {"Content-Type": "application/json"}
    if args.kibana:
        headers["kbn-xsrf"] = "true"
        headers["osd-xsrf"] = "true"
        kibana_search_path = quote_plus(
            "/{}/_search?size={}".format(args.index, args.size)
        )
        if args.q:
            kibana_search_path += quote_plus("&q={}".format(args.q))
        if args.fields:
            kibana_search_path += quote_plus("&_source={}".format(args.fields))
        if args.excludes:
            kibana_search_path += quote_plus(
                "&_source_excludes={}".format(args.excludes)
            )
    if args.C:
        headers.update(COMPRESSION_HEADER)

    query_body = json.dumps({"search_after": [args.search_after], "sort": ["_doc"]})
    if args.kibana:
        rt = session.post(
            "{}/api/console/proxy?method=POST&path={}".format(
                args.host, kibana_search_path
            ),
            verify=False,
            headers=headers,
            auth=(args.username, args.password) if args.password else None,
            data=query_body,
        )
    else:
        params = {"size": args.size}
        if args.q:
            params["q"] = args.q
        if args.fields:
            params["_source"] = args.fields
        if args.excludes:
            params["_source_excludes"] = args.excludes
        if args.kibana_esapi:
            url = "{}/elasticsearch/{}/_search".format(args.host, args.index)
        else:
            url = "{}/{}/_search".format(args.host, args.index)
        if args.bruteforce:
            delete_search_tasks()
        rt = session.get(
            url,
            headers=headers,
            params=params,
            data=query_body,
        )
    if args.debug:
        display(rt.request.headers)
        if args.kibana:
            display(kibana_search_path)
        display(rt.request.body)
        display(rt.text)
    r = json.loads(rt.text)
    if "hits" not in r:
        display("Missing hits error: {}".format(rt.text))
        exit(1)
    display("Total docs:" + str(r["hits"]["total"]))
    cnt = 0
    while True:
        if "hits" in r and len(r["hits"]["hits"]) == 0:
            break
        cnt += len(r["hits"]["hits"])
        for row in r["hits"]["hits"]:
            outq.put(row)
        last_sort_id = r["hits"]["hits"][-1]["sort"][0]
        display("Dumped {} documents\nlast sort id {}".format(cnt, last_sort_id), "\r")
        query_body = json.dumps({"search_after": [last_sort_id], "sort": ["_doc"]})
        query_kwargs = {
            "verify": False,
            "headers": headers,
            "data": query_body,
            "params": params,
        }
        if args.password and args.username:
            query_kwargs["auth"] = (args.username, args.password)
        if args.kibana:
            url = "{}/api/console/proxy?method=POST&path={}".format(
                args.host, kibana_search_path
            )
        else:
            url = "{}/{}/_search".format(args.host, args.index)
            if args.bruteforce:
                delete_search_tasks()

        rt = session.get(url, **query_kwargs)
        r = json.loads(rt.text)
    alldone.set()
    display("All done!", "\n")


def dump(outq, alldone, total, slice_id=None, slice_max=None):
    session = requests.Session()
    if args.kibana:
        esversion = getVersionKibana()
    else:
        esversion = getVersion()
    if esversion < 2.1:
        scroll_func = ES21scroll
    elif args.kibana:
        scroll_func = kibanaScroll
    else:
        scroll_func = ESscroll
    session_file_name = "{}_{}".format(urlparse(args.host).netloc, args.index)
    if len(session_file_name.encode("utf8")) > 255 - 32:
        session_file_name = "{}_{}_{}".format(
            urlparse(args.host).netloc,
            args.index[: int(len(args.index) / 2)],
            hashlib.md5(args.index.encode()).hexdigest()[0:8],
        )
    query_body = {}
    if slice_id is not None and slice_max is not None:
        session_file_name += "_{}_{}".format(slice_id, slice_max)
        query_body["slice"] = {"id": slice_id, "max": slice_max}
    if args.q:
        session_file_name += "_{}".format(
            hashlib.md5(args.q.encode()).hexdigest()[0:16]
        )
    if args.query:
        query_body["query"] = json.loads(args.query)
    session_file_name += ".session"
    headers = {"Content-Type": "application/json"}
    if args.C:
        headers.update(COMPRESSION_HEADER)
    if not os.path.isfile(session_file_name):
        if args.kibana:
            headers["kbn-xsrf"] = "true"
            headers["osd-xsrf"] = "true"
            scroll_path = quote_plus(
                "/{}/_search?size={}&scroll={}".format(args.index, args.size, TIMEOUT)
            )
            if args.q:
                scroll_path += quote_plus("&q={}".format(args.q))
            if args.fields:
                scroll_path += quote_plus("&_source={}".format(args.fields))
            if args.excludes:
                scroll_path += quote_plus("&_source_excludes={}".format(args.excludes))
            if args.sort:
                scroll_path += quote_plus("&sort={}".format(args.sort))
            else:
                scroll_path += quote_plus("&sort=_doc")
            query_kwargs = {
                "url": "{}/api/console/proxy?method=POST&path={}".format(
                    args.host, scroll_path
                ),
                "verify": False,
                "headers": headers,
            }
            if args.password and args.username:
                query_kwargs["auth"] = (args.username, args.password)
            if args.query:
                query_kwargs["data"] = json.dumps(query_body)
            rt = session.post(**query_kwargs)
            if args.debug:
                display(rt.request.headers)
                display(rt.request.body)
                display(rt.text)
            if rt.status_code != 200:
                display("Error:" + rt.text)
                exit(1)
        else:
            params = {"size": args.size}
            if args.q:
                params["q"] = args.q
            if args.fields:
                params["_source"] = args.fields
            if args.excludes:
                params["_source_excludes"] = args.excludes
            params["scroll"] = TIMEOUT
            if esversion <= 2.1:
                params["search_type"] = "scan"
            else:
                if args.sort:
                    params["sort"] = args.sort
                else:
                    params["sort"] = ["_doc"]
            if args.kibana_esapi:
                url = "{}/elasticsearch/{}/_search".format(args.host, args.index)
            else:
                url = "{}/{}/_search".format(args.host, args.index)
            query_kwargs = {
                "url": url,
                "verify": False,
                "headers": headers,
                "params": params,
            }
            if args.password and args.username:
                query_kwargs["auth"] = (args.username, args.password)
            if query_body:
                query_kwargs["data"] = json.dumps(query_body)
            if args.bruteforce:
                delete_search_tasks()
            rt = session.get(**query_kwargs)
            if args.debug:
                display(rt.request.headers)
                display(rt.request.body)
                display(rt.text)
        r = json.loads(rt.text)
        if esversion <= 2.1 and "_scroll_id" in r:
            r = scroll_func(r["_scroll_id"], session)
        display("Total docs (or in this slice):" + str(r["hits"]["total"]), "\n")
    else:
        fs = open(session_file_name, "r")
        sid = fs.readlines()[0].strip()
        fs.close()
        if args.bruteforce:
            delete_search_tasks()
        r = scroll_func(sid, session)
        display("Continue session...")

    if "_scroll_id" in r:
        sid = r["_scroll_id"]
        f = open(
            session_file_name,
            "w",
        )
        f.write(sid + "\n")
        f.close()

    while True:
        if "hits" in r and len(r["hits"]["hits"]) == 0:
            break
        if "hits" not in r:
            if r.get("statusCode") == 504:
                display(r)
                continue
            display("Missing hits error: {}".format(r))
            break
        if r.get("_scroll_id") is not None and sid != r["_scroll_id"]:
            f = open(session_file_name, "w")
            f.write(sid + "\n")
            f.close()
            sid = r["_scroll_id"]
        with total.get_lock():
            total.value += len(r["hits"]["hits"])
        display("Dumped {} documents".format(total.value), "\r")
        for row in r["hits"]["hits"]:
            outq.put(row)
        if args.bruteforce:
            delete_search_tasks()
        try:
            r = scroll_func(sid, session)
        except Exception as e:
            display(str(e))
            display(json.dumps(r).decode("utf-8"))
            continue
    alldone.set()
    display("All done!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dump ES index with custom scan_id")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--url",
        help="Full ES query url to dump, http[s]://host:port/index/_search?q=...",
    )
    group.add_argument("--host", help="ES OR Kibana host, http[s]://host:port")
    parser.add_argument(
        "--index",
        help="Index name or index pattern, for example, logstash-* will work as well. Use _all for all indices",
    )
    parser.add_argument("--size", help="Scroll size", default=1000)
    parser.add_argument(
        "--timeout",
        help="Read timeout. Wait time for long queries.",
        default=300,
        type=int,
    )
    parser.add_argument(
        "--fields", help="Filter output source fields. Separate keys with , (comma)."
    )
    parser.add_argument(
        "--excludes",
        help="Exclude output source fields using _source_excludes. Separate fields with , (comma).",
    )
    parser.add_argument(
        "--sort",
        help="sort parameter with _search request. Example: 'sort=field1,field2:asc'",
    )
    parser.add_argument(
        "-b",
        "--bruteforce",
        help="Clear all search tasks before executing the query.\nThis will disrupt the remote server.\nUse with caution.",
        action="store_true",
        default=False,
    )
    parser.add_argument("-u", "--username", help="Username to auth with")
    parser.add_argument("-p", "--password", help="Password to auth with")
    parser.add_argument(
        "--follow_redirect", help="Follow http redirects", type=bool, default=True
    )
    parser.add_argument(
        "-C",
        help="Enable HTTP compression. Might not work on some older ES versions.",
        type=bool,
        default=True,
    )
    parser.add_argument(
        "--kibana", help="Whether target is Kibana", action="store_true", default=False
    )
    parser.add_argument(
        "--kibana_esapi",
        help="Use Elasticsearch api instead of Kibana console. Only available in older versions",
        action="store_true",
        default=False,
    )
    group1 = parser.add_mutually_exclusive_group()
    group1.add_argument(
        "--query",
        help="Query string in Elasticsearch DSL format. Include parts inside \{\} only.",
    )
    group1.add_argument("--q", help="Query string in Lucene query format.")

    parser.add_argument(
        "--scroll_jump_id",
        help="When scroll session is expired, use this to jump to last doc _id. (Must delete existing .session file)",
    )
    group2 = parser.add_mutually_exclusive_group()
    group2.add_argument(
        "--slices",
        help="Number of slices to use. Default to None (no slice). This uses sliced scroll in ES.",
        type=int,
    )
    group2.add_argument(
        "--search_after",
        help="Recover dump using search_after with sort by _doc",
        type=int,
    )
    parser.add_argument(
        "--debug", help="Print debug messages to STDERR", action="store_true"
    )
    total = Value("i", 0)
    args = parser.parse_args()

    outq = Queue(maxsize=100000)

    alldone_flags = []
    if args.url is None and (args.host or args.index) is None:
        display("must provide url or host and index name!")
        exit(1)

    if args.url is not None:
        url = urlparse(args.url)
        args.host = "{}://{}".format(url.scheme, url.netloc)
        args.index = url.path.split("/")[1]
        if url.query != "":
            qs = url.query.split("&")
            for qq in qs:
                qa = qq.split("=")
                if qa[0] == "q":
                    args.q = qa[1]
                if qa[0] == "_source":
                    args.fields = qa[1]
                if qa[0] == "sort":
                    args.sort = qa[1]
                if qa[0] == "_source_excludes":
                    args.excludes = qa[1]
    else:
        url = urlparse(args.host)

    if args.slices:
        if args.kibana:
            version = getVersionKibana()
            shards = get_kibana_index_shard_count()
        else:
            version = getVersion()
            shards = get_index_shard_count()
        if version <= 2.1:
            display("Sliced scroll is not supported in ES 2.1 or below")
            exit(1)
        display("Total shards: {}".format(shards))
        if args.slices > shards:
            display(
                "Slice count is greater than total shards. Setting slice to total shards."
            )
            args.slices = shards
        for slice_id in range(args.slices):
            alldone = Event()
            alldone_flags.append(alldone)
            dumpproc = Process(
                target=dump,
                args=(outq, alldone, total, slice_id, args.slices),
            )
            dumpproc.daemon = True
            dumpproc.start()
    elif args.search_after:
        alldone_flags.append(Event())
        dumpproc = Process(target=search_after_dump, args=(outq, alldone_flags[0]))
        dumpproc.daemon = True
        dumpproc.start()
    else:
        alldone_flags.append(Event())
        dumpproc = Process(
            target=dump, args=(outq, alldone_flags[0], total, None, None)
        )
        dumpproc.daemon = True
        dumpproc.start()

    while True:
        try:
            print(json.dumps(outq.get(block=False)).decode("utf-8"))
        except Empty:
            if (
                all([alldone.is_set() for alldone in alldone_flags])
                and outq.qsize() == 0
            ):
                break
            time.sleep(0.1)
