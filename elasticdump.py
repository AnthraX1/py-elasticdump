import argparse
import requests
import os
import sys
import time
import simplejson as json
import urllib3
import hashlib
from urllib.parse import urlparse, quote_plus
from multiprocessing import Process, Queue, Event
from elasticsearch import Elasticsearch

# ES default to 24 hours max
TIMEOUT = "1d"
session = requests.Session()
COMPRESSION_HEADER = {"Accept-Encoding": "deflate, compress, gzip"}

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def ES21scroll(sid):
    headers = {"Content-Type": "application/json"}
    if args.C:
        headers.update(COMPRESSION_HEADER)
    return json.loads(
        session.post(
            "{}/_search/scroll?scroll={}".format(args.host, TIMEOUT),
            data=sid,
            verify=False,
            headers=headers,
            auth=(args.username, args.password),
        ).text
    )


def ESscroll(sid):
    headers = {"Content-Type": "application/json"}
    if args.C:
        headers.update(COMPRESSION_HEADER)
    return json.loads(
        session.post(
            "{}/_search/scroll".format(args.host),
            data=json.dumps({"scroll": TIMEOUT, "scroll_id": sid}),
            verify=False,
            auth=(args.username, args.password),
            headers=headers,
        ).text
    )


def kibanaScroll(sid):
    headers = {"Content-Type": "application/json", "kbn-xsrf": "true"}
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
            auth=(args.username, args.password),
            headers=headers,
        ).text
    )


def display(msg, end="\n"):
    print(msg, file=sys.stderr, end=end)


def search_after_dump(es, outq, alldone):
    esversion = getVersion(es)
    if esversion < 2.1 and args.search_after != None:
        alldone.set()
        exit("search_after is not supported before ES version 2.1")
    query_body = json.dumps({"search_after": [args.search_after]})
    r = es.search(
        args.index,
        sort=["_doc"],
        size=args.size,
        q=args.q,
        body=query_body,
        _source=args.fields,
    )
    display("Total docs:" + str(r["hits"]["total"]))
    cnt = 0
    while True:
        if "hits" in r and len(r["hits"]["hits"]) == 0:
            break
        cnt += len(r["hits"]["hits"])
        for row in r["hits"]["hits"]:
            outq.put(row)
        display("Dumped {} documents".format(cnt), "\r")
        last_sort_id = r["hits"]["hits"][-1]["sort"][0]
        display("\nlast sort id {}".format(last_sort_id), "\r")
        query_body = json.dumps({"search_after": [last_sort_id]})
        r = es.search(
            args.index,
            sort=["_doc"],
            size=args.size,
            q=args.q,
            body=query_body,
            _source=args.fields,
        )
    alldone.set()
    display("All done!")


def dump(es, outq, alldone):
    esversion = getVersion(es)
    session_file_name = "{}_{}_{}.sesssion".format(url.netloc, args.index, hashlib.md5(args.q.encode()).hexdigest()[0:16])
    if not os.path.isfile(session_file_name):
        if esversion < 2.1:
            r = es.search(
                args.index,
                search_type="scan",
                size=args.size,
                scroll=TIMEOUT,
                q=args.q,
                body=args.query,
                _source=args.fields,
            )
            if "_scroll_id" in r:
                r = ES21scroll(r["_scroll_id"])
        else:
            r = es.search(
                args.index,
                sort=["_doc"],
                size=args.size,
                scroll=TIMEOUT,
                q=args.q,
                body=args.query,
                _source=args.fields,
            )
        display("Total docs:" + str(r["hits"]["total"]))
    else:
        fs = open(session_file_name, "r")
        sid = fs.readlines()[0].strip()
        fs.close()
        if esversion < 2.1:
            r = ES21scroll(sid)
        else:
            r = ESscroll(sid)
        display("Continue session...")

    if "_scroll_id" in r:
        sid = r["_scroll_id"]
        f = open(session_file_name, "w",)
        f.write(sid + "\n")
        f.close()
    cnt = 0
    while True:
        if "hits" in r and len(r["hits"]["hits"]) == 0:
            break
        if sid != r["_scroll_id"]:
            f = open(session_file_name, "w")
            f.write(sid + "\n")
            f.close()
            sid = r["_scroll_id"]
        cnt += len(r["hits"]["hits"])
        display("Dumped {} documents".format(cnt), "\r")
        for row in r["hits"]["hits"]:
            outq.put(row)
        if esversion < 2.1:
            try:
                r = ES21scroll(sid)
            except Exception as e:
                display(str(e))
                display(json.dumps(r))
                continue
        else:
            try:
                r = ESscroll(sid)
            except Exception as e:
                display(str(e))
                display(json.dumps(r))
                continue
    alldone.set()
    display("All done!")


def getVersion(es):
    clusterinfo = es.info()
    varr = clusterinfo["version"]["number"].split(".")
    vv = ".".join(varr[0:-1])
    return float(vv)


def dumpkibana(outq, alldone):
    url = urlparse(args.host)
    session_file_name = "{}_{}_{}.sesssion".format(url.netloc, args.index, hashlib.md5(args.q.encode()).hexdigest()[0:16])
    if not os.path.isfile(session_file_name):
        headers = {"Content-Type": "application/json", "kbn-xsrf": "true"}
        if args.C:
            headers.update(COMPRESSION_HEADER)
        scroll_path = quote_plus(
            "/{}/_search?size={}&sort=_doc&scroll={}".format(
                args.index, args.size, TIMEOUT
            )
        )
        if args.q:
            scroll_path += quote_plus("&q={}".format(args.q))
        if args.fields:
            scroll_path += quote_plus("&_source={}".format(args.fields))
        r = session.post(
            "{}/api/console/proxy?method=GET&path={}".format(args.host, scroll_path),
            verify=False,
            auth=(args.username, args.password),
            headers=headers,
        )
        if r.status_code != 200:
            display("Error:" + r.text)
            exit(1)
        r = json.loads(r.text)
        if "_scroll_id" in r:
            sid = r["_scroll_id"]
            f = open(session_file_name, "w")
            f.write(sid + "\n")
            f.close()
    else:
        fs = open(session_file_name, "r")
        sid = fs.readlines()[0].strip()
        fs.close()
        display("Continue session...")
        r = kibanaScroll(sid)
    display("Total docs:" + str(r["hits"]["total"]))
    cnt = 0
    while True:
        if "hits" in r and len(r["hits"]["hits"]) == 0:
            break
        if sid != r["_scroll_id"]:
            f = open(session_file_name, "w")
            f.write(sid + "\n")
            f.close()
            sid = r["_scroll_id"]
        cnt += len(r["hits"]["hits"])
        for row in r["hits"]["hits"]:
            outq.put(row)
        display("Dumped {} documents".format(cnt), "\r")
        r = kibanaScroll(sid)
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
    parser.add_argument("--username", help="Username to auth with")
    parser.add_argument("--password", help="Password to auth with")
    parser.add_argument(
        "--search_after",
        help="Recover dump using search_after with sort by _doc",
        type=int,
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
    group1 = parser.add_mutually_exclusive_group()
    group1.add_argument("--query", help="Query string in Elasticsearch DSL format.")
    group1.add_argument("--q", help="Query string in Lucene query format.")

    parser.add_argument(
        "--scroll_jump_id",
        help="When scroll session is expired, use this to jump to last doc _id. (Must delete existing .session file)",
    )

    args = parser.parse_args()

    outq = Queue(maxsize=50000)
    alldone = Event()

    if args.url is None and (args.host or args.index) is None:
        display("must provide url or host and index name!")
        exit(1)

    if args.kibana:
        dumpproc = Process(target=dumpkibana, args=(outq, alldone))
        dumpproc.daemon = True
        dumpproc.start()
        while not alldone.is_set() or outq.qsize() > 0:
            try:
                print(json.dumps(outq.get(block=False)))
            except:
                time.sleep(0.1)
        exit(0)

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
    else:
        url = urlparse(args.host)
    if args.username and args.password:
        es = Elasticsearch(
            url.netloc,
            request_timeout=5,
            timeout=args.timeout,
            http_auth=(args.username, args.password),
        )
        if url.scheme == "https":
            es = Elasticsearch(
                url.netloc,
                use_ssl=True,
                verify_certs=False,
                request_timeout=5,
                timeout=args.timeout,
                http_auth=(args.username, args.password),
            )
    else:
        es = Elasticsearch(url.netloc, request_timeout=5, timeout=args.timeout)
        if url.scheme == "https":
            es = Elasticsearch(
                url.netloc,
                use_ssl=True,
                verify_certs=False,
                request_timeout=5,
                timeout=args.timeout,
            )

    if args.search_after is None:
        dumpproc = Process(target=dump, args=(es, outq, alldone))
    else:
        dumpproc = Process(target=search_after_dump, args=(es, outq, alldone))
    dumpproc.daemon = True
    dumpproc.start()
    while not alldone.is_set() or outq.qsize() > 0:
        try:
            print(json.dumps(outq.get(block=False)))
        except:
            time.sleep(0.1)
