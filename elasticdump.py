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
from multiprocessing import Process, Value, Event, Queue


TIMEOUT = "10m"
COMPRESSION_HEADER = {
    "Accept-Encoding": "deflate, compress, gzip",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0",
}

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ElasticsearchClient:
    def __init__(self, host, username=None, password=None, use_compression=True, use_kibana=False, use_kibana_esapi=False, follow_redirect=True):
        self.host = host
        self.username = username
        self.password = password
        self.use_compression = use_compression
        self.use_kibana = use_kibana
        self.use_kibana_esapi = use_kibana_esapi
        self.follow_redirect = follow_redirect
        self.session = requests.Session()

    def get_version(self):
        url = f"{self.host}/"
        if self.use_kibana and self.use_kibana_esapi:
            url = f"{self.host}/elasticsearch/"
        r = self.session.get(
            url,
            verify=False,
            allow_redirects=self.follow_redirect,
            auth=(self.username, self.password) if self.password else None,
        )
        clusterinfo = r.json()
        return float(".".join(clusterinfo["version"]["number"].split(".")[:-1]))

    def get_kibana_version(self):
        headers = {
            "Content-Type": "application/json",
            "kbn-xsrf": "true",
            "osd-xsrf": "true",
        }
        r = self.session.post(
            f"{self.host}/api/console/proxy?method=GET&path={quote_plus('/')}",
            verify=False,
            headers=headers,
            allow_redirects=self.follow_redirect,
            auth=(self.username, self.password) if self.password else None,
        )
        clusterinfo = r.json()
        return float(".".join(clusterinfo["version"]["number"].split(".")[:-1]))

    def scroll(self, scroll_id):
        headers = {"Content-Type": "application/json"}
        if self.use_compression:
            headers.update(COMPRESSION_HEADER)
        if self.use_kibana and self.use_kibana_esapi:
            headers.update({"kbn-xsrf": "true", "osd-xsrf": "true"})
            url = f"{self.host}/elasticsearch/_search/scroll"
        else:
            url = f"{self.host}/_search/scroll"
        return json.loads(
            self.session.post(
                url,
                data=json.dumps({"scroll": TIMEOUT, "scroll_id": scroll_id}),
                verify=False,
                auth=(self.username, self.password) if self.password else None,
                headers=headers,
            ).text
        )

    def scroll_es21(self, scroll_id):
        headers = {"Content-Type": "application/json"}
        if self.use_compression:
            headers.update(COMPRESSION_HEADER)
        return json.loads(
            self.session.post(
                f"{self.host}/_search/scroll?scroll={TIMEOUT}",
                data=scroll_id,
                verify=False,
                headers=headers,
                auth=(self.username, self.password) if self.password else None,
            ).text
        )

    def scroll_kibana(self, scroll_id):
        headers = {
            "Content-Type": "application/json",
            "kbn-xsrf": "true",
            "osd-xsrf": "true",
        }
        scroll_url = f"{self.host}/api/console/proxy?method=POST&path={quote_plus('/_search/scroll')}"
        if self.use_compression:
            headers.update(COMPRESSION_HEADER)
        return json.loads(
            self.session.post(
                scroll_url,
                data=json.dumps({"scroll": TIMEOUT, "scroll_id": scroll_id}),
                verify=False,
                auth=(self.username, self.password) if self.password else None,
                headers=headers,
            ).text
        )

    def get_index_shard_count(self, index):
        r = self.session.get(
            f"{self.host}/_cat/shards/{index}?format=json",
            verify=False,
            auth=(self.username, self.password) if self.password else None,
        )
        return len(r.json())

    def get_kibana_index_shard_count(self, index):
        headers = {
            "Content-Type": "application/json",
            "kbn-xsrf": "true",
            "kbn-xsrf": "true",
        }
        url = f"{self.host}/api/console/proxy?method=GET&path={quote_plus(f'_cat/shards/{index}?format=json')}"
        r = self.session.post(
            url,
            verify=False,
            auth=(self.username, self.password) if self.password else None,
            headers=headers,
        )
        return len(r.json())

    def delete_search_tasks(self):
        headers = {"Content-Type": "application/json"}
        if self.use_compression:
            headers.update(COMPRESSION_HEADER)
        r = self.session.post(
            f"{self.host}/_tasks/_cancel?actions=*search*",
            verify=False,
            auth=(self.username, self.password) if self.password else None,
            headers=COMPRESSION_HEADER
        )
        if args.debug:
            display(r.text)

class Query:
    def __init__(self, index, size=100, query_string=None, dsl_query=None, fields=None, excludes=None, sort=None, scroll_jump_id=None, slices=None, search_after=None):
        self.index = index
        self.size = size
        self.query_string = query_string
        self.dsl_query = dsl_query
        self.fields = fields
        self.excludes = excludes
        self.sort = sort
        self.scroll_jump_id = scroll_jump_id
        self.slices = slices
        self.search_after = search_after

    def build_initial_scroll_body(self, timeout, slice_id=None, slice_max=None):
        body = {}
        if slice_id is not None and slice_max is not None:
            body["slice"] = {"id": slice_id, "max": slice_max}
        if self.dsl_query:
            body["query"] = self.dsl_query
        return json.dumps(body)

    def build_search_after_body(self, last_sort_value):
        return json.dumps({"search_after": [last_sort_value], "sort": ["_doc"]})

    def get_initial_scroll_params(self, timeout, es_version):
        params = {"size": self.size, "scroll": timeout}
        if self.query_string:
            params["q"] = self.query_string
        if self.fields:
            params["_source"] = self.fields
        if self.excludes:
            params["_source_excludes"] = self.excludes
        if es_version <= 2.1:
            params["search_type"] = "scan"
        else:
            if self.sort:
                params["sort"] = self.sort
            else:
                params["sort"] = ["_doc"]
        return params

    def get_search_params(self):
        params = {"size": self.size}
        if self.query_string:
            params["q"] = self.query_string
        if self.fields:
            params["_source"] = self.fields
        if self.excludes:
            params["_source_excludes"] = self.excludes
        return params

    def get_scroll_path(self, use_kibana):
        if use_kibana:
            return f"/api/console/proxy?method=POST&path={quote_plus('/_search/scroll')}"
        else:
            return "/_search/scroll"

    def get_search_path(self, use_kibana, use_kibana_esapi):
        if use_kibana:
            return f"/api/console/proxy?method=POST&path={quote_plus(f'/{self.index}/_search')}"
        elif use_kibana_esapi:
            return f"/elasticsearch/{self.index}/_search"
        else:
            return f"/{self.index}/_search"

class DataExporter:
    def __init__(self, client, query, output_queue, total_exported, alldone_event, debug_mode=False,
                 output_filename=None, output_rewrite=False, output_splitsize=None):
        self.client = client
        self.query = query
        self.output_queue = output_queue
        self.total_exported = total_exported
        self.alldone_event = alldone_event
        self.debug_mode = debug_mode
        self.session_file_name = self._generate_session_file_name()
        self.output_filename = output_filename
        self.output_rewrite = output_rewrite
        self.output_splitsize = output_splitsize

    def _generate_session_file_name(self):
        session_file_name = f"{urlparse(self.client.host).netloc}_{self.query.index}"
        if len(session_file_name.encode("utf8")) > 255 - 32:
            session_file_name = f"{urlparse(self.client.host).netloc}_{self.query.index[:int(len(self.query.index) / 2)]}_{hashlib.md5(self.query.index.encode()).hexdigest()[:8]}"
        if self.query.query_string:
            session_file_name += f"_{hashlib.md5(self.query.query_string.encode()).hexdigest()[:16]}"
        return session_file_name + ".session"

    def display(self, msg, end='\n'):
        print(msg, file=sys.stderr, end=end)

    """TODO: move query-building logic to Query class. leave only read-data and check-es-response-is-not-error. dump-es-response-to-queue, dump-queue-to-output  """
    def read_with_scroll(self, slice_id=None, slice_max=None):
        if self.client.use_kibana:
            es_version = self.client.get_kibana_version()
            scroll_func = self.client.scroll_kibana
        else:
            es_version = self.client.get_version()
            scroll_func = self.client.scroll

        if es_version < 2.1:
            scroll_func = self.client.scroll_es21

        if os.path.isfile(self.session_file_name):
            self._continue_scroll_session(scroll_func)
        else:
            self._start_new_scroll_session(scroll_func, es_version, slice_id, slice_max)

        self.alldone_event.set()
        self.display("\nAll done!")

    def _start_new_scroll_session(self, scroll_func, es_version, slice_id=None, slice_max=None):
        headers = {"Content-Type": "application/json"}
        if self.client.use_compression:
            headers.update(COMPRESSION_HEADER)
        if self.client.use_kibana:
            headers["kbn-xsrf"] = "true"
            headers["osd-xsrf"] = "true"

        search_path = self.query.get_search_path(self.client.use_kibana, self.client.use_kibana_esapi)
        params = self.query.get_initial_scroll_params(TIMEOUT, es_version)
        query_body = self.query.build_initial_scroll_body(TIMEOUT, slice_id, slice_max)


        if self.debug_mode:
            # todo: rewrite to logging module
            self.display(f'{self.client.host=}')
            self.display(f'{search_path=}')

        query_kwargs = {
            "url": f"{self.client.host}{search_path}",
            "verify": False,
            "headers": headers,
            "params": params,
        }
        if self.client.username and self.client.password:
            query_kwargs["auth"] = (self.client.username, self.client.password)
        if query_body:
            query_kwargs["data"] = query_body

        if args.clear_tasks:
            self.client.delete_search_tasks()

        rt = self.client.session.get(**query_kwargs) if es_version > 2.1 else self.client.session.post(**query_kwargs)

        if self.debug_mode:
            # todo: rewrite to logging module
            self.display('query_body:')
            self.display(query_body)
            self.display('url:'+rt.request.url)
            self.display('headers:' + str(rt.request.headers))
            self.display('request body:')
            self.display(rt.request.body)
            self.display(rt.text)

        if rt.status_code != 200:
            self.display(f"Error: {rt.text}")
            exit(1)

        r = json.loads(rt.text)
        if es_version <= 2.1 and "_scroll_id" in r:
            r = scroll_func(r["_scroll_id"])

        self.display(f"Total docs (or in this slice): {r['hits']['total']}\n")

        if "_scroll_id" in r:
            sid = r["_scroll_id"]
            self._save_scroll_id(sid)
        else:
            return

        while True:
            if "hits" in r and len(r["hits"]["hits"]) == 0:
                break
            if "hits" not in r:
                if r.get("statusCode") == 504:
                    self.display(r)
                    continue
                self.display(f"Missing hits error: {r}")
                break
            if r.get("_scroll_id") is not None and sid != r["_scroll_id"]:
                self._save_scroll_id(sid)
                sid = r["_scroll_id"]
            with self.total_exported.get_lock():
                self.total_exported.value += len(r["hits"]["hits"])
            self.display(f"Dumped {self.total_exported.value} documents", "\r")
            for row in r["hits"]["hits"]:
                self.output_queue.put(row)
            if args.clear_tasks:
                self.client.delete_search_tasks()
            try:
                r = scroll_func(sid)
            except Exception as e:
                self.display(str(e))
                self.display(json.dumps(r).decode("utf-8"))
                continue

    def _continue_scroll_session(self, scroll_func):
        try:
            sid = self._load_scroll_id()
        except FileNotFoundError:
            self.display("Session file not found. Starting new session.")
            self._start_new_scroll_session(scroll_func)
            return

        self.display("Continue session...")
        if args.clear_tasks:
            self.client.delete_search_tasks()
        try:
            r = scroll_func(sid)
        except Exception as e:
            self.display(str(e))
            self.display(json.dumps(r).decode("utf-8"))
            return

        if "_scroll_id" in r:
            sid = r["_scroll_id"]
            self._save_scroll_id(sid)

        while True:
            if "hits" in r and len(r["hits"]["hits"]) == 0:
                break
            if "hits" not in r:
                if r.get("statusCode") == 504:
                    self.display(r)
                    continue
                self.display(f"Missing hits error: {r}")
                break
            if r.get("_scroll_id") is not None and sid != r["_scroll_id"]:
                self._save_scroll_id(sid)
                sid = r["_scroll_id"]
            with self.total_exported.get_lock():
                self.total_exported.value += len(r["hits"]["hits"])
            self.display(f"Dumped {self.total_exported.value} documents", "\r")
            for row in r["hits"]["hits"]:
                self.output_queue.put(row)
            if args.clear_tasks:
                self.client.delete_search_tasks()
            try:
                r = scroll_func(sid)
            except Exception as e:
                self.display(str(e))
                self.display(json.dumps(r).decode("utf-8"))
                continue

    def read_with_search_after(self):
        if self.client.use_kibana:
            es_version = self.client.get_kibana_version()
        else:
            es_version = self.client.get_version()
        if es_version < 2.1:
            self.alldone_event.set()
            exit("search_after is not supported before ES version 2.1")

        headers = {"Content-Type": "application/json"}
        if self.client.use_kibana:
            headers["kbn-xsrf"] = "true"
            headers["osd-xsrf"] = "true"
        kibana_search_path = quote_plus(
            f"/{self.query.index}/_search?size={self.query.size}"
        )
        if self.query.query_string:
            kibana_search_path += quote_plus(f"&q={self.query.query_string}")
        if self.query.fields:
            kibana_search_path += quote_plus(f"&_source={self.query.fields}")
        if self.query.excludes:
            kibana_search_path += quote_plus(
                f"&_source_excludes={self.query.excludes}"
            )
        if self.client.use_compression:
            headers.update(COMPRESSION_HEADER)

        query_body = self.query.build_search_after_body(self.query.search_after)

        if self.client.use_kibana:
            rt = self.client.session.post(
                f"{self.client.host}/api/console/proxy?method=POST&path={kibana_search_path}",
                verify=False,
                headers=headers,
                auth=(self.client.username, self.client.password) if self.client.password else None,
                data=query_body,
            )
        else:
            params = self.query.get_search_params()
            url = f"{self.client.host}{self.query.get_search_path(self.client.use_kibana, self.client.use_kibana_esapi)}"
            if args.clear_tasks:
                self.client.delete_search_tasks()
            rt = self.client.session.get(
                url,
                headers=headers,
                params=params,
                data=query_body,
            )

        if args.debug:
            self.display('url:'+rt.request.url)
            self.display('header:'+rt.request.headers)
            if self.client.use_kibana:
                self.display(kibana_search_path)
            self.display(rt.request.body)
            self.display(rt.text)

        r = json.loads(rt.text)
        if "hits" not in r:
            self.display(f"Missing hits error: {rt.text}")
            exit(1)
        self.display(f"Total docs: {r['hits']['total']}")
        cnt = 0
        while True:
            if "hits" in r and len(r["hits"]["hits"]) == 0:
                break
            if args.debug and "hits" not in r:
                self.display(f"Missing hits error: {r}")
            cnt += len(r["hits"]["hits"])
            for row in r["hits"]["hits"]:
                self.output_queue.put(row)
            if not r["hits"]["hits"]:
                break
            last_sort_id = r["hits"]["hits"][-1]["sort"][0]
            self.display(f"Dumped {cnt} documents\nlast sort id {last_sort_id}", "\r")
            query_body = self.query.build_search_after_body(last_sort_id)
            query_kwargs = {
                "verify": False,
                "headers": headers,
                "data": query_body,
            }
            if self.client.password and self.client.username:
                query_kwargs["auth"] = (self.client.username, self.client.password)
            if self.client.use_kibana:
                query_kwargs["method"] = "POST"
                url = f"{self.client.host}/api/console/proxy?method=POST&path={kibana_search_path}"
            else:
                url = f"{self.client.host}{self.query.get_search_path(self.client.use_kibana, self.client.use_kibana_esapi)}"
                query_kwargs["params"] = self.query.get_search_params()
                query_kwargs["method"] = "GET"
                if args.clear_tasks:
                    self.client.delete_search_tasks()
            if args.debug:
                self.display(query_kwargs)
            rt = self.client.session.request(url, **query_kwargs)
            r = json.loads(rt.text)
        self.alldone_event.set()
        self.display("All done!", "\n")

    def _save_scroll_id(self, scroll_id):
        with open(self.session_file_name, "w") as f:
            f.write(scroll_id + "\n")

    def _load_scroll_id(self):
        with open(self.session_file_name, "r") as f:
            return f.readline().strip()

    def dump_queue(self):
        """TODO: refactor for DRY/KISS cycles: while True: try """
        if self.output_filename:
            if self.output_splitsize and self.output_splitsize > 0:
                file_counter = 0
                line_counter = 0
                current_file = None
                filename_base, filename_ext = os.path.splitext(self.output_filename)
                while True:
                    try:
                        dump_data = json.dumps(self.output_queue.get(block=False)).decode("utf-8") + "\n"
                        if current_file is None or line_counter >= self.output_splitsize:
                            if current_file:
                                current_file.close()
                            filename = f"{filename_base}-{file_counter:02d}{filename_ext}"
                            if not self.output_rewrite and os.path.exists(filename):
                                raise FileExistsError(
                                    f"Can not create {filename}. File is already exists \nUse --output-rewrite parameter for rewrite.")
                            current_file = open(filename, "w")
                            line_counter = 0
                            file_counter += 1
                        current_file.write(dump_data) #  buffering=1 is not necessary bc we cannot have unbuffered text I/O

                        line_counter += 1
                    except Empty:
                        if (
                            all([ad.is_set() for ad in alldone_flags])
                            and self.output_queue.qsize() == 0
                        ):
                            if current_file:
                                current_file.close()
                            break
                        time.sleep(0.1)
            else:
                filename = self.output_filename
                if not self.output_rewrite and os.path.exists(filename):
                    raise FileExistsError(
                        f"Cannot create {filename}. File is already exists \nUse --output-rewrite parameter for rewrite.")
                with open(filename, "w") as outfile:
                    while True:
                        try:
                            dump_data = json.dumps(self.output_queue.get(block=False)).decode("utf-8") + "\n"
                            outfile.write(dump_data)
                        except Empty:
                            if (
                                all([ad.is_set() for ad in alldone_flags])
                                and self.output_queue.qsize() == 0
                            ):
                                break
                            time.sleep(0.1)
        else:
            while True:
                try:
                    dump_data = json.dumps(self.output_queue.get(block=False)).decode("utf-8")
                    print(dump_data)
                except Empty:
                    if (
                        all([ad.is_set() for ad in alldone_flags])
                        and self.output_queue.qsize() == 0
                    ):
                        break
                    time.sleep(0.1)


def display(msg, end='\n'):
    print(msg, file=sys.stderr, end=end)

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
    parser.add_argument("--size", help="Scroll size", default=1000, type=int)
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
        "--clear-tasks",
        "--clear-task",
        help="Clear all search tasks before executing the query.\nThis will disrupt the remote server.\nUse with caution.",
        action="store_true",
        default=False,
    )
    parser.add_argument("-ignore-errors", help="Ignore all errors and keep retrying", action="store_true")
    parser.add_argument("-u", "--username", help="Username to auth with")
    parser.add_argument("-p", "--password", help="Password to auth with")
    parser.add_argument(
        "--follow_redirect", help="Follow http redirects", type=bool, default=True
    )
    parser.add_argument(
        "-C",
        help="Enable HTTP compression. Might not work on some older ES versions.",
        action="store_true",
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

    output_group = parser.add_argument_group("Output options")
    output_group.add_argument("--output", help="Output filename to export. Default stdout")
    output_group.add_argument("--output-rewrite", help="Rewrite output file if exists", action="store_true")
    output_group.add_argument("--output-splitsize", help="Split output into multiple files by line count", type=int)


    parser.add_argument(
        "--debug", help="Print debug messages to STDERR", action="store_true"
    )
    total = Value("i", 0)
    args = parser.parse_args()

    outq = Queue(maxsize=10000)
    alldone_flags = []


    output_filename = args.output
    output_rewrite = args.output_rewrite
    output_splitsize = args.output_splitsize
    if not (args.output):
        if output_rewrite:
            display("Must provide --output filename for use --output-rewrite")
            exit(1)
        if output_splitsize:
            display("Must provide --output filename for use --output_splitsize")
            exit(1)

    if args.url is None and (args.host or args.index) is None:
        display("must provide url or host and index name!")
        exit(1)

    host = args.host
    index = args.index
    query_string = args.q
    dsl_query = None
    if args.query:
        try:
            dsl_query = json.loads(args.query)
        except json.JSONDecodeError as e:
            display(f"Error decoding JSON query: {e}")
            exit(1)
    fields = args.fields
    excludes = args.excludes
    sort = args.sort
    scroll_jump_id = args.scroll_jump_id
    slices = args.slices
    search_after = args.search_after
    size = args.size
    debug = args.debug
    username = args.username
    password = args.password
    use_compression = args.C
    use_kibana = args.kibana
    use_kibana_esapi = args.kibana_esapi
    follow_redirect = args.follow_redirect

    if args.url is not None:
        url = urlparse(args.url)
        host = f"{url.scheme}://{url.netloc}"
        index = url.path.split("/")[1]
        qs = url.query.split("&")
        for qq in qs:
            qa = qq.split("=")
            if qa[0] == "q":
                query_string = qa[1]
            if qa[0] == "_source":
                fields = qa[1]
            if qa[0] == "sort":
                sort = qa[1]
            if qa[0] == "_source_excludes":
                excludes = qa[1]

    client = ElasticsearchClient(host, username, password, use_compression, use_kibana, use_kibana_esapi, follow_redirect)
    query = Query(index, size, query_string, dsl_query, fields, excludes, sort, scroll_jump_id, slices, search_after)
    alldone = Event()
    exporter = DataExporter(client, query, outq, total, alldone, debug, output_filename, output_rewrite, output_splitsize)

    alldone_flags.append(alldone)

    if args.slices:
        if args.kibana:
            version = client.get_kibana_version()
            shards = client.get_kibana_index_shard_count(args.index)
        else:
            version = client.get_version()
            shards = client.get_index_shard_count(args.index)
        if version <= 2.1:
            display("Sliced scroll is not supported in ES 2.1 or below")
            exit(1)
        display(f"Total shards: {shards}")
        if args.slices > shards:
            display(
                "Slice count is greater than total shards. Setting slice to total shards."
            )
            args.slices = shards
        for slice_id in range(args.slices):
            alldone = Event()
            alldone_flags.append(alldone)
            readproc = Process(
                target=exporter.read_with_scroll,
                args=(slice_id, args.slices),
            )
            readproc.daemon = True
            readproc.start()
    elif args.search_after:
        readproc = Process(
            target=exporter.read_with_search_after,
            args=()
        )
        readproc.daemon = True
        readproc.start()
    else:
        readproc = Process(
            target=exporter.read_with_scroll,
            args=()
        )
        readproc.daemon = True
        readproc.start()

    dumpproc = Process(
        target=exporter.dump_queue,
        args=()
    )
    dumpproc.daemon = True
    dumpproc.start()
    dumpproc.join()

