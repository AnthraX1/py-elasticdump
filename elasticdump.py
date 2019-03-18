from elasticsearch import Elasticsearch
import argparse
import requests
import os
import time
import simplejson as json
from urlparse import urlparse
from multiprocessing import Process, Queue, Event

#ES default to 24 hours max
TIMEOUT = "1d" 

def ES21scroll(sid):
    return json.loads(requests.get("{}/_search/scroll?scroll={}".format(args.host,TIMEOUT),data=sid,verify=False).text)

def display(msg):
    sys.stderr.write(msg+"\n")

def dump(es,outq,alldone):
    esversion = getVersion(es)
    if not os.path.isfile(url.netloc+'_'+args.index+'.session'):
        if esversion< 2.1:
            r = es.search(args.index, search_type="scan",size=args.size, scroll=TIMEOUT,q=args.q, body=args.query)
            if "_scroll_id" in r:
                r = ES21scroll(r["_scroll_id"])
        else:
            r = es.search(args.index, sort=["_doc"],size=args.size, scroll=TIMEOUT,q=args.q, body=args.query)
        display("Total docs:"+str(r["hits"]["total"]))
    else:
        fs=open(url.netloc+'_'+args.index+'.session','r')
        sid=fs.readlines()[0].strip()
        fs.close()
        if esversion<2.1:
            r= ES21scroll(sid)
        else:
            r = es.scroll(scroll_id=sid, scroll=TIMEOUT)
    while '_scroll_id' in r and len(r['hits']['hits'])>0:
        if sid!=r['_scroll_id']:
            f=open(url.netloc+'_'+args.index+'.session','w')
            f.write(sid+"\n")
            f.close()
            sid=r['_scroll_id']
        for row in r['hits']['hits']:
            outq.put(row)
        try:
            if esversion <2.1:
                r = ES21scroll(sid)
            else:
                r = es.scroll(scroll_id=sid, scroll=TIMEOUT)
        except Exception as e:
            print e
            break
    alldone.set()



def getVersion(es):
    clusterinfo=es.info()
    varr=clusterinfo["version"]["number"].split(".")
    vv='.'.join(varr[0:-1])
    return float(vv)


    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Dump ES index with custom scan_id')
    parser.add_argument('--host', help='ES host, http[s]://host:port',required=True)
    parser.add_argument('--index',help='Index name or index pattern, for example, logstash-* will work as well. Use _all for all indices', required=True)
    parser.add_argument('--size',help='Scroll size',default=500)
    parser.add_argument('--timeout',help='Read timeout. Wait time for long queries.',default=300, type=int)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--query', help='Query string in Elasticsearch DSL format.')
    group.add_argument('--q', help='Query string in Lucene query format.')
    args = parser.parse_args()
    url=urlparse(args.host)
    es=Elasticsearch(url.netloc,request_timeout=5,timeout=args.timeout)
    if url.scheme=='https':
        es=Elasticsearch(url.netloc,use_ssl=True,verify_certs=False,request_timeout=5,timeout=args.timeout)
    outq=Queue(maxsize=50000)
    alldone=Event()
    dumpproc=Process(target=dump,args=(es,outq,alldone))
    dumpproc.daemon=True
    dumpproc.start()
    while not alldone.is_set() or outq.qsize()>0:
        try:
            print json.dumps(outq.get(block=False))
        except:
            time.sleep(0.1)

