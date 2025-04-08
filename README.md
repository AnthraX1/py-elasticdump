# py-elasticdump


```
usage: elasticdump.py [-h] [--url URL | --host HOST] [--index INDEX] [--size SIZE] [--timeout TIMEOUT] [--fields FIELDS]
                      [--username USERNAME] [--password PASSWORD] [-C C] [--kibana] [--query QUERY | --q Q]
                      [--scroll_jump_id SCROLL_JUMP_ID] [--slices SLICES | --search_after SEARCH_AFTER]

Dump elasticsearch index/indices without breaking.

optional arguments:
  -h, --help            show this help message and exit
  --url URL             Full ES query url to dump, http[s]://host:port/index/_search?q=...
  --host HOST           ES OR Kibana host, http[s]://host:port
  --index INDEX         Index name or index pattern, for example, logstash-* will work as well. Use _all for all indices
  --size SIZE           Scroll size
  --timeout TIMEOUT     Read timeout. Wait time for long queries.
  --fields FIELDS       Filter output source fields. Separate keys with , (comma).
  --username USERNAME   Username to auth with
  --password PASSWORD   Password to auth with
  -C C                  Enable HTTP compression. Might not work on some older ES versions.
  --kibana              Whether target is Kibana
  --query QUERY         Query string in Elasticsearch DSL format. Include parts inside \{\} only.
  --q Q                 Query string in Lucene query format.
  --scroll_jump_id SCROLL_JUMP_ID
                        When scroll session is expired, use this to jump to last doc _id. (Must delete existing .session file)
  --slices SLICES       Number of slices to use. Default to None (no slice). This uses sliced scroll in ES.
  --search_after SEARCH_AFTER
                        Recover dump using search_after with sort by _doc
Output options:
  --output OUTPUT       Output filename to export. Default stdout
  --output-rewrite      Rewrite output file if exists
  --output-splitsize OUTPUT_SPLITSIZE
                        Split output into multiple files by line count
```
