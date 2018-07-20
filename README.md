# prefixed
In memory KV cache server allowing fast prefix based ops. Gets, upserts, and deletes are O(logN) operation. Prefix-get and prefix-delete are O(M+logN) operation, where M is number of elements returned/deleted.

## License
Apache 2.0.