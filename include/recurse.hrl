-record(tried, {servers=[], queries=[], afs=[]}).

-define(DEFAULT_STRATEGY, {recurse_stub,nil}).
-define(ITERATE_STRATEGY, {recurse_root_iterate, [{cache_glue, true}, include_namespace]}).
