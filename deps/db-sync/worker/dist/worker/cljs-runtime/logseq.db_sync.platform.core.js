import "./cljs_env.js";
import "./cljs.core.js";
goog.provide('logseq.db_sync.platform.core');
logseq.db_sync.platform.core.response = (function logseq$db_sync$platform$core$response(body,init){
return (new Response(body,init));
});
logseq.db_sync.platform.core.request = (function logseq$db_sync$platform$core$request(url,init){
return (new Request(url,init));
});
logseq.db_sync.platform.core.request_url = (function logseq$db_sync$platform$core$request_url(request){
return (new URL(request.url));
});

//# sourceMappingURL=logseq.db_sync.platform.core.js.map
