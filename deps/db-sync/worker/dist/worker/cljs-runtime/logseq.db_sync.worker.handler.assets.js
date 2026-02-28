import "./cljs_env.js";
import "./cljs.core.js";
import "./cljs_bean.core.js";
import "./clojure.string.js";
import "./logseq.db_sync.common.js";
import "./logseq.db_sync.worker.http.js";
goog.provide('logseq.db_sync.worker.handler.assets');
logseq.db_sync.worker.handler.assets.max_asset_size = (((100) * (1024)) * (1024));
logseq.db_sync.worker.handler.assets.parse_asset_path = (function logseq$db_sync$worker$handler$assets$parse_asset_path(path){
var prefix = "/assets/";
if(clojure.string.starts_with_QMARK_(path,prefix)){
var rest_path = cljs.core.subs.cljs$core$IFn$_invoke$arity$2(path,((prefix).length));
var slash_idx = clojure.string.index_of.cljs$core$IFn$_invoke$arity$2(rest_path,"/");
var graph_id = (cljs.core.truth_((function (){var and__5140__auto__ = slash_idx;
if(cljs.core.truth_(and__5140__auto__)){
return (slash_idx > (0));
} else {
return and__5140__auto__;
}
})())?cljs.core.subs.cljs$core$IFn$_invoke$arity$3(rest_path,(0),slash_idx):null);
var file = (cljs.core.truth_((function (){var and__5140__auto__ = slash_idx;
if(cljs.core.truth_(and__5140__auto__)){
return (slash_idx > (0));
} else {
return and__5140__auto__;
}
})())?cljs.core.subs.cljs$core$IFn$_invoke$arity$2(rest_path,(slash_idx + (1))):null);
var dot_idx = (cljs.core.truth_(file)?clojure.string.last_index_of.cljs$core$IFn$_invoke$arity$2(file,"."):null);
var asset_uuid = (cljs.core.truth_((function (){var and__5140__auto__ = dot_idx;
if(cljs.core.truth_(and__5140__auto__)){
return (dot_idx > (0));
} else {
return and__5140__auto__;
}
})())?cljs.core.subs.cljs$core$IFn$_invoke$arity$3(file,(0),dot_idx):null);
var asset_type = (cljs.core.truth_((function (){var and__5140__auto__ = dot_idx;
if(cljs.core.truth_(and__5140__auto__)){
return (dot_idx > (0));
} else {
return and__5140__auto__;
}
})())?cljs.core.subs.cljs$core$IFn$_invoke$arity$2(file,(dot_idx + (1))):null);
if(((cljs.core.seq(graph_id)) && (((cljs.core.seq(asset_uuid)) && (cljs.core.seq(asset_type)))))){
return new cljs.core.PersistentArrayMap(null, 4, [new cljs.core.Keyword(null,"graph-id","graph-id",-205404489),graph_id,new cljs.core.Keyword(null,"asset-uuid","asset-uuid",-1797647247),asset_uuid,new cljs.core.Keyword(null,"asset-type","asset-type",-1419820094),asset_type,new cljs.core.Keyword(null,"key","key",-1516042587),(""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(graph_id)+"/"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(asset_uuid)+"."+cljs.core.str.cljs$core$IFn$_invoke$arity$1(asset_type))], null);
} else {
return null;
}
} else {
return null;
}
});
logseq.db_sync.worker.handler.assets.handle = (function logseq$db_sync$worker$handler$assets$handle(request,env){
var url = (new URL(request.url));
var path = url.pathname;
var method = request.method;
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(method,"OPTIONS")){
return (new Response(null,({"status": (204), "headers": logseq.db_sync.common.cors_headers()})));
} else {
var temp__5802__auto__ = logseq.db_sync.worker.handler.assets.parse_asset_path(path);
if(cljs.core.truth_(temp__5802__auto__)){
var map__40641 = temp__5802__auto__;
var map__40641__$1 = cljs.core.__destructure_map(map__40641);
var key = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40641__$1,new cljs.core.Keyword(null,"key","key",-1516042587));
var asset_type = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40641__$1,new cljs.core.Keyword(null,"asset-type","asset-type",-1419820094));
var bucket = env.LOGSEQ_SYNC_ASSETS;
if(cljs.core.not(bucket)){
return logseq.db_sync.worker.http.error_response("missing assets bucket",(500));
} else {
var G__40643 = method;
switch (G__40643) {
case "GET":
return bucket.get(key).then((function (obj){
if((obj == null)){
return logseq.db_sync.worker.http.error_response("not found",(404));
} else {
var metadata = obj.httpMetadata;
var content_type = (function (){var or__5142__auto__ = metadata.contentType;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return "application/octet-stream";
}
})();
var content_encoding = metadata.contentEncoding;
var cache_control = metadata.cacheControl;
var headers = (function (){var G__40645 = new cljs.core.PersistentArrayMap(null, 2, ["content-type",content_type,"x-asset-type",asset_type], null);
var G__40645__$1 = ((((typeof content_encoding === 'string') && (((cljs.core.not_EQ_.cljs$core$IFn$_invoke$arity$2(content_encoding,"null")) && ((content_encoding.length > (0)))))))?cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__40645,"content-encoding",content_encoding):G__40645);
var G__40645__$2 = ((((typeof cache_control === 'string') && ((cache_control.length > (0)))))?cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__40645__$1,"cache-control",cache_control):G__40645__$1);
return cljs_bean.core.__GT_js.cljs$core$IFn$_invoke$arity$1(G__40645__$2);

})();
return (new Response(obj.body,({"status": (200), "headers": Object.assign(headers,logseq.db_sync.common.cors_headers())})));
}
}));

break;
case "PUT":
return request.arrayBuffer().then((function (buf){
if((buf.byteLength > logseq.db_sync.worker.handler.assets.max_asset_size)){
return logseq.db_sync.worker.http.error_response("asset too large",(413));
} else {
return bucket.put(key,buf,({"httpMetadata": ({"contentType": (function (){var or__5142__auto__ = request.headers.get("content-type");
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return "application/octet-stream";
}
})()}), "customMetadata": ({"checksum": request.headers.get("x-amz-meta-checksum"), "type": asset_type})})).then((function (_){
return logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$3(new cljs.core.Keyword("assets","put","assets/put",-1719046115),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"ok","ok",967785236),true], null),(200));
}));
}
}));

break;
case "DELETE":
return bucket.delete(key).then((function (_){
return logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$3(new cljs.core.Keyword("assets","delete","assets/delete",849630831),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"ok","ok",967785236),true], null),(200));
}));

break;
default:
return logseq.db_sync.worker.http.error_response("method not allowed",(405));

}
}
} else {
return logseq.db_sync.worker.http.error_response("invalid asset path",(400));
}

}
});

//# sourceMappingURL=logseq.db_sync.worker.handler.assets.js.map
