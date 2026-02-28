import "./cljs_env.js";
import "./cljs.core.js";
import "./clojure.string.js";
import "./logseq.db_sync.common.js";
import "./logseq.db_sync.platform.core.js";
import "./logseq.db_sync.worker.handler.assets.js";
import "./logseq.db_sync.worker.handler.index.js";
import "./logseq.db_sync.worker.http.js";
import "./promesa.core.js";
goog.provide('logseq.db_sync.worker.dispatch');
logseq.db_sync.worker.dispatch.handle_worker_fetch = (function logseq$db_sync$worker$dispatch$handle_worker_fetch(request,env){
var url = logseq.db_sync.platform.core.request_url(request);
var path = url.pathname;
var method = request.method;
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(path,"/health")){
return logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("worker","health","worker/health",226298609),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"ok","ok",967785236),true], null));
} else {
if(((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(path,"/graphs")) || (clojure.string.starts_with_QMARK_(path,"/graphs/")))){
return logseq.db_sync.worker.handler.index.handle_fetch(({"env": env, "d1": (env["DB"])}),request);
} else {
if(clojure.string.starts_with_QMARK_(path,"/e2ee")){
return logseq.db_sync.worker.handler.index.handle_fetch(({"env": env, "d1": (env["DB"])}),request);
} else {
if(clojure.string.starts_with_QMARK_(path,"/assets/")){
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(method,"OPTIONS")){
return logseq.db_sync.worker.handler.assets.handle(request,env);
} else {
var temp__5802__auto__ = logseq.db_sync.worker.handler.assets.parse_asset_path(path);
if(cljs.core.truth_(temp__5802__auto__)){
var map__41013 = temp__5802__auto__;
var map__41013__$1 = cljs.core.__destructure_map(map__41013);
var graph_id = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__41013__$1,new cljs.core.Keyword(null,"graph-id","graph-id",-205404489));
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.worker.handler.index.graph_access_response(request,env,graph_id)),(function (access_resp){
return promesa.protocols._promise((cljs.core.truth_(access_resp.ok)?logseq.db_sync.worker.handler.assets.handle(request,env):access_resp));
}));
}));
} else {
return logseq.db_sync.worker.http.bad_request("invalid asset path");
}
}
} else {
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(method,"OPTIONS")){
return logseq.db_sync.common.options_response();
} else {
if(clojure.string.starts_with_QMARK_(path,"/sync/")){
var prefix = (("/sync/").length);
var rest_path = cljs.core.subs.cljs$core$IFn$_invoke$arity$2(path,prefix);
var rest_path__$1 = ((clojure.string.starts_with_QMARK_(rest_path,"/"))?cljs.core.subs.cljs$core$IFn$_invoke$arity$2(rest_path,(1)):rest_path);
var slash_idx = (function (){var or__5142__auto__ = clojure.string.index_of.cljs$core$IFn$_invoke$arity$2(rest_path__$1,"/");
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return (-1);
}
})();
var graph_id = (((slash_idx < (0)))?rest_path__$1:cljs.core.subs.cljs$core$IFn$_invoke$arity$3(rest_path__$1,(0),slash_idx));
var tail = (((slash_idx < (0)))?"/":cljs.core.subs.cljs$core$IFn$_invoke$arity$2(rest_path__$1,slash_idx));
var new_url = (new URL((""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(url.origin)+cljs.core.str.cljs$core$IFn$_invoke$arity$1(tail)+cljs.core.str.cljs$core$IFn$_invoke$arity$1(url.search))));
if(cljs.core.seq(graph_id)){
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(method,"OPTIONS")){
return logseq.db_sync.common.options_response();
} else {
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.worker.handler.index.graph_access_response(request,env,graph_id)),(function (access_resp){
return promesa.protocols._promise((cljs.core.truth_(access_resp.ok)?(function (){var namespace = env.LOGSEQ_SYNC_DO;
var do_id = namespace.idFromName(graph_id);
var stub = namespace.get(do_id);
if(logseq.db_sync.common.upgrade_request_QMARK_(request)){
return stub.fetch(request);
} else {
new_url.searchParams.set("graph-id",graph_id);

var rewritten = logseq.db_sync.platform.core.request(new_url.toString(),request);
return stub.fetch(rewritten);
}
})():access_resp));
}));
}));
}
} else {
return logseq.db_sync.worker.http.bad_request("missing graph id");
}
} else {
return logseq.db_sync.worker.http.not_found();

}
}
}
}
}
}
});

//# sourceMappingURL=logseq.db_sync.worker.dispatch.js.map
