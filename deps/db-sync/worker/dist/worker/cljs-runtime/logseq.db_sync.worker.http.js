import "./cljs_env.js";
import "./cljs.core.js";
import "./logseq.db_sync.common.js";
import "./logseq.db_sync.malli_schema.js";
import "./logseq.db_sync.worker.coerce.js";
goog.provide('logseq.db_sync.worker.http');
logseq.db_sync.worker.http.coerce_http_request = (function logseq$db_sync$worker$http$coerce_http_request(schema_key,body){
var temp__5802__auto__ = cljs.core.get.cljs$core$IFn$_invoke$arity$2(logseq.db_sync.malli_schema.http_request_coercers,schema_key);
if(cljs.core.truth_(temp__5802__auto__)){
var coercer = temp__5802__auto__;
var coerced = logseq.db_sync.worker.coerce.coerce(coercer,body,new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"schema","schema",-1582001791),schema_key,new cljs.core.Keyword(null,"dir","dir",1734754661),new cljs.core.Keyword(null,"request","request",1772954723)], null));
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(coerced,logseq.db_sync.worker.coerce.invalid_coerce)){
return null;
} else {
return coerced;
}
} else {
return body;
}
});
logseq.db_sync.worker.http.json_response = (function logseq$db_sync$worker$http$json_response(var_args){
var G__40572 = arguments.length;
switch (G__40572) {
case 2:
return logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2((arguments[(0)]),(arguments[(1)]));

break;
case 3:
return logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$3((arguments[(0)]),(arguments[(1)]),(arguments[(2)]));

break;
default:
throw (new Error(["Invalid arity: ",arguments.length].join("")));

}
});

(logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2 = (function (schema_key,data){
return logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$3(schema_key,data,(200));
}));

(logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$3 = (function (schema_key,data,status){
var temp__5802__auto__ = cljs.core.get.cljs$core$IFn$_invoke$arity$2(logseq.db_sync.malli_schema.http_response_coercers,schema_key);
if(cljs.core.truth_(temp__5802__auto__)){
var coercer = temp__5802__auto__;
var coerced = logseq.db_sync.worker.coerce.coerce(coercer,data,new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"schema","schema",-1582001791),schema_key,new cljs.core.Keyword(null,"dir","dir",1734754661),new cljs.core.Keyword(null,"response","response",-1068424192)], null));
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(coerced,logseq.db_sync.worker.coerce.invalid_coerce)){
return logseq.db_sync.common.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"error","error",-978969032),"server error"], null),(500));
} else {
return logseq.db_sync.common.json_response.cljs$core$IFn$_invoke$arity$2(coerced,status);
}
} else {
return logseq.db_sync.common.json_response.cljs$core$IFn$_invoke$arity$2(data,status);
}
}));

(logseq.db_sync.worker.http.json_response.cljs$lang$maxFixedArity = 3);

logseq.db_sync.worker.http.error_response = (function logseq$db_sync$worker$http$error_response(message,status){
return logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$3(new cljs.core.Keyword(null,"error","error",-978969032),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"error","error",-978969032),message], null),status);
});
logseq.db_sync.worker.http.bad_request = (function logseq$db_sync$worker$http$bad_request(message){
return logseq.db_sync.worker.http.error_response(message,(400));
});
logseq.db_sync.worker.http.unauthorized = (function logseq$db_sync$worker$http$unauthorized(){
return logseq.db_sync.worker.http.error_response("unauthorized",(401));
});
logseq.db_sync.worker.http.forbidden = (function logseq$db_sync$worker$http$forbidden(){
return logseq.db_sync.worker.http.error_response("forbidden",(403));
});
logseq.db_sync.worker.http.not_found = (function logseq$db_sync$worker$http$not_found(){
return logseq.db_sync.worker.http.error_response("not found",(404));
});

//# sourceMappingURL=logseq.db_sync.worker.http.js.map
