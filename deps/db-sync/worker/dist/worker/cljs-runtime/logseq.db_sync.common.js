import "./cljs_env.js";
import "./cljs.core.js";
import "./clojure.string.js";
import "./logseq.db_sync.platform.core.js";
import "./logseq.db.sqlite.util.js";
import "./promesa.core.js";
goog.provide('logseq.db_sync.common');
logseq.db_sync.common.text_decoder = (new TextDecoder());
logseq.db_sync.common.cors_headers = (function logseq$db_sync$common$cors_headers(){
return ({"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "content-type,content-encoding,authorization,x-amz-meta-checksum,x-amz-meta-type", "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS,HEAD", "Access-Control-Expose-Headers": "content-type,content-encoding,cache-control,x-asset-type"});
});
logseq.db_sync.common.json_response = (function logseq$db_sync$common$json_response(var_args){
var G__40507 = arguments.length;
switch (G__40507) {
case 1:
return logseq.db_sync.common.json_response.cljs$core$IFn$_invoke$arity$1((arguments[(0)]));

break;
case 2:
return logseq.db_sync.common.json_response.cljs$core$IFn$_invoke$arity$2((arguments[(0)]),(arguments[(1)]));

break;
default:
throw (new Error(["Invalid arity: ",arguments.length].join("")));

}
});

(logseq.db_sync.common.json_response.cljs$core$IFn$_invoke$arity$1 = (function (data){
return logseq.db_sync.common.json_response.cljs$core$IFn$_invoke$arity$2(data,(200));
}));

(logseq.db_sync.common.json_response.cljs$core$IFn$_invoke$arity$2 = (function (data,status){
return logseq.db_sync.platform.core.response(JSON.stringify(cljs.core.clj__GT_js(data)),({"status": status, "headers": Object.assign(({"content-type": "application/json"}),logseq.db_sync.common.cors_headers())}));
}));

(logseq.db_sync.common.json_response.cljs$lang$maxFixedArity = 2);

logseq.db_sync.common.options_response = (function logseq$db_sync$common$options_response(){
return logseq.db_sync.platform.core.response(null,({"status": (204), "headers": logseq.db_sync.common.cors_headers()}));
});
logseq.db_sync.common.bad_request = (function logseq$db_sync$common$bad_request(message){
return logseq.db_sync.common.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"error","error",-978969032),message], null),(400));
});
logseq.db_sync.common.unauthorized = (function logseq$db_sync$common$unauthorized(){
return logseq.db_sync.common.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"error","error",-978969032),"unauthorized"], null),(401));
});
logseq.db_sync.common.forbidden = (function logseq$db_sync$common$forbidden(){
return logseq.db_sync.common.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"error","error",-978969032),"forbidden"], null),(403));
});
logseq.db_sync.common.not_found = (function logseq$db_sync$common$not_found(){
return logseq.db_sync.common.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"error","error",-978969032),"not found"], null),(404));
});
logseq.db_sync.common.get_sql_rows = (function logseq$db_sync$common$get_sql_rows(result){
var iter_fn = (cljs.core.truth_(result)?(result[Symbol.iterator]):null);
if((result == null)){
return cljs.core.PersistentVector.EMPTY;
} else {
if(cljs.core.fn_QMARK_(result.toArray)){
return result.toArray();
} else {
if(cljs.core.fn_QMARK_(iter_fn)){
return cljs.core.vec(Array.from(result));
} else {
if(cljs.core.truth_(cljs.core.array_QMARK_(result.results))){
return result.results;
} else {
if(cljs.core.truth_(cljs.core.array_QMARK_(result.rows))){
return result.rows;
} else {
if(cljs.core.truth_(cljs.core.array_QMARK_(result))){
if(cljs.core.empty_QMARK_(result)){
return cljs.core.PersistentVector.EMPTY;
} else {
var first_row = cljs.core.first(result);
if(cljs.core.truth_(cljs.core.array_QMARK_(first_row.results))){
return first_row.results;
} else {
if(cljs.core.truth_(cljs.core.array_QMARK_(first_row.rows))){
return first_row.rows;
} else {
return result;

}
}
}
} else {
return cljs.core.PersistentVector.EMPTY;

}
}
}
}
}
}
});
logseq.db_sync.common.sql_exec = (function logseq$db_sync$common$sql_exec(var_args){
var args__5882__auto__ = [];
var len__5876__auto___40567 = arguments.length;
var i__5877__auto___40568 = (0);
while(true){
if((i__5877__auto___40568 < len__5876__auto___40567)){
args__5882__auto__.push((arguments[i__5877__auto___40568]));

var G__40569 = (i__5877__auto___40568 + (1));
i__5877__auto___40568 = G__40569;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((2) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((2)),(0),null)):null);
return logseq.db_sync.common.sql_exec.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),(arguments[(1)]),argseq__5883__auto__);
});

(logseq.db_sync.common.sql_exec.cljs$core$IFn$_invoke$arity$variadic = (function (sql,sql_str,args){
return sql.exec.apply(sql,cljs.core.to_array(cljs.core.cons(sql_str,args)));
}));

(logseq.db_sync.common.sql_exec.cljs$lang$maxFixedArity = (2));

/** @this {Function} */
(logseq.db_sync.common.sql_exec.cljs$lang$applyTo = (function (seq40519){
var G__40520 = cljs.core.first(seq40519);
var seq40519__$1 = cljs.core.next(seq40519);
var G__40521 = cljs.core.first(seq40519__$1);
var seq40519__$2 = cljs.core.next(seq40519__$1);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__40520,G__40521,seq40519__$2);
}));

logseq.db_sync.common._LT_d1_run = (function logseq$db_sync$common$_LT_d1_run(var_args){
var args__5882__auto__ = [];
var len__5876__auto___40579 = arguments.length;
var i__5877__auto___40580 = (0);
while(true){
if((i__5877__auto___40580 < len__5876__auto___40579)){
args__5882__auto__.push((arguments[i__5877__auto___40580]));

var G__40581 = (i__5877__auto___40580 + (1));
i__5877__auto___40580 = G__40581;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((2) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((2)),(0),null)):null);
return logseq.db_sync.common._LT_d1_run.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),(arguments[(1)]),argseq__5883__auto__);
});

(logseq.db_sync.common._LT_d1_run.cljs$core$IFn$_invoke$arity$variadic = (function (db,sql_str,args){
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(db.prepare(sql_str)),(function (stmt){
return promesa.protocols._mcat(promesa.protocols._promise(((cljs.core.seq(args))?stmt.bind.apply(stmt,cljs.core.to_array(args)):stmt)),(function (stmt__$1){
return promesa.protocols._promise(stmt__$1.run());
}));
}));
}));
}));

(logseq.db_sync.common._LT_d1_run.cljs$lang$maxFixedArity = (2));

/** @this {Function} */
(logseq.db_sync.common._LT_d1_run.cljs$lang$applyTo = (function (seq40532){
var G__40533 = cljs.core.first(seq40532);
var seq40532__$1 = cljs.core.next(seq40532);
var G__40534 = cljs.core.first(seq40532__$1);
var seq40532__$2 = cljs.core.next(seq40532__$1);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__40533,G__40534,seq40532__$2);
}));

logseq.db_sync.common._LT_d1_all = (function logseq$db_sync$common$_LT_d1_all(var_args){
var args__5882__auto__ = [];
var len__5876__auto___40582 = arguments.length;
var i__5877__auto___40583 = (0);
while(true){
if((i__5877__auto___40583 < len__5876__auto___40582)){
args__5882__auto__.push((arguments[i__5877__auto___40583]));

var G__40584 = (i__5877__auto___40583 + (1));
i__5877__auto___40583 = G__40584;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((2) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((2)),(0),null)):null);
return logseq.db_sync.common._LT_d1_all.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),(arguments[(1)]),argseq__5883__auto__);
});

(logseq.db_sync.common._LT_d1_all.cljs$core$IFn$_invoke$arity$variadic = (function (db,sql_or_opts,more){
var vec__40540 = ((cljs.core.map_QMARK_(sql_or_opts))?new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [sql_or_opts,cljs.core.first(more),cljs.core.rest(more)], null):new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [null,sql_or_opts,more], null));
var opts = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40540,(0),null);
var sql_str = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40540,(1),null);
var args = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40540,(2),null);
var session_mode = new cljs.core.Keyword(null,"session","session",1008279103).cljs$core$IFn$_invoke$arity$1(opts);
var session = (((!((session_mode == null))))?db.withSession(session_mode):db.withSession());
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(session.prepare(sql_str)),(function (stmt){
return promesa.protocols._mcat(promesa.protocols._promise(((cljs.core.seq(args))?stmt.bind.apply(stmt,cljs.core.to_array(args)):stmt)),(function (stmt__$1){
return promesa.protocols._promise(stmt__$1.all());
}));
}));
}));
}));

(logseq.db_sync.common._LT_d1_all.cljs$lang$maxFixedArity = (2));

/** @this {Function} */
(logseq.db_sync.common._LT_d1_all.cljs$lang$applyTo = (function (seq40537){
var G__40538 = cljs.core.first(seq40537);
var seq40537__$1 = cljs.core.next(seq40537);
var G__40539 = cljs.core.first(seq40537__$1);
var seq40537__$2 = cljs.core.next(seq40537__$1);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__40538,G__40539,seq40537__$2);
}));

logseq.db_sync.common.read_json = (function logseq$db_sync$common$read_json(request){
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(request.text()),(function (body){
return promesa.protocols._promise(((cljs.core.seq(body))?JSON.parse(body):null));
}));
}));
});
logseq.db_sync.common.read_transit = (function logseq$db_sync$common$read_transit(value){
if(typeof value === 'string'){
return logseq.db.sqlite.util.read_transit_str(value);
} else {
return null;
}
});
logseq.db_sync.common.write_transit = (function logseq$db_sync$common$write_transit(value){
return logseq.db.sqlite.util.write_transit_str(value);
});
logseq.db_sync.common.now_ms = (function logseq$db_sync$common$now_ms(){
return Date.now();
});
logseq.db_sync.common.upgrade_request_QMARK_ = (function logseq$db_sync$common$upgrade_request_QMARK_(request){
return cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2("websocket",clojure.string.lower_case((function (){var or__5142__auto__ = request.headers.get("upgrade");
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return "";
}
})()));
});

//# sourceMappingURL=logseq.db_sync.common.js.map
