import "./cljs_env.js";
import "./cljs.core.js";
import "./clojure.string.js";
goog.provide('logseq.db_sync.batch');
logseq.db_sync.batch.max_sql_params = (99);
logseq.db_sync.batch.row_param_count = (3);
logseq.db_sync.batch.rows__GT_insert_batches = (function logseq$db_sync$batch$rows__GT_insert_batches(table,rows,p__50799){
var map__50800 = p__50799;
var map__50800__$1 = cljs.core.__destructure_map(map__50800);
var max_params = cljs.core.get.cljs$core$IFn$_invoke$arity$3(map__50800__$1,new cljs.core.Keyword(null,"max-params","max-params",1318024818),logseq.db_sync.batch.max_sql_params);
if(cljs.core.seq(rows)){
var max_rows = cljs.core.max.cljs$core$IFn$_invoke$arity$2((1),Math.floor((max_params / logseq.db_sync.batch.row_param_count)));
var batches = cljs.core.partition_all.cljs$core$IFn$_invoke$arity$2(max_rows,rows);
return cljs.core.mapv.cljs$core$IFn$_invoke$arity$2((function (batch){
var placeholders = clojure.string.join.cljs$core$IFn$_invoke$arity$2(",",cljs.core.repeat.cljs$core$IFn$_invoke$arity$2(cljs.core.count(batch),"(?, ?, ?)"));
var sql = (""+"insert or replace into "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(table)+" (addr, content, addresses) values "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(placeholders));
var args = cljs.core.vec(cljs.core.mapcat.cljs$core$IFn$_invoke$arity$variadic(cljs.core.identity,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([batch], 0)));
return new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"sql","sql",1251448786),sql,new cljs.core.Keyword(null,"args","args",1315556576),args], null);
}),batches);
} else {
return cljs.core.PersistentVector.EMPTY;
}
});

//# sourceMappingURL=logseq.db_sync.batch.js.map
