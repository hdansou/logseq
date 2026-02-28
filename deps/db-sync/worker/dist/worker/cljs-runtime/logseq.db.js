import "./cljs_env.js";
import "./cljs.core.js";
import "./clojure.set.js";
import "./clojure.string.js";
import "./clojure.walk.js";
import "./datascript.conn.js";
import "./datascript.core.js";
import "./datascript.impl.entity.js";
import "./logseq.common.config.js";
import "./logseq.common.plural.js";
import "./logseq.common.util.js";
import "./logseq.common.uuid.js";
import "./logseq.db.common.delete_blocks.js";
import "./logseq.db.common.entity_plus.js";
import "./logseq.db.common.initial_data.js";
import "./logseq.db.common.normalize.js";
import "./logseq.db.frontend.class.js";
import "./logseq.db.frontend.db.js";
import "./logseq.db.frontend.entity_util.js";
import "./logseq.db.frontend.property.js";
import "./logseq.db.frontend.validate.js";
import "./logseq.db.sqlite.util.js";
goog.provide('logseq.db');
logseq.db.built_in_QMARK_ = logseq.db.frontend.entity_util.built_in_QMARK_;
logseq.db.built_in_class_property_QMARK_ = logseq.db.frontend.db.built_in_class_property_QMARK_;
logseq.db.private_built_in_page_QMARK_ = logseq.db.frontend.db.private_built_in_page_QMARK_;
logseq.db.write_transit_str = logseq.db.sqlite.util.write_transit_str;
logseq.db.read_transit_str = logseq.db.sqlite.util.read_transit_str;
logseq.db.build_favorite_tx = logseq.db.frontend.db.build_favorite_tx;
if((typeof logseq !== 'undefined') && (typeof logseq.db !== 'undefined') && (typeof logseq.db._STAR_transact_fn !== 'undefined')){
} else {
logseq.db._STAR_transact_fn = cljs.core.atom.cljs$core$IFn$_invoke$arity$1(null);
}
if((typeof logseq !== 'undefined') && (typeof logseq.db !== 'undefined') && (typeof logseq.db._STAR_transact_invalid_callback !== 'undefined')){
} else {
logseq.db._STAR_transact_invalid_callback = cljs.core.atom.cljs$core$IFn$_invoke$arity$1(null);
}
if((typeof logseq !== 'undefined') && (typeof logseq.db !== 'undefined') && (typeof logseq.db._STAR_transact_pipeline_fn !== 'undefined')){
} else {
logseq.db._STAR_transact_pipeline_fn = cljs.core.atom.cljs$core$IFn$_invoke$arity$1(null);
}
logseq.db.register_transact_fn_BANG_ = (function logseq$db$register_transact_fn_BANG_(f){
if(cljs.core.truth_(f)){
return cljs.core.reset_BANG_(logseq.db._STAR_transact_fn,f);
} else {
return null;
}
});
logseq.db.register_transact_invalid_callback_fn_BANG_ = (function logseq$db$register_transact_invalid_callback_fn_BANG_(f){
if(cljs.core.truth_(f)){
return cljs.core.reset_BANG_(logseq.db._STAR_transact_invalid_callback,f);
} else {
return null;
}
});
logseq.db.register_transact_pipeline_fn_BANG_ = (function logseq$db$register_transact_pipeline_fn_BANG_(f){
if(cljs.core.truth_(f)){
return cljs.core.reset_BANG_(logseq.db._STAR_transact_pipeline_fn,f);
} else {
return null;
}
});
logseq.db.remove_temp_block_data = (function logseq$db$remove_temp_block_data(tx_data){
var remove_block_temp_f = (function (m){
return cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,cljs.core.remove.cljs$core$IFn$_invoke$arity$2((function (p__41006){
var vec__41007 = p__41006;
var k = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__41007,(0),null);
var _v = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__41007,(1),null);
return cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2("block.temp",cljs.core.namespace(k));
}),m));
});
return cljs.core.keep.cljs$core$IFn$_invoke$arity$2((function (data){
if(cljs.core.map_QMARK_(data)){
var G__41010 = remove_block_temp_f(data);
if(cljs.core.seq(new cljs.core.Keyword("block","refs","block/refs",-1214495349).cljs$core$IFn$_invoke$arity$1(data))){
return cljs.core.update.cljs$core$IFn$_invoke$arity$3(G__41010,new cljs.core.Keyword("block","refs","block/refs",-1214495349),(function (refs){
return cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (ref){
if(cljs.core.map_QMARK_(ref)){
return remove_block_temp_f(ref);
} else {
return ref;
}
}),refs);
}));
} else {
return G__41010;
}
} else {
if(((cljs.core.vector_QMARK_(data)) && (((cljs.core.contains_QMARK_(new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword("db","retract","db/retract",-1549825231),null,new cljs.core.Keyword("db","add","db/add",235286841),null], null), null),cljs.core.first(data))) && ((((cljs.core.count(data) > (2))) && ((((cljs.core.nth.cljs$core$IFn$_invoke$arity$2(data,(2)) instanceof cljs.core.Keyword)) && (cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2("block.temp",cljs.core.namespace(cljs.core.nth.cljs$core$IFn$_invoke$arity$2(data,(2))))))))))))){
return null;
} else {
return data;

}
}
}),tx_data);
});
logseq.db.entity__GT_db_id = (function logseq$db$entity__GT_db_id(tx_data){
return clojure.walk.prewalk((function (f){
if(datascript.impl.entity.entity_QMARK_(f)){
var temp__5802__auto__ = new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(f);
if(cljs.core.truth_(temp__5802__auto__)){
var id = temp__5802__auto__;
return id;
} else {
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2("ldb/transact! doesn't support Entity",new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"entity","entity",-450970276),f,new cljs.core.Keyword(null,"tx-data","tx-data",934159761),tx_data], null));
}
} else {
return f;
}
}),tx_data);
});
logseq.db.throw_if_page_has_block_parent_BANG_ = (function logseq$db$throw_if_page_has_block_parent_BANG_(db,tx_data){
if(cljs.core.truth_(cljs.core.some((function (d){
var and__5140__auto__ = new cljs.core.Keyword(null,"added","added",2057651688).cljs$core$IFn$_invoke$arity$1(d);
if(cljs.core.truth_(and__5140__auto__)){
var and__5140__auto____$1 = cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("block","parent","block/parent",-918309064),new cljs.core.Keyword(null,"a","a",-2123407586).cljs$core$IFn$_invoke$arity$1(d));
if(and__5140__auto____$1){
var and__5140__auto____$2 = logseq.db.frontend.entity_util.page_QMARK_((function (){var G__41014 = db;
var G__41015 = new cljs.core.Keyword(null,"e","e",1381269198).cljs$core$IFn$_invoke$arity$1(d);
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(G__41014,G__41015) : datascript.core.entity.call(null,G__41014,G__41015));
})());
if(cljs.core.truth_(and__5140__auto____$2)){
return cljs.core.not(logseq.db.frontend.entity_util.page_QMARK_((function (){var G__41016 = db;
var G__41017 = new cljs.core.Keyword(null,"v","v",21465059).cljs$core$IFn$_invoke$arity$1(d);
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(G__41016,G__41017) : datascript.core.entity.call(null,G__41016,G__41017));
})()));
} else {
return and__5140__auto____$2;
}
} else {
return and__5140__auto____$1;
}
} else {
return and__5140__auto__;
}
}),tx_data))){
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2("Page can't have block as parent",new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"tx-data","tx-data",934159761),tx_data], null));
} else {
return null;
}
});
logseq.db.transact_sync = (function logseq$db$transact_sync(conn,tx_data,tx_meta){
try{var db = cljs.core.deref(conn);
var db_based_QMARK_ = logseq.db.common.entity_plus.db_based_graph_QMARK_(db);
if(cljs.core.truth_((function (){var and__5140__auto__ = db_based_QMARK_;
if(cljs.core.truth_(and__5140__auto__)){
return cljs.core.not((function (){var or__5142__auto__ = new cljs.core.Keyword(null,"batch-temp-conn?","batch-temp-conn?",1353825284).cljs$core$IFn$_invoke$arity$1(cljs.core.deref(conn));
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
var or__5142__auto____$1 = new cljs.core.Keyword(null,"rtc-download-graph?","rtc-download-graph?",-1013530237).cljs$core$IFn$_invoke$arity$1(tx_meta);
if(cljs.core.truth_(or__5142__auto____$1)){
return or__5142__auto____$1;
} else {
var or__5142__auto____$2 = new cljs.core.Keyword(null,"reset-conn!","reset-conn!",-325354379).cljs$core$IFn$_invoke$arity$1(tx_meta);
if(cljs.core.truth_(or__5142__auto____$2)){
return or__5142__auto____$2;
} else {
var or__5142__auto____$3 = new cljs.core.Keyword(null,"initial-db?","initial-db?",-930665302).cljs$core$IFn$_invoke$arity$1(tx_meta);
if(cljs.core.truth_(or__5142__auto____$3)){
return or__5142__auto____$3;
} else {
var or__5142__auto____$4 = new cljs.core.Keyword(null,"skip-validate-db?","skip-validate-db?",-140194140).cljs$core$IFn$_invoke$arity$2(tx_meta,false);
if(cljs.core.truth_(or__5142__auto____$4)){
return or__5142__auto____$4;
} else {
return new cljs.core.Keyword("logseq.graph-parser.exporter","new-graph?","logseq.graph-parser.exporter/new-graph?",-2038807565).cljs$core$IFn$_invoke$arity$1(tx_meta);
}
}
}
}
}
})());
} else {
return and__5140__auto__;
}
})())){
var tx_report_STAR_ = (datascript.core.with$.cljs$core$IFn$_invoke$arity$3 ? datascript.core.with$.cljs$core$IFn$_invoke$arity$3(db,tx_data,tx_meta) : datascript.core.with$.call(null,db,tx_data,tx_meta));
var pipeline_f = cljs.core.deref(logseq.db._STAR_transact_pipeline_fn);
var tx_report = (function (){var temp__5802__auto__ = pipeline_f;
if(cljs.core.truth_(temp__5802__auto__)){
var f = temp__5802__auto__;
return (f.cljs$core$IFn$_invoke$arity$1 ? f.cljs$core$IFn$_invoke$arity$1(tx_report_STAR_) : f.call(null,tx_report_STAR_));
} else {
return tx_report_STAR_;
}
})();
var _ = logseq.db.throw_if_page_has_block_parent_BANG_(new cljs.core.Keyword(null,"db-after","db-after",-571884666).cljs$core$IFn$_invoke$arity$1(tx_report),new cljs.core.Keyword(null,"tx-data","tx-data",934159761).cljs$core$IFn$_invoke$arity$1(tx_report));
var vec__41019 = logseq.db.frontend.validate.validate_tx_report(tx_report,null);
var validate_result = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__41019,(0),null);
var errors = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__41019,(1),null);
if(cljs.core.truth_(validate_result)){
if(cljs.core.truth_((function (){var and__5140__auto__ = tx_report;
if(cljs.core.truth_(and__5140__auto__)){
return cljs.core.seq(new cljs.core.Keyword(null,"tx-data","tx-data",934159761).cljs$core$IFn$_invoke$arity$1(tx_report));
} else {
return and__5140__auto__;
}
})())){
cljs.core.reset_BANG_(conn,new cljs.core.Keyword(null,"db-after","db-after",-571884666).cljs$core$IFn$_invoke$arity$1(tx_report));

datascript.conn.store_after_transact_BANG_(conn,tx_report);

datascript.conn.run_callbacks(conn,tx_report);
} else {
}
} else {
var temp__5804__auto___41121 = cljs.core.deref(logseq.db._STAR_transact_invalid_callback);
if(cljs.core.truth_(temp__5804__auto___41121)){
var f_41122 = temp__5804__auto___41121;
(f_41122.cljs$core$IFn$_invoke$arity$2 ? f_41122.cljs$core$IFn$_invoke$arity$2(tx_report,errors) : f_41122.call(null,tx_report,errors));
} else {
}

throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2("DB write failed with invalid data",new cljs.core.PersistentArrayMap(null, 4, [new cljs.core.Keyword(null,"tx-meta","tx-meta",1159283194),tx_meta,new cljs.core.Keyword(null,"tx-data","tx-data",934159761),tx_data,new cljs.core.Keyword(null,"errors","errors",-908790718),errors,new cljs.core.Keyword(null,"pipeline-tx-data","pipeline-tx-data",-1815328125),cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (p__41022){
var vec__41023 = p__41022;
var e = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__41023,(0),null);
var a = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__41023,(1),null);
var v = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__41023,(2),null);
var t = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__41023,(3),null);
return new cljs.core.PersistentVector(null, 4, 5, cljs.core.PersistentVector.EMPTY_NODE, [e,a,v,t], null);
}),new cljs.core.Keyword(null,"tx-data","tx-data",934159761).cljs$core$IFn$_invoke$arity$1(tx_report))], null));

}

return tx_report;
} else {
return (datascript.core.transact_BANG_.cljs$core$IFn$_invoke$arity$3 ? datascript.core.transact_BANG_.cljs$core$IFn$_invoke$arity$3(conn,tx_data,tx_meta) : datascript.core.transact_BANG_.call(null,conn,tx_data,tx_meta));
}
}catch (e41018){var e = e41018;
cljs.core.prn.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"debug","debug",-1608172596),new cljs.core.Keyword(null,"transact-failed","transact-failed",-1049262566),new cljs.core.Keyword(null,"tx-meta","tx-meta",1159283194),tx_meta,new cljs.core.Keyword(null,"tx-data","tx-data",934159761),tx_data], 0));

console.error(e);

throw e;
}});
/**
 * `repo-or-conn`: repo for UI thread and conn for worker/node
 */
logseq.db.transact_BANG_ = (function logseq$db$transact_BANG_(var_args){
var G__41027 = arguments.length;
switch (G__41027) {
case 2:
return logseq.db.transact_BANG_.cljs$core$IFn$_invoke$arity$2((arguments[(0)]),(arguments[(1)]));

break;
case 3:
return logseq.db.transact_BANG_.cljs$core$IFn$_invoke$arity$3((arguments[(0)]),(arguments[(1)]),(arguments[(2)]));

break;
default:
throw (new Error(["Invalid arity: ",arguments.length].join("")));

}
});

(logseq.db.transact_BANG_.cljs$core$IFn$_invoke$arity$2 = (function (repo_or_conn,tx_data){
return logseq.db.transact_BANG_.cljs$core$IFn$_invoke$arity$3(repo_or_conn,tx_data,null);
}));

(logseq.db.transact_BANG_.cljs$core$IFn$_invoke$arity$3 = (function (repo_or_conn,tx_data,tx_meta){
var tx_data__$1 = cljs.core.remove.cljs$core$IFn$_invoke$arity$2((function (m){
return ((cljs.core.integer_QMARK_(m)) || (cljs.core.empty_QMARK_(m)));
}),logseq.common.util.fast_remove_nils(cljs.core.remove.cljs$core$IFn$_invoke$arity$2((function (m){
return ((cljs.core.map_QMARK_(m)) && (cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db","ident","db/ident",-737096).cljs$core$IFn$_invoke$arity$1(m),new cljs.core.Keyword("block","path-refs","block/path-refs",-2109181352))));
}),logseq.db.remove_temp_block_data(cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (m){
if(cljs.core.map_QMARK_(m)){
var G__41028 = cljs.core.dissoc.cljs$core$IFn$_invoke$arity$variadic(m,new cljs.core.Keyword("block","children","block/children",-1040716209),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword("block","meta","block/meta",1064819153),new cljs.core.Keyword("block","top?","block/top?",-1838733025),new cljs.core.Keyword("block","bottom?","block/bottom?",-1886197289),new cljs.core.Keyword("block","anchor","block/anchor",1325786860),new cljs.core.Keyword("block","level","block/level",1182509971),new cljs.core.Keyword("block","container","block/container",510671002),new cljs.core.Keyword("db","other-tx","db/other-tx",337296620),new cljs.core.Keyword("block","unordered","block/unordered",-772044101)], 0));
if(cljs.core.not(cljs.core.deref(logseq.db._STAR_transact_fn))){
return cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(G__41028,new cljs.core.Keyword("block.temp","load-status","block.temp/load-status",-1066723049));
} else {
return G__41028;
}
} else {
return m;
}
}),logseq.db.entity__GT_db_id(tx_data))))));
var delete_blocks_tx = ((typeof repo_or_conn === 'string')?null:logseq.db.common.delete_blocks.update_refs_history(cljs.core.deref(repo_or_conn),tx_data__$1,tx_meta));
var tx_data__$2 = cljs.core.concat.cljs$core$IFn$_invoke$arity$2(tx_data__$1,delete_blocks_tx);
if(cljs.core.seq(tx_data__$2)){
var temp__5802__auto__ = cljs.core.deref(logseq.db._STAR_transact_fn);
if(cljs.core.truth_(temp__5802__auto__)){
var transact_fn = temp__5802__auto__;
return (transact_fn.cljs$core$IFn$_invoke$arity$3 ? transact_fn.cljs$core$IFn$_invoke$arity$3(repo_or_conn,tx_data__$2,tx_meta) : transact_fn.call(null,repo_or_conn,tx_data__$2,tx_meta));
} else {
return logseq.db.transact_sync(repo_or_conn,tx_data__$2,tx_meta);
}
} else {
return null;
}
}));

(logseq.db.transact_BANG_.cljs$lang$maxFixedArity = 3);

logseq.db.remove_conflict_datoms = logseq.db.common.normalize.remove_conflict_datoms;
/**
 * Validate db and store once for a batch transaction, the `temp` conn can still load data from disk,
 *   however it can't write to the disk.
 */
logseq.db.transact_with_temp_conn_BANG_ = (function logseq$db$transact_with_temp_conn_BANG_(var_args){
var args__5882__auto__ = [];
var len__5876__auto___41124 = arguments.length;
var i__5877__auto___41125 = (0);
while(true){
if((i__5877__auto___41125 < len__5876__auto___41124)){
args__5882__auto__.push((arguments[i__5877__auto___41125]));

var G__41126 = (i__5877__auto___41125 + (1));
i__5877__auto___41125 = G__41126;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((3) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((3)),(0),null)):null);
return logseq.db.transact_with_temp_conn_BANG_.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),(arguments[(1)]),(arguments[(2)]),argseq__5883__auto__);
});

(logseq.db.transact_with_temp_conn_BANG_.cljs$core$IFn$_invoke$arity$variadic = (function (conn,tx_meta,batch_tx_fn,p__41033){
var map__41034 = p__41033;
var map__41034__$1 = cljs.core.__destructure_map(map__41034);
var listen_db = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__41034__$1,new cljs.core.Keyword(null,"listen-db","listen-db",1775571058));
var temp_conn = (function (){var G__41035 = cljs.core.deref(conn);
return (datascript.core.conn_from_db.cljs$core$IFn$_invoke$arity$1 ? datascript.core.conn_from_db.cljs$core$IFn$_invoke$arity$1(G__41035) : datascript.core.conn_from_db.call(null,G__41035));
})();
var _STAR_batch_tx_data = cljs.core.volatile_BANG_(cljs.core.PersistentVector.EMPTY);
cljs.core.swap_BANG_.cljs$core$IFn$_invoke$arity$variadic(temp_conn,cljs.core.assoc,new cljs.core.Keyword(null,"skip-store?","skip-store?",-484019625),true,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"batch-temp-conn?","batch-temp-conn?",1353825284),true], 0));

var G__41036_41127 = temp_conn;
var G__41037_41128 = new cljs.core.Keyword("logseq.db","temp-conn-batch-tx","logseq.db/temp-conn-batch-tx",691657992);
var G__41038_41129 = (function (p__41039){
var map__41040 = p__41039;
var map__41040__$1 = cljs.core.__destructure_map(map__41040);
var tx_report = map__41040__$1;
var tx_data = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__41040__$1,new cljs.core.Keyword(null,"tx-data","tx-data",934159761));
_STAR_batch_tx_data.cljs$core$IVolatile$_vreset_BANG_$arity$2(null,cljs.core.into.cljs$core$IFn$_invoke$arity$2(_STAR_batch_tx_data.cljs$core$IDeref$_deref$arity$1(null),tx_data));

if(cljs.core.fn_QMARK_(listen_db)){
return (listen_db.cljs$core$IFn$_invoke$arity$1 ? listen_db.cljs$core$IFn$_invoke$arity$1(tx_report) : listen_db.call(null,tx_report));
} else {
return null;
}
});
(datascript.core.listen_BANG_.cljs$core$IFn$_invoke$arity$3 ? datascript.core.listen_BANG_.cljs$core$IFn$_invoke$arity$3(G__41036_41127,G__41037_41128,G__41038_41129) : datascript.core.listen_BANG_.call(null,G__41036_41127,G__41037_41128,G__41038_41129));

(batch_tx_fn.cljs$core$IFn$_invoke$arity$2 ? batch_tx_fn.cljs$core$IFn$_invoke$arity$2(temp_conn,_STAR_batch_tx_data) : batch_tx_fn.call(null,temp_conn,_STAR_batch_tx_data));

var tx_data = cljs.core.deref(_STAR_batch_tx_data);
var temp_after_db = cljs.core.deref(temp_conn);
(datascript.core.unlisten_BANG_.cljs$core$IFn$_invoke$arity$2 ? datascript.core.unlisten_BANG_.cljs$core$IFn$_invoke$arity$2(temp_conn,new cljs.core.Keyword("logseq.db","temp-conn-batch-tx","logseq.db/temp-conn-batch-tx",691657992)) : datascript.core.unlisten_BANG_.call(null,temp_conn,new cljs.core.Keyword("logseq.db","temp-conn-batch-tx","logseq.db/temp-conn-batch-tx",691657992)));

cljs.core.reset_BANG_(temp_conn,null);

cljs.core.vreset_BANG_(_STAR_batch_tx_data,null);

if(cljs.core.seq(tx_data)){
var tx_data_SINGLEQUOTE_ = logseq.db.common.normalize.replace_attr_retract_with_retract_entity(temp_after_db,(logseq.db.remove_conflict_datoms.cljs$core$IFn$_invoke$arity$1 ? logseq.db.remove_conflict_datoms.cljs$core$IFn$_invoke$arity$1(tx_data) : logseq.db.remove_conflict_datoms.call(null,tx_data)));
return logseq.db.transact_BANG_.cljs$core$IFn$_invoke$arity$3(conn,tx_data_SINGLEQUOTE_,tx_meta);
} else {
return null;
}
}));

(logseq.db.transact_with_temp_conn_BANG_.cljs$lang$maxFixedArity = (3));

/** @this {Function} */
(logseq.db.transact_with_temp_conn_BANG_.cljs$lang$applyTo = (function (seq41029){
var G__41030 = cljs.core.first(seq41029);
var seq41029__$1 = cljs.core.next(seq41029);
var G__41031 = cljs.core.first(seq41029__$1);
var seq41029__$2 = cljs.core.next(seq41029__$1);
var G__41032 = cljs.core.first(seq41029__$2);
var seq41029__$3 = cljs.core.next(seq41029__$2);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__41030,G__41031,G__41032,seq41029__$3);
}));

logseq.db.page_QMARK_ = logseq.db.frontend.entity_util.page_QMARK_;
logseq.db.internal_page_QMARK_ = logseq.db.frontend.entity_util.internal_page_QMARK_;
logseq.db.class_QMARK_ = logseq.db.frontend.entity_util.class_QMARK_;
logseq.db.property_QMARK_ = logseq.db.frontend.entity_util.property_QMARK_;
logseq.db.closed_value_QMARK_ = logseq.db.frontend.entity_util.closed_value_QMARK_;
logseq.db.journal_QMARK_ = logseq.db.frontend.entity_util.journal_QMARK_;
logseq.db.hidden_QMARK_ = logseq.db.frontend.entity_util.hidden_QMARK_;
logseq.db.object_QMARK_ = logseq.db.frontend.entity_util.object_QMARK_;
logseq.db.asset_QMARK_ = logseq.db.frontend.entity_util.asset_QMARK_;
logseq.db.public_built_in_property_QMARK_ = logseq.db.frontend.property.public_built_in_property_QMARK_;
logseq.db.get_entity_types = logseq.db.frontend.entity_util.get_entity_types;
logseq.db.internal_tags = logseq.db.frontend.class$.internal_tags;
logseq.db.private_tags = logseq.db.frontend.class$.private_tags;
logseq.db.extends_hidden_tags = logseq.db.frontend.class$.extends_hidden_tags;
logseq.db.hidden_tags = logseq.db.frontend.class$.hidden_tags;
logseq.db.sort_by_order = (function logseq$db$sort_by_order(blocks){
return cljs.core.sort_by.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("block","order","block/order",-1429282437),blocks);
});
logseq.db.get_block_and_children_aux = (function logseq$db$get_block_and_children_aux(var_args){
var args__5882__auto__ = [];
var len__5876__auto___41130 = arguments.length;
var i__5877__auto___41131 = (0);
while(true){
if((i__5877__auto___41131 < len__5876__auto___41130)){
args__5882__auto__.push((arguments[i__5877__auto___41131]));

var G__41132 = (i__5877__auto___41131 + (1));
i__5877__auto___41131 = G__41132;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((1) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((1)),(0),null)):null);
return logseq.db.get_block_and_children_aux.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),argseq__5883__auto__);
});

(logseq.db.get_block_and_children_aux.cljs$core$IFn$_invoke$arity$variadic = (function (entity,p__41044){
var map__41045 = p__41044;
var map__41045__$1 = cljs.core.__destructure_map(map__41045);
var opts = map__41045__$1;
var include_property_block_QMARK_ = cljs.core.get.cljs$core$IFn$_invoke$arity$3(map__41045__$1,new cljs.core.Keyword(null,"include-property-block?","include-property-block?",-211563499),false);
var temp__5802__auto__ = logseq.db.sort_by_order((cljs.core.truth_(include_property_block_QMARK_)?(function (){var children = logseq.db.common.entity_plus.lookup_kv_then_entity.cljs$core$IFn$_invoke$arity$2(entity,new cljs.core.Keyword("block","_raw-parent","block/_raw-parent",628025875));
return cljs.core.concat.cljs$core$IFn$_invoke$arity$2(children,cljs.core.keep.cljs$core$IFn$_invoke$arity$2((function (c){
return new cljs.core.Keyword("logseq.property","query","logseq.property/query",-97414126).cljs$core$IFn$_invoke$arity$1(c);
}),children));
})():logseq.db.common.entity_plus.lookup_kv_then_entity.cljs$core$IFn$_invoke$arity$2(entity,new cljs.core.Keyword("block","_parent","block/_parent",-639389670))));
if(cljs.core.truth_(temp__5802__auto__)){
var children = temp__5802__auto__;
return cljs.core.cons(entity,cljs.core.mapcat.cljs$core$IFn$_invoke$arity$variadic((function (p1__41041_SHARP_){
return logseq.db.get_block_and_children_aux.cljs$core$IFn$_invoke$arity$variadic(p1__41041_SHARP_,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([opts], 0));
}),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([children], 0)));
} else {
return new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [entity], null);
}
}));

(logseq.db.get_block_and_children_aux.cljs$lang$maxFixedArity = (1));

/** @this {Function} */
(logseq.db.get_block_and_children_aux.cljs$lang$applyTo = (function (seq41042){
var G__41043 = cljs.core.first(seq41042);
var seq41042__$1 = cljs.core.next(seq41042);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__41043,seq41042__$1);
}));

logseq.db.get_block_and_children = (function logseq$db$get_block_and_children(var_args){
var args__5882__auto__ = [];
var len__5876__auto___41133 = arguments.length;
var i__5877__auto___41134 = (0);
while(true){
if((i__5877__auto___41134 < len__5876__auto___41133)){
args__5882__auto__.push((arguments[i__5877__auto___41134]));

var G__41135 = (i__5877__auto___41134 + (1));
i__5877__auto___41134 = G__41135;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((2) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((2)),(0),null)):null);
return logseq.db.get_block_and_children.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),(arguments[(1)]),argseq__5883__auto__);
});

(logseq.db.get_block_and_children.cljs$core$IFn$_invoke$arity$variadic = (function (db,block_uuid,p__41049){
var map__41050 = p__41049;
var map__41050__$1 = cljs.core.__destructure_map(map__41050);
var opts = map__41050__$1;
var temp__5804__auto__ = (function (){var G__41051 = db;
var G__41052 = new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("block","uuid","block/uuid",-1991494552),block_uuid], null);
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(G__41051,G__41052) : datascript.core.entity.call(null,G__41051,G__41052));
})();
if(cljs.core.truth_(temp__5804__auto__)){
var e = temp__5804__auto__;
return logseq.db.get_block_and_children_aux.cljs$core$IFn$_invoke$arity$variadic(e,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([opts], 0));
} else {
return null;
}
}));

(logseq.db.get_block_and_children.cljs$lang$maxFixedArity = (2));

/** @this {Function} */
(logseq.db.get_block_and_children.cljs$lang$applyTo = (function (seq41046){
var G__41047 = cljs.core.first(seq41046);
var seq41046__$1 = cljs.core.next(seq41046);
var G__41048 = cljs.core.first(seq41046__$1);
var seq41046__$2 = cljs.core.next(seq41046__$1);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__41047,G__41048,seq41046__$2);
}));

/**
 * Return blocks of the designated page, without using cache.
 * page-id - eid
 */
logseq.db.get_page_blocks = (function logseq$db$get_page_blocks(var_args){
var args__5882__auto__ = [];
var len__5876__auto___41136 = arguments.length;
var i__5877__auto___41137 = (0);
while(true){
if((i__5877__auto___41137 < len__5876__auto___41136)){
args__5882__auto__.push((arguments[i__5877__auto___41137]));

var G__41138 = (i__5877__auto___41137 + (1));
i__5877__auto___41137 = G__41138;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((2) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((2)),(0),null)):null);
return logseq.db.get_page_blocks.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),(arguments[(1)]),argseq__5883__auto__);
});

(logseq.db.get_page_blocks.cljs$core$IFn$_invoke$arity$variadic = (function (db,page_id,p__41056){
var map__41057 = p__41056;
var map__41057__$1 = cljs.core.__destructure_map(map__41057);
var pull_keys = cljs.core.get.cljs$core$IFn$_invoke$arity$3(map__41057__$1,new cljs.core.Keyword(null,"pull-keys","pull-keys",-768938808),new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"*","*",345799209,null)], null));
if(cljs.core.truth_(page_id)){
var datoms = datascript.core.datoms.cljs$core$IFn$_invoke$arity$4(db,new cljs.core.Keyword(null,"avet","avet",1383857032),new cljs.core.Keyword("block","page","block/page",822314108),page_id);
var block_eids = cljs.core.mapv.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"e","e",1381269198),datoms);
return (datascript.core.pull_many.cljs$core$IFn$_invoke$arity$3 ? datascript.core.pull_many.cljs$core$IFn$_invoke$arity$3(db,pull_keys,block_eids) : datascript.core.pull_many.call(null,db,pull_keys,block_eids));
} else {
return null;
}
}));

(logseq.db.get_page_blocks.cljs$lang$maxFixedArity = (2));

/** @this {Function} */
(logseq.db.get_page_blocks.cljs$lang$applyTo = (function (seq41053){
var G__41054 = cljs.core.first(seq41053);
var seq41053__$1 = cljs.core.next(seq41053);
var G__41055 = cljs.core.first(seq41053__$1);
var seq41053__$2 = cljs.core.next(seq41053__$1);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__41054,G__41055,seq41053__$2);
}));

logseq.db.get_page_blocks_count = (function logseq$db$get_page_blocks_count(db,page_id){
return cljs.core.count(datascript.core.datoms.cljs$core$IFn$_invoke$arity$4(db,new cljs.core.Keyword(null,"avet","avet",1383857032),new cljs.core.Keyword("block","page","block/page",822314108),page_id));
});
logseq.db.get_block_children_or_property_children = (function logseq$db$get_block_children_or_property_children(block,parent){
var from_property = new cljs.core.Keyword("logseq.property","created-from-property","logseq.property/created-from-property",-861892267).cljs$core$IFn$_invoke$arity$1(block);
var closed_property = new cljs.core.Keyword("block","closed-value-property","block/closed-value-property",1157792813).cljs$core$IFn$_invoke$arity$1(block);
return logseq.db.sort_by_order((cljs.core.truth_(closed_property)?new cljs.core.Keyword("block","_closed-value-property","block/_closed-value-property",-1361451442).cljs$core$IFn$_invoke$arity$1(closed_property):(cljs.core.truth_(from_property)?cljs.core.filter.cljs$core$IFn$_invoke$arity$2((function (e){
return cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(new cljs.core.Keyword("logseq.property","created-from-property","logseq.property/created-from-property",-861892267).cljs$core$IFn$_invoke$arity$1(e)),new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(from_property));
}),new cljs.core.Keyword("block","_raw-parent","block/_raw-parent",628025875).cljs$core$IFn$_invoke$arity$1(parent)):new cljs.core.Keyword("block","_parent","block/_parent",-639389670).cljs$core$IFn$_invoke$arity$1(parent)
)));
});
logseq.db.get_right_sibling = (function logseq$db$get_right_sibling(block){
if(((datascript.impl.entity.entity_QMARK_(block)) || ((block == null)))){
} else {
throw (new Error("Assert failed: (or (de/entity? block) (nil? block))"));
}

var temp__5804__auto__ = new cljs.core.Keyword("block","parent","block/parent",-918309064).cljs$core$IFn$_invoke$arity$1(block);
if(cljs.core.truth_(temp__5804__auto__)){
var parent = temp__5804__auto__;
var children = logseq.db.get_block_children_or_property_children(block,parent);
var right = cljs.core.some((function (child){
if((cljs.core.compare(new cljs.core.Keyword("block","order","block/order",-1429282437).cljs$core$IFn$_invoke$arity$1(child),new cljs.core.Keyword("block","order","block/order",-1429282437).cljs$core$IFn$_invoke$arity$1(block)) > (0))){
return child;
} else {
return null;
}
}),children);
if(cljs.core.not_EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(right),new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(block))){
return right;
} else {
return null;
}
} else {
return null;
}
});
logseq.db.get_left_sibling = (function logseq$db$get_left_sibling(block){
if(((datascript.impl.entity.entity_QMARK_(block)) || ((block == null)))){
} else {
throw (new Error("Assert failed: (or (de/entity? block) (nil? block))"));
}

var temp__5804__auto__ = new cljs.core.Keyword("block","parent","block/parent",-918309064).cljs$core$IFn$_invoke$arity$1(block);
if(cljs.core.truth_(temp__5804__auto__)){
var parent = temp__5804__auto__;
var children = cljs.core.reverse(logseq.db.get_block_children_or_property_children(block,parent));
var left = cljs.core.some((function (child){
if((cljs.core.compare(new cljs.core.Keyword("block","order","block/order",-1429282437).cljs$core$IFn$_invoke$arity$1(child),new cljs.core.Keyword("block","order","block/order",-1429282437).cljs$core$IFn$_invoke$arity$1(block)) < (0))){
return child;
} else {
return null;
}
}),children);
if(cljs.core.not_EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(left),new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(block))){
return left;
} else {
return null;
}
} else {
return null;
}
});
logseq.db.get_down = (function logseq$db$get_down(block){
if(((datascript.impl.entity.entity_QMARK_(block)) || ((block == null)))){
} else {
throw (new Error("Assert failed: (or (de/entity? block) (nil? block))"));
}

return cljs.core.first(logseq.db.sort_by_order(new cljs.core.Keyword("block","_parent","block/_parent",-639389670).cljs$core$IFn$_invoke$arity$1(block)));
});
logseq.db.get_pages = (function logseq$db$get_pages(db){
return cljs.core.remove.cljs$core$IFn$_invoke$arity$2(logseq.db.hidden_QMARK_,cljs.core.map.cljs$core$IFn$_invoke$arity$2(cljs.core.first,(function (){var G__41058 = new cljs.core.PersistentVector(null, 5, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"find","find",496279456),new cljs.core.Symbol(null,"?page-title","?page-title",-2070092934,null),new cljs.core.Keyword(null,"where","where",-2044795965),new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?page","?page",-1343187612,null),new cljs.core.Keyword("block","name","block/name",1619760316),new cljs.core.Symbol(null,"?page-name","?page-name",-1643414076,null)], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [cljs.core.list(new cljs.core.Symbol(null,"get-else","get-else",1312024065,null),new cljs.core.Symbol(null,"$","$",-1580747756,null),new cljs.core.Symbol(null,"?page","?page",-1343187612,null),new cljs.core.Keyword("block","title","block/title",710445684),new cljs.core.Symbol(null,"?page-name","?page-name",-1643414076,null)),new cljs.core.Symbol(null,"?page-title","?page-title",-2070092934,null)], null)], null);
var G__41059 = db;
return (datascript.core.q.cljs$core$IFn$_invoke$arity$2 ? datascript.core.q.cljs$core$IFn$_invoke$arity$2(G__41058,G__41059) : datascript.core.q.call(null,G__41058,G__41059));
})()));
});
logseq.db.get_first_page_by_name = logseq.db.common.initial_data.get_first_page_by_name;
/**
 * Returns all page db ids that given title and one of the given `tags`.
 */
logseq.db.page_exists_QMARK_ = (function logseq$db$page_exists_QMARK_(db,page_name,tags){
if(cljs.core.truth_(page_name)){
var tags_SINGLEQUOTE_ = ((cljs.core.coll_QMARK_(tags))?cljs.core.set(tags):cljs.core.PersistentHashSet.createAsIfByAssoc([tags]));
if(((cljs.core.seq(tags_SINGLEQUOTE_)) && (cljs.core.every_QMARK_(new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword("logseq.class","Property","logseq.class/Property",1038767048),null,new cljs.core.Keyword("logseq.class","Tag","logseq.class/Tag",-538902083),null], null), null),tags_SINGLEQUOTE_)))){
return cljs.core.seq((function (){var G__41060 = new cljs.core.PersistentVector(null, 10, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"find","find",496279456),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?p","?p",-10896580,null),new cljs.core.Symbol(null,"...","...",-1926939749,null)], null),new cljs.core.Keyword(null,"in","in",-1531184865),new cljs.core.Symbol(null,"$","$",-1580747756,null),new cljs.core.Symbol(null,"?name","?name",2050703390,null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?tag-ident","?tag-ident",-1384320875,null),new cljs.core.Symbol(null,"...","...",-1926939749,null)], null),new cljs.core.Keyword(null,"where","where",-2044795965),new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?p","?p",-10896580,null),new cljs.core.Keyword("block","title","block/title",710445684),new cljs.core.Symbol(null,"?name","?name",2050703390,null)], null),new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?p","?p",-10896580,null),new cljs.core.Keyword("block","tags","block/tags",1814948340),new cljs.core.Symbol(null,"?tag","?tag",157764474,null)], null),new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?tag","?tag",157764474,null),new cljs.core.Keyword("db","ident","db/ident",-737096),new cljs.core.Symbol(null,"?tag-ident","?tag-ident",-1384320875,null)], null)], null);
var G__41061 = db;
var G__41062 = page_name;
var G__41063 = tags_SINGLEQUOTE_;
return (datascript.core.q.cljs$core$IFn$_invoke$arity$4 ? datascript.core.q.cljs$core$IFn$_invoke$arity$4(G__41060,G__41061,G__41062,G__41063) : datascript.core.q.call(null,G__41060,G__41061,G__41062,G__41063));
})());
} else {
return cljs.core.seq((function (){var G__41064 = new cljs.core.PersistentVector(null, 10, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"find","find",496279456),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?p","?p",-10896580,null),new cljs.core.Symbol(null,"...","...",-1926939749,null)], null),new cljs.core.Keyword(null,"in","in",-1531184865),new cljs.core.Symbol(null,"$","$",-1580747756,null),new cljs.core.Symbol(null,"?name","?name",2050703390,null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?tag-ident","?tag-ident",-1384320875,null),new cljs.core.Symbol(null,"...","...",-1926939749,null)], null),new cljs.core.Keyword(null,"where","where",-2044795965),new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?p","?p",-10896580,null),new cljs.core.Keyword("block","name","block/name",1619760316),new cljs.core.Symbol(null,"?name","?name",2050703390,null)], null),new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?p","?p",-10896580,null),new cljs.core.Keyword("block","tags","block/tags",1814948340),new cljs.core.Symbol(null,"?tag","?tag",157764474,null)], null),new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?tag","?tag",157764474,null),new cljs.core.Keyword("db","ident","db/ident",-737096),new cljs.core.Symbol(null,"?tag-ident","?tag-ident",-1384320875,null)], null)], null);
var G__41065 = db;
var G__41066 = logseq.common.util.page_name_sanity_lc(page_name);
var G__41067 = tags_SINGLEQUOTE_;
return (datascript.core.q.cljs$core$IFn$_invoke$arity$4 ? datascript.core.q.cljs$core$IFn$_invoke$arity$4(G__41064,G__41065,G__41066,G__41067) : datascript.core.q.call(null,G__41064,G__41065,G__41066,G__41067));
})());
}
} else {
return null;
}
});
/**
 * Get a page given its unsanitized name or uuid
 */
logseq.db.get_page = (function logseq$db$get_page(db,page_id_name_or_uuid){
if(cljs.core.truth_(db)){
if(typeof page_id_name_or_uuid === 'number'){
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,page_id_name_or_uuid) : datascript.core.entity.call(null,db,page_id_name_or_uuid));
} else {
var temp__5802__auto__ = ((cljs.core.uuid_QMARK_(page_id_name_or_uuid))?page_id_name_or_uuid:cljs.core.parse_uuid(page_id_name_or_uuid));
if(cljs.core.truth_(temp__5802__auto__)){
var id = temp__5802__auto__;
var G__41068 = db;
var G__41069 = new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("block","uuid","block/uuid",-1991494552),id], null);
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(G__41068,G__41069) : datascript.core.entity.call(null,G__41068,G__41069));
} else {
var G__41070 = db;
var G__41071 = (function (){var G__41072 = db;
var G__41073 = cljs.core.name(page_id_name_or_uuid);
return (logseq.db.get_first_page_by_name.cljs$core$IFn$_invoke$arity$2 ? logseq.db.get_first_page_by_name.cljs$core$IFn$_invoke$arity$2(G__41072,G__41073) : logseq.db.get_first_page_by_name.call(null,G__41072,G__41073));
})();
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(G__41070,G__41071) : datascript.core.entity.call(null,G__41070,G__41071));
}
}
} else {
return null;
}
});
/**
 * Get a journal page given its unsanitized name.
 * This will be useful for DB graphs later as we can switch to a different lookup
 * approach for journals e.g. like get-built-in-page
 */
logseq.db.get_journal_page = (function logseq$db$get_journal_page(db,page_name){
if(cljs.core.truth_(db)){
var G__41074 = db;
var G__41075 = (logseq.db.get_first_page_by_name.cljs$core$IFn$_invoke$arity$2 ? logseq.db.get_first_page_by_name.cljs$core$IFn$_invoke$arity$2(db,page_name) : logseq.db.get_first_page_by_name.call(null,db,page_name));
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(G__41074,G__41075) : datascript.core.entity.call(null,G__41074,G__41075));
} else {
return null;
}
});
logseq.db.get_built_in_page = logseq.db.frontend.db.get_built_in_page;
logseq.db.library_QMARK_ = logseq.db.frontend.db.library_QMARK_;
logseq.db.get_library_page = (function logseq$db$get_library_page(db){
return (logseq.db.get_built_in_page.cljs$core$IFn$_invoke$arity$2 ? logseq.db.get_built_in_page.cljs$core$IFn$_invoke$arity$2(db,logseq.common.config.library_page_name) : logseq.db.get_built_in_page.call(null,db,logseq.common.config.library_page_name));
});
/**
 * Case sensitive version of get-page. For use with DB graphs
 */
logseq.db.get_case_page = (function logseq$db$get_case_page(db,page_name_or_uuid){
if(cljs.core.truth_(db)){
var temp__5802__auto__ = ((cljs.core.uuid_QMARK_(page_name_or_uuid))?page_name_or_uuid:cljs.core.parse_uuid(page_name_or_uuid));
if(cljs.core.truth_(temp__5802__auto__)){
var id = temp__5802__auto__;
var G__41076 = db;
var G__41077 = new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("block","uuid","block/uuid",-1991494552),id], null);
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(G__41076,G__41077) : datascript.core.entity.call(null,G__41076,G__41077));
} else {
var G__41078 = db;
var G__41079 = logseq.db.common.initial_data.get_first_page_by_title(db,page_name_or_uuid);
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(G__41078,G__41079) : datascript.core.entity.call(null,G__41078,G__41079));
}
} else {
return null;
}
});
/**
 * Whether a page is empty. Does it has a non-page block?
 *   `page-id` could be either a string or a db/id.
 */
logseq.db.page_empty_QMARK_ = (function logseq$db$page_empty_QMARK_(db,page_id){
var page_id__$1 = ((typeof page_id === 'string')?(logseq.db.get_first_page_by_name.cljs$core$IFn$_invoke$arity$2 ? logseq.db.get_first_page_by_name.cljs$core$IFn$_invoke$arity$2(db,page_id) : logseq.db.get_first_page_by_name.call(null,db,page_id)):page_id);
var page = (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,page_id__$1) : datascript.core.entity.call(null,db,page_id__$1));
return cljs.core.empty_QMARK_(new cljs.core.Keyword("block","_parent","block/_parent",-639389670).cljs$core$IFn$_invoke$arity$1(page));
});
logseq.db.get_first_child = (function logseq$db$get_first_child(db,id){
return cljs.core.first(logseq.db.sort_by_order(new cljs.core.Keyword("block","_parent","block/_parent",-639389670).cljs$core$IFn$_invoke$arity$1((datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,id) : datascript.core.entity.call(null,db,id)))));
});
logseq.db.get_orphaned_pages = (function logseq$db$get_orphaned_pages(db,p__41080){
var map__41081 = p__41080;
var map__41081__$1 = cljs.core.__destructure_map(map__41081);
var pages = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__41081__$1,new cljs.core.Keyword(null,"pages","pages",-285406513));
var empty_ref_f = cljs.core.get.cljs$core$IFn$_invoke$arity$3(map__41081__$1,new cljs.core.Keyword(null,"empty-ref-f","empty-ref-f",666507359),(function (page){
return (cljs.core.count(new cljs.core.Keyword("block","_refs","block/_refs",830218531).cljs$core$IFn$_invoke$arity$1(page)) === (0));
}));
var built_in_pages_names = cljs.core.get.cljs$core$IFn$_invoke$arity$3(map__41081__$1,new cljs.core.Keyword(null,"built-in-pages-names","built-in-pages-names",-104089994),cljs.core.PersistentHashSet.EMPTY);
var pages__$1 = cljs.core.remove.cljs$core$IFn$_invoke$arity$2(cljs.core.nil_QMARK_,(function (){var or__5142__auto__ = pages;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return logseq.db.get_pages(db);
}
})());
var built_in_pages = cljs.core.set(cljs.core.map.cljs$core$IFn$_invoke$arity$2(clojure.string.lower_case,built_in_pages_names));
var orphaned_pages = cljs.core.remove.cljs$core$IFn$_invoke$arity$2(logseq.db.hidden_QMARK_,cljs.core.remove.cljs$core$IFn$_invoke$arity$2(cljs.core.nil_QMARK_,cljs.core.remove.cljs$core$IFn$_invoke$arity$2(cljs.core.false_QMARK_,cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (page){
var temp__5804__auto__ = logseq.db.get_page(db,page);
if(cljs.core.truth_(temp__5804__auto__)){
var page__$1 = temp__5804__auto__;
var name_SINGLEQUOTE_ = new cljs.core.Keyword("block","name","block/name",1619760316).cljs$core$IFn$_invoke$arity$1(page__$1);
var and__5140__auto__ = (empty_ref_f.cljs$core$IFn$_invoke$arity$1 ? empty_ref_f.cljs$core$IFn$_invoke$arity$1(page__$1) : empty_ref_f.call(null,page__$1));
if(cljs.core.truth_(and__5140__auto__)){
var and__5140__auto____$1 = (function (){var or__5142__auto__ = logseq.db.page_empty_QMARK_(db,new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(page__$1));
if(or__5142__auto__){
return or__5142__auto__;
} else {
var first_child = logseq.db.get_first_child(db,new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(page__$1));
var children = new cljs.core.Keyword("block","_page","block/_page",1150043350).cljs$core$IFn$_invoke$arity$1(page__$1);
var and__5140__auto____$1 = first_child;
if(cljs.core.truth_(and__5140__auto____$1)){
return ((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2((1),cljs.core.count(children))) && (cljs.core.contains_QMARK_(new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 3, ["",null,"*",null,"-",null], null), null),clojure.string.trim(new cljs.core.Keyword("block","title","block/title",710445684).cljs$core$IFn$_invoke$arity$1(first_child)))));
} else {
return and__5140__auto____$1;
}
}
})();
if(cljs.core.truth_(and__5140__auto____$1)){
var and__5140__auto____$2 = (!(cljs.core.contains_QMARK_(built_in_pages,name_SINGLEQUOTE_)));
if(and__5140__auto____$2){
var and__5140__auto____$3 = cljs.core.not((logseq.db.property_QMARK_.cljs$core$IFn$_invoke$arity$1 ? logseq.db.property_QMARK_.cljs$core$IFn$_invoke$arity$1(page__$1) : logseq.db.property_QMARK_.call(null,page__$1)));
if(and__5140__auto____$3){
var and__5140__auto____$4 = (!(((clojure.string.includes_QMARK_(name_SINGLEQUOTE_,"/")) && (cljs.core.not((logseq.db.journal_QMARK_.cljs$core$IFn$_invoke$arity$1 ? logseq.db.journal_QMARK_.cljs$core$IFn$_invoke$arity$1(page__$1) : logseq.db.journal_QMARK_.call(null,page__$1)))))));
if(and__5140__auto____$4){
var and__5140__auto____$5 = cljs.core.not(new cljs.core.Keyword("block","properties","block/properties",708347145).cljs$core$IFn$_invoke$arity$1(page__$1));
if(and__5140__auto____$5){
return page__$1;
} else {
return and__5140__auto____$5;
}
} else {
return and__5140__auto____$4;
}
} else {
return and__5140__auto____$3;
}
} else {
return and__5140__auto____$2;
}
} else {
return and__5140__auto____$1;
}
} else {
return and__5140__auto__;
}
} else {
return null;
}
}),pages__$1))));
return orphaned_pages;
});
logseq.db.has_children_QMARK_ = (function logseq$db$has_children_QMARK_(db,block_id){
var eid = ((cljs.core.uuid_QMARK_(block_id))?new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("block","uuid","block/uuid",-1991494552),block_id], null):block_id);
return (!((new cljs.core.Keyword("block","_parent","block/_parent",-639389670).cljs$core$IFn$_invoke$arity$1((datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,eid) : datascript.core.entity.call(null,db,eid))) == null)));
});
logseq.db.collapsed_and_has_children_QMARK_ = (function logseq$db$collapsed_and_has_children_QMARK_(db,block){
var and__5140__auto__ = new cljs.core.Keyword("block","collapsed?","block/collapsed?",2140210991).cljs$core$IFn$_invoke$arity$1(block);
if(cljs.core.truth_(and__5140__auto__)){
return logseq.db.has_children_QMARK_(db,new cljs.core.Keyword("block","uuid","block/uuid",-1991494552).cljs$core$IFn$_invoke$arity$1(block));
} else {
return and__5140__auto__;
}
});
/**
 * Notice: if `not-collapsed?` is true, will skip searching for any collapsed block.
 */
logseq.db.get_block_last_direct_child_id = (function logseq$db$get_block_last_direct_child_id(var_args){
var G__41083 = arguments.length;
switch (G__41083) {
case 2:
return logseq.db.get_block_last_direct_child_id.cljs$core$IFn$_invoke$arity$2((arguments[(0)]),(arguments[(1)]));

break;
case 3:
return logseq.db.get_block_last_direct_child_id.cljs$core$IFn$_invoke$arity$3((arguments[(0)]),(arguments[(1)]),(arguments[(2)]));

break;
default:
throw (new Error(["Invalid arity: ",arguments.length].join("")));

}
});

(logseq.db.get_block_last_direct_child_id.cljs$core$IFn$_invoke$arity$2 = (function (db,db_id){
return logseq.db.get_block_last_direct_child_id.cljs$core$IFn$_invoke$arity$3(db,db_id,false);
}));

(logseq.db.get_block_last_direct_child_id.cljs$core$IFn$_invoke$arity$3 = (function (db,db_id,not_collapsed_QMARK_){
var temp__5804__auto__ = (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,db_id) : datascript.core.entity.call(null,db,db_id));
if(cljs.core.truth_(temp__5804__auto__)){
var block = temp__5804__auto__;
if((cljs.core.truth_(not_collapsed_QMARK_)?cljs.core.not(logseq.db.collapsed_and_has_children_QMARK_(db,block)):true)){
var children = cljs.core.sort_by.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("block","order","block/order",-1429282437),new cljs.core.Keyword("block","_parent","block/_parent",-639389670).cljs$core$IFn$_invoke$arity$1(block));
return new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(cljs.core.last(children));
} else {
return null;
}
} else {
return null;
}
}));

(logseq.db.get_block_last_direct_child_id.cljs$lang$maxFixedArity = 3);

/**
 * Doesn't include nested children.
 */
logseq.db.get_children = (function logseq$db$get_children(var_args){
var G__41085 = arguments.length;
switch (G__41085) {
case 1:
return logseq.db.get_children.cljs$core$IFn$_invoke$arity$1((arguments[(0)]));

break;
case 2:
return logseq.db.get_children.cljs$core$IFn$_invoke$arity$2((arguments[(0)]),(arguments[(1)]));

break;
default:
throw (new Error(["Invalid arity: ",arguments.length].join("")));

}
});

(logseq.db.get_children.cljs$core$IFn$_invoke$arity$1 = (function (block_entity){
return logseq.db.get_children.cljs$core$IFn$_invoke$arity$2(null,block_entity);
}));

(logseq.db.get_children.cljs$core$IFn$_invoke$arity$2 = (function (db,block_entity_or_eid){
var temp__5804__auto__ = ((typeof block_entity_or_eid === 'number')?(datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,block_entity_or_eid) : datascript.core.entity.call(null,db,block_entity_or_eid)):((cljs.core.uuid_QMARK_(block_entity_or_eid))?(function (){var G__41086 = db;
var G__41087 = new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("block","uuid","block/uuid",-1991494552),block_entity_or_eid], null);
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(G__41086,G__41087) : datascript.core.entity.call(null,G__41086,G__41087));
})():block_entity_or_eid
));
if(cljs.core.truth_(temp__5804__auto__)){
var parent = temp__5804__auto__;
return logseq.db.sort_by_order(new cljs.core.Keyword("block","_parent","block/_parent",-639389670).cljs$core$IFn$_invoke$arity$1(parent));
} else {
return null;
}
}));

(logseq.db.get_children.cljs$lang$maxFixedArity = 2);

logseq.db.get_block_parents = (function logseq$db$get_block_parents(var_args){
var args__5882__auto__ = [];
var len__5876__auto___41141 = arguments.length;
var i__5877__auto___41142 = (0);
while(true){
if((i__5877__auto___41142 < len__5876__auto___41141)){
args__5882__auto__.push((arguments[i__5877__auto___41142]));

var G__41143 = (i__5877__auto___41142 + (1));
i__5877__auto___41142 = G__41143;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((2) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((2)),(0),null)):null);
return logseq.db.get_block_parents.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),(arguments[(1)]),argseq__5883__auto__);
});

(logseq.db.get_block_parents.cljs$core$IFn$_invoke$arity$variadic = (function (db,block_id,p__41091){
var map__41092 = p__41091;
var map__41092__$1 = cljs.core.__destructure_map(map__41092);
var depth = cljs.core.get.cljs$core$IFn$_invoke$arity$3(map__41092__$1,new cljs.core.Keyword(null,"depth","depth",1768663640),(100));
var block_id__$1 = block_id;
var parents_SINGLEQUOTE_ = cljs.core.List.EMPTY;
var d = (1);
while(true){
if((d > depth)){
return parents_SINGLEQUOTE_;
} else {
var temp__5802__auto__ = new cljs.core.Keyword("block","parent","block/parent",-918309064).cljs$core$IFn$_invoke$arity$1((function (){var G__41093 = db;
var G__41094 = new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("block","uuid","block/uuid",-1991494552),block_id__$1], null);
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(G__41093,G__41094) : datascript.core.entity.call(null,G__41093,G__41094));
})());
if(cljs.core.truth_(temp__5802__auto__)){
var parent = temp__5802__auto__;
var G__41144 = new cljs.core.Keyword("block","uuid","block/uuid",-1991494552).cljs$core$IFn$_invoke$arity$1(parent);
var G__41145 = cljs.core.conj.cljs$core$IFn$_invoke$arity$2(parents_SINGLEQUOTE_,parent);
var G__41146 = (d + (1));
block_id__$1 = G__41144;
parents_SINGLEQUOTE_ = G__41145;
d = G__41146;
continue;
} else {
return parents_SINGLEQUOTE_;
}
}
break;
}
}));

(logseq.db.get_block_parents.cljs$lang$maxFixedArity = (2));

/** @this {Function} */
(logseq.db.get_block_parents.cljs$lang$applyTo = (function (seq41088){
var G__41089 = cljs.core.first(seq41088);
var seq41088__$1 = cljs.core.next(seq41088);
var G__41090 = cljs.core.first(seq41088__$1);
var seq41088__$2 = cljs.core.next(seq41088__$1);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__41089,G__41090,seq41088__$2);
}));

logseq.db.get_block_children_ids = logseq.db.common.initial_data.get_block_children_ids;
logseq.db.get_block_full_children_ids = logseq.db.common.initial_data.get_block_full_children_ids;
logseq.db.get_sorted_page_block_ids = (function logseq$db$get_sorted_page_block_ids(db,page_id){
var root = (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,page_id) : datascript.core.entity.call(null,db,page_id));
var result = cljs.core.PersistentVector.EMPTY;
var children = logseq.db.sort_by_order(new cljs.core.Keyword("block","_parent","block/_parent",-639389670).cljs$core$IFn$_invoke$arity$1(root));
while(true){
if(cljs.core.seq(children)){
var child = cljs.core.first(children);
var G__41147 = cljs.core.conj.cljs$core$IFn$_invoke$arity$2(result,new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(child));
var G__41148 = cljs.core.concat.cljs$core$IFn$_invoke$arity$2(logseq.db.sort_by_order(new cljs.core.Keyword("block","_parent","block/_parent",-639389670).cljs$core$IFn$_invoke$arity$1(child)),cljs.core.rest(children));
result = G__41147;
children = G__41148;
continue;
} else {
return result;
}
break;
}
});
/**
 * Blocks could be non consecutive.
 */
logseq.db.sort_page_random_blocks = (function logseq$db$sort_page_random_blocks(db,blocks){
if(cljs.core.every_QMARK_((function (p1__41095_SHARP_){
return cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("block","page","block/page",822314108).cljs$core$IFn$_invoke$arity$1(p1__41095_SHARP_),new cljs.core.Keyword("block","page","block/page",822314108).cljs$core$IFn$_invoke$arity$1(cljs.core.first(blocks)));
}),blocks)){
} else {
throw (new Error((""+"Assert failed: "+"Blocks must to be in a same page."+"\n"+"(every? (fn* [p1__41095#] (= (:block/page p1__41095#) (:block/page (first blocks)))) blocks)")));
}

var page_id = new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(new cljs.core.Keyword("block","page","block/page",822314108).cljs$core$IFn$_invoke$arity$1(cljs.core.first(blocks)));
var sorted_ids = logseq.db.get_sorted_page_block_ids(db,page_id);
var blocks_map = cljs.core.zipmap(cljs.core.map.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db","id","db/id",-1388397098),blocks),blocks);
return cljs.core.keep.cljs$core$IFn$_invoke$arity$2(blocks_map,sorted_ids);
});
/**
 * The child block could be collapsed.
 */
logseq.db.last_child_block_QMARK_ = (function logseq$db$last_child_block_QMARK_(db,parent_id,child_id){
var temp__5804__auto__ = (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,child_id) : datascript.core.entity.call(null,db,child_id));
if(cljs.core.truth_(temp__5804__auto__)){
var child = temp__5804__auto__;
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(parent_id,child_id)){
return true;
} else {
if(cljs.core.truth_(logseq.db.get_right_sibling(child))){
return false;
} else {
var G__41096 = db;
var G__41097 = parent_id;
var G__41098 = new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(new cljs.core.Keyword("block","parent","block/parent",-918309064).cljs$core$IFn$_invoke$arity$1(child));
return (logseq.db.last_child_block_QMARK_.cljs$core$IFn$_invoke$arity$3 ? logseq.db.last_child_block_QMARK_.cljs$core$IFn$_invoke$arity$3(G__41096,G__41097,G__41098) : logseq.db.last_child_block_QMARK_.call(null,G__41096,G__41097,G__41098));

}
}
} else {
return null;
}
});
logseq.db.consecutive_block_QMARK_ = (function logseq$db$consecutive_block_QMARK_(db,block_1,block_2){
var aux_fn = (function (block_1__$1,block_2__$1){
var and__5140__auto__ = cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("block","page","block/page",822314108).cljs$core$IFn$_invoke$arity$1(block_1__$1),new cljs.core.Keyword("block","page","block/page",822314108).cljs$core$IFn$_invoke$arity$1(block_2__$1));
if(and__5140__auto__){
var or__5142__auto__ = cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(logseq.db.get_left_sibling(block_2__$1)),new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(block_1__$1));
if(or__5142__auto__){
return or__5142__auto__;
} else {
var temp__5804__auto__ = logseq.db.get_left_sibling((function (){var G__41099 = db;
var G__41100 = new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(block_2__$1);
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(G__41099,G__41100) : datascript.core.entity.call(null,G__41099,G__41100));
})());
if(cljs.core.truth_(temp__5804__auto__)){
var prev_sibling = temp__5804__auto__;
return logseq.db.last_child_block_QMARK_(db,new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(prev_sibling),new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(block_1__$1));
} else {
return null;
}
}
} else {
return and__5140__auto__;
}
});
var or__5142__auto__ = aux_fn(block_1,block_2);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return aux_fn(block_2,block_1);
}
});
logseq.db.get_non_consecutive_blocks = (function logseq$db$get_non_consecutive_blocks(db,blocks){
return cljs.core.vec(cljs.core.keep_indexed.cljs$core$IFn$_invoke$arity$2((function (i,_block){
if(((i + (1)) < cljs.core.count(blocks))){
if(cljs.core.truth_(logseq.db.consecutive_block_QMARK_(db,cljs.core.nth.cljs$core$IFn$_invoke$arity$2(blocks,i),cljs.core.nth.cljs$core$IFn$_invoke$arity$2(blocks,(i + (1)))))){
return null;
} else {
return cljs.core.nth.cljs$core$IFn$_invoke$arity$2(blocks,i);
}
} else {
return null;
}
}),blocks));
});
logseq.db.new_block_id = (function logseq$db$new_block_id(){
return logseq.common.uuid.gen_uuid.cljs$core$IFn$_invoke$arity$0();
});
/**
 * return the source page (page-name) of an alias
 */
logseq.db.get_alias_source_page = (function logseq$db$get_alias_source_page(db,alias_id){
if(cljs.core.truth_(alias_id)){
return cljs.core.first(new cljs.core.Keyword("block","_alias","block/_alias",444442061).cljs$core$IFn$_invoke$arity$1((datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,alias_id) : datascript.core.entity.call(null,db,alias_id))));
} else {
return null;
}
});
logseq.db.get_block_alias = logseq.db.common.initial_data.get_block_alias;
logseq.db.page_alias_set = (function logseq$db$page_alias_set(db,page_id){
return clojure.set.union.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentHashSet.createAsIfByAssoc([page_id]),cljs.core.set((logseq.db.get_block_alias.cljs$core$IFn$_invoke$arity$2 ? logseq.db.get_block_alias.cljs$core$IFn$_invoke$arity$2(db,page_id) : logseq.db.get_block_alias.call(null,db,page_id))));
});
logseq.db.get_block_refs_count = logseq.db.common.initial_data.get_block_refs_count;
logseq.db.hidden_or_internal_tag_QMARK_ = (function logseq$db$hidden_or_internal_tag_QMARK_(e){
var or__5142__auto__ = logseq.db.frontend.entity_util.hidden_QMARK_(e);
if(or__5142__auto__){
return or__5142__auto__;
} else {
var G__41101 = new cljs.core.Keyword("db","ident","db/ident",-737096).cljs$core$IFn$_invoke$arity$1(e);
return (logseq.db.frontend.class$.internal_tags.cljs$core$IFn$_invoke$arity$1 ? logseq.db.frontend.class$.internal_tags.cljs$core$IFn$_invoke$arity$1(G__41101) : logseq.db.frontend.class$.internal_tags.call(null,G__41101));
}
});
logseq.db.get_all_pages = (function logseq$db$get_all_pages(db){
return cljs.core.keep.cljs$core$IFn$_invoke$arity$2((function (d){
var e = (function (){var G__41102 = db;
var G__41103 = new cljs.core.Keyword(null,"e","e",1381269198).cljs$core$IFn$_invoke$arity$1(d);
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(G__41102,G__41103) : datascript.core.entity.call(null,G__41102,G__41103));
})();
if(cljs.core.truth_(logseq.db.hidden_or_internal_tag_QMARK_(e))){
return null;
} else {
return e;
}
}),datascript.core.datoms.cljs$core$IFn$_invoke$arity$3(db,new cljs.core.Keyword(null,"avet","avet",1383857032),new cljs.core.Keyword("block","name","block/name",1619760316)));
});
logseq.db.get_key_value = (function logseq$db$get_key_value(db,key_ident){
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2("logseq.kv",cljs.core.namespace(key_ident))){
} else {
throw (new Error((""+"Assert failed: "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(key_ident)+"\n"+"(= \"logseq.kv\" (namespace key-ident))")));
}

return new cljs.core.Keyword("kv","value","kv/value",305981670).cljs$core$IFn$_invoke$arity$1((datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,key_ident) : datascript.core.entity.call(null,db,key_ident)));
});
logseq.db.kv = logseq.db.sqlite.util.kv;
logseq.db.get_graph_rtc_uuid = (function logseq$db$get_graph_rtc_uuid(db){
if(cljs.core.truth_(db)){
return logseq.db.get_key_value(db,new cljs.core.Keyword("logseq.kv","graph-uuid","logseq.kv/graph-uuid",339522676));
} else {
return null;
}
});
logseq.db.get_graph_local_uuid = (function logseq$db$get_graph_local_uuid(db){
if(cljs.core.truth_(db)){
return logseq.db.get_key_value(db,new cljs.core.Keyword("logseq.kv","local-graph-uuid","logseq.kv/local-graph-uuid",1789980105));
} else {
return null;
}
});
logseq.db.get_graph_schema_version = (function logseq$db$get_graph_schema_version(db){
if(cljs.core.truth_(db)){
return logseq.db.get_key_value(db,new cljs.core.Keyword("logseq.kv","schema-version","logseq.kv/schema-version",-1467606676));
} else {
return null;
}
});
logseq.db.get_graph_remote_schema_version = (function logseq$db$get_graph_remote_schema_version(db){
if(cljs.core.truth_(db)){
return logseq.db.get_key_value(db,new cljs.core.Keyword("logseq.kv","remote-schema-version","logseq.kv/remote-schema-version",988970829));
} else {
return null;
}
});
logseq.db.get_graph_rtc_e2ee_QMARK_ = (function logseq$db$get_graph_rtc_e2ee_QMARK_(db){
if(cljs.core.truth_(db)){
return logseq.db.get_key_value(db,new cljs.core.Keyword("logseq.kv","graph-rtc-e2ee?","logseq.kv/graph-rtc-e2ee?",1988827753));
} else {
return null;
}
});
logseq.db.get_all_properties = logseq.db.frontend.db.get_all_properties;
logseq.db.get_class_extends = logseq.db.frontend.class$.get_class_extends;
logseq.db.get_classes_parents = logseq.db.frontend.db.get_classes_parents;
logseq.db.get_page_parents = logseq.db.frontend.db.get_page_parents;
logseq.db.get_title_with_parents = logseq.db.frontend.db.get_title_with_parents;
logseq.db.class_instance_QMARK_ = logseq.db.frontend.db.class_instance_QMARK_;
logseq.db.inline_tag_QMARK_ = logseq.db.frontend.db.inline_tag_QMARK_;
logseq.db.node_display_type_classes = logseq.db.frontend.db.node_display_type_classes;
logseq.db.get_class_ident_by_display_type = logseq.db.frontend.db.get_class_ident_by_display_type;
logseq.db.get_display_type_by_class_ident = logseq.db.frontend.db.get_display_type_by_class_ident;
logseq.db.get_recent_updated_pages = logseq.db.common.initial_data.get_recent_updated_pages;
logseq.db.get_latest_journals = logseq.db.common.initial_data.get_latest_journals;
logseq.db.get_pages_relation = (function logseq$db$get_pages_relation(db,with_journal_QMARK_){
var q = (cljs.core.truth_(with_journal_QMARK_)?new cljs.core.PersistentVector(null, 6, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"find","find",496279456),new cljs.core.Symbol(null,"?p","?p",-10896580,null),new cljs.core.Symbol(null,"?ref-page","?ref-page",-893277661,null),new cljs.core.Keyword(null,"where","where",-2044795965),new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?block","?block",1541466123,null),new cljs.core.Keyword("block","page","block/page",822314108),new cljs.core.Symbol(null,"?p","?p",-10896580,null)], null),new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?block","?block",1541466123,null),new cljs.core.Keyword("block","refs","block/refs",-1214495349),new cljs.core.Symbol(null,"?ref-page","?ref-page",-893277661,null)], null)], null):new cljs.core.PersistentVector(null, 8, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"find","find",496279456),new cljs.core.Symbol(null,"?p","?p",-10896580,null),new cljs.core.Symbol(null,"?ref-page","?ref-page",-893277661,null),new cljs.core.Keyword(null,"where","where",-2044795965),new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?block","?block",1541466123,null),new cljs.core.Keyword("block","page","block/page",822314108),new cljs.core.Symbol(null,"?p","?p",-10896580,null)], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?p","?p",-10896580,null),new cljs.core.Keyword("block","tags","block/tags",1814948340)], null),cljs.core.list(new cljs.core.Symbol(null,"not","not",1044554643,null),new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?p","?p",-10896580,null),new cljs.core.Keyword("block","tags","block/tags",1814948340),new cljs.core.Keyword("logseq.class","Journal","logseq.class/Journal",1979741081)], null)),new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?block","?block",1541466123,null),new cljs.core.Keyword("block","refs","block/refs",-1214495349),new cljs.core.Symbol(null,"?ref-page","?ref-page",-893277661,null)], null)], null));
return (datascript.core.q.cljs$core$IFn$_invoke$arity$2 ? datascript.core.q.cljs$core$IFn$_invoke$arity$2(q,db) : datascript.core.q.call(null,q,db));
});
logseq.db.get_all_tagged_pages = (function logseq$db$get_all_tagged_pages(db){
var G__41104 = new cljs.core.PersistentVector(null, 5, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"find","find",496279456),new cljs.core.Symbol(null,"?page","?page",-1343187612,null),new cljs.core.Symbol(null,"?tag","?tag",157764474,null),new cljs.core.Keyword(null,"where","where",-2044795965),new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?page","?page",-1343187612,null),new cljs.core.Keyword("block","tags","block/tags",1814948340),new cljs.core.Symbol(null,"?tag","?tag",157764474,null)], null)], null);
var G__41105 = db;
return (datascript.core.q.cljs$core$IFn$_invoke$arity$2 ? datascript.core.q.cljs$core$IFn$_invoke$arity$2(G__41104,G__41105) : datascript.core.q.call(null,G__41104,G__41105));
});
/**
 * Check whether a `page` exists on the Library page
 */
logseq.db.page_in_library_QMARK_ = (function logseq$db$page_in_library_QMARK_(db,page){
if(cljs.core.truth_((logseq.db.page_QMARK_.cljs$core$IFn$_invoke$arity$1 ? logseq.db.page_QMARK_.cljs$core$IFn$_invoke$arity$1(page) : logseq.db.page_QMARK_.call(null,page)))){
var temp__5804__auto__ = (logseq.db.get_built_in_page.cljs$core$IFn$_invoke$arity$2 ? logseq.db.get_built_in_page.cljs$core$IFn$_invoke$arity$2(db,logseq.common.config.library_page_name) : logseq.db.get_built_in_page.call(null,db,logseq.common.config.library_page_name));
if(cljs.core.truth_(temp__5804__auto__)){
var library_page = temp__5804__auto__;
var parent = new cljs.core.Keyword("block","parent","block/parent",-918309064).cljs$core$IFn$_invoke$arity$1(page);
while(true){
if((parent == null)){
return false;
} else {
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(parent),new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(library_page))){
return true;
} else {
var G__41153 = new cljs.core.Keyword("block","parent","block/parent",-918309064).cljs$core$IFn$_invoke$arity$1(parent);
parent = G__41153;
continue;

}
}
break;
}
} else {
return null;
}
} else {
return null;
}
});
logseq.db.get_class_title_with_extends = logseq.db.frontend.db.get_class_title_with_extends;
logseq.db.bidirectional_property_attr_QMARK_ = (function logseq$db$bidirectional_property_attr_QMARK_(db,attr){
if(cljs.core.qualified_keyword_QMARK_(attr)){
var attr_ns = cljs.core.namespace(attr);
var and__5140__auto__ = ((logseq.db.frontend.property.user_property_namespace_QMARK_(attr_ns)) || (logseq.db.frontend.property.plugin_property_QMARK_(attr)));
if(and__5140__auto__){
var temp__5804__auto__ = (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,attr) : datascript.core.entity.call(null,db,attr));
if(cljs.core.truth_(temp__5804__auto__)){
var property = temp__5804__auto__;
return cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db.type","ref","db.type/ref",-1728373079),new cljs.core.Keyword("db","valueType","db/valueType",1827971944).cljs$core$IFn$_invoke$arity$1(property));
} else {
return null;
}
} else {
return and__5140__auto__;
}
} else {
return null;
}
});
logseq.db.get_ea_by_v = (function logseq$db$get_ea_by_v(db,v){
var G__41106 = new cljs.core.PersistentVector(null, 10, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"find","find",496279456),new cljs.core.Symbol(null,"?e","?e",-1194391683,null),new cljs.core.Symbol(null,"?a","?a",1314302913,null),new cljs.core.Keyword(null,"in","in",-1531184865),new cljs.core.Symbol(null,"$","$",-1580747756,null),new cljs.core.Symbol(null,"?v","?v",-464183118,null),new cljs.core.Keyword(null,"where","where",-2044795965),new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?e","?e",-1194391683,null),new cljs.core.Symbol(null,"?a","?a",1314302913,null),new cljs.core.Symbol(null,"?v","?v",-464183118,null)], null),new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?ea","?ea",723308761,null),new cljs.core.Keyword("db","ident","db/ident",-737096),new cljs.core.Symbol(null,"?a","?a",1314302913,null)], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?ea","?ea",723308761,null),new cljs.core.Keyword("logseq.property","classes","logseq.property/classes",913750486)], null)], null);
var G__41107 = db;
var G__41108 = v;
return (datascript.core.q.cljs$core$IFn$_invoke$arity$3 ? datascript.core.q.cljs$core$IFn$_invoke$arity$3(G__41106,G__41107,G__41108) : datascript.core.q.call(null,G__41106,G__41107,G__41108));
});
/**
 * Given a target entity id, returns a seq of maps with:
 * * :class - class entity
 * * :title - pluralized class title
 * * :entities - node entities that reference the target via ref properties
 */
logseq.db.get_bidirectional_properties = (function logseq$db$get_bidirectional_properties(db,target_id){
if(cljs.core.truth_((function (){var and__5140__auto__ = db;
if(cljs.core.truth_(and__5140__auto__)){
var and__5140__auto____$1 = target_id;
if(cljs.core.truth_(and__5140__auto____$1)){
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,target_id) : datascript.core.entity.call(null,db,target_id));
} else {
return and__5140__auto____$1;
}
} else {
return and__5140__auto__;
}
})())){
var add_entity = (function (acc,class_id,entity){
if(cljs.core.truth_(class_id)){
return cljs.core.update.cljs$core$IFn$_invoke$arity$4(acc,class_id,cljs.core.fnil.cljs$core$IFn$_invoke$arity$2(cljs.core.conj,cljs.core.PersistentHashSet.EMPTY),entity);
} else {
return acc;
}
});
return cljs.core.sort_by.cljs$core$IFn$_invoke$arity$2(cljs.core.comp.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("block","created-at","block/created-at",1440015),new cljs.core.Keyword(null,"class","class",-2030961996)),cljs.core.keep.cljs$core$IFn$_invoke$arity$2((function (p__41109){
var vec__41110 = p__41109;
var class_id = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__41110,(0),null);
var entities = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__41110,(1),null);
var class$ = (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,class_id) : datascript.core.entity.call(null,db,class_id));
if(new cljs.core.Keyword("logseq.property.class","enable-bidirectional?","logseq.property.class/enable-bidirectional?",-291786138).cljs$core$IFn$_invoke$arity$1(class$) === true){
var custom_title = (function (){var temp__5804__auto__ = new cljs.core.Keyword("logseq.property.class","bidirectional-property-title","logseq.property.class/bidirectional-property-title",-605054187).cljs$core$IFn$_invoke$arity$1(class$);
if(cljs.core.truth_(temp__5804__auto__)){
var custom = temp__5804__auto__;
if(typeof custom === 'string'){
return custom;
} else {
return logseq.db.frontend.property.property_value_content(custom);
}
} else {
return null;
}
})();
var title = ((clojure.string.blank_QMARK_(custom_title))?logseq.common.plural.plural(new cljs.core.Keyword("block","title","block/title",710445684).cljs$core$IFn$_invoke$arity$1(class$)):custom_title);
return new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"title","title",636505583),title,new cljs.core.Keyword(null,"class","class",-2030961996),cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,class$),new cljs.core.Keyword("db","id","db/id",-1388397098),new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(class$)),new cljs.core.Keyword(null,"entities","entities",1940967403),cljs.core.sort_by.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("block","created-at","block/created-at",1440015),entities)], null);
} else {
return null;
}
}),cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (acc,p__41113){
var vec__41114 = p__41113;
var class_ent = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__41114,(0),null);
var entity = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__41114,(1),null);
return add_entity(acc,class_ent,entity);
}),cljs.core.PersistentArrayMap.EMPTY,cljs.core.mapcat.cljs$core$IFn$_invoke$arity$variadic(cljs.core.identity,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([cljs.core.keep.cljs$core$IFn$_invoke$arity$2((function (p__41117){
var vec__41118 = p__41117;
var e = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__41118,(0),null);
var a = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__41118,(1),null);
if(cljs.core.truth_(logseq.db.bidirectional_property_attr_QMARK_(db,a))){
var temp__5804__auto__ = (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,e) : datascript.core.entity.call(null,db,e));
if(cljs.core.truth_(temp__5804__auto__)){
var entity = temp__5804__auto__;
if(((cljs.core.not_EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(entity),target_id)) && (((cljs.core.not(logseq.db.frontend.entity_util.class_QMARK_(entity))) && (cljs.core.not(logseq.db.frontend.entity_util.property_QMARK_(entity))))))){
var classes = cljs.core.filter.cljs$core$IFn$_invoke$arity$2(logseq.db.frontend.entity_util.class_QMARK_,new cljs.core.Keyword("block","tags","block/tags",1814948340).cljs$core$IFn$_invoke$arity$1(entity));
if(cljs.core.seq(classes)){
return cljs.core.keep.cljs$core$IFn$_invoke$arity$2((function (class_ent){
if(cljs.core.truth_((logseq.db.built_in_QMARK_.cljs$core$IFn$_invoke$arity$1 ? logseq.db.built_in_QMARK_.cljs$core$IFn$_invoke$arity$1(class_ent) : logseq.db.built_in_QMARK_.call(null,class_ent)))){
return null;
} else {
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(class_ent),entity], null);
}
}),classes);
} else {
return null;
}
} else {
return null;
}
} else {
return null;
}
} else {
return null;
}
}),logseq.db.get_ea_by_v(db,target_id))], 0)))));
} else {
return null;
}
});

//# sourceMappingURL=logseq.db.js.map
