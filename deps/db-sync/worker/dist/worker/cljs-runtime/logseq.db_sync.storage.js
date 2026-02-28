import "./cljs_env.js";
import "./cljs.core.js";
import "./cljs_bean.core.js";
import "./clojure.string.js";
import "./datascript.core.js";
import "./datascript.storage.js";
import "./logseq.db_sync.common.js";
import "./logseq.db.common.normalize.js";
import "./logseq.db.common.sqlite.js";
import "./logseq.db.frontend.schema.js";
goog.provide('logseq.db_sync.storage');
logseq.db_sync.storage.init_schema_BANG_ = (function logseq$db_sync$storage$init_schema_BANG_(sql){
logseq.db_sync.common.sql_exec(sql,"create table if not exists kvs (addr INTEGER primary key, content TEXT, addresses JSON)");

logseq.db_sync.common.sql_exec(sql,(""+"create table if not exists tx_log ("+"t INTEGER primary key,"+"tx TEXT not null,"+"created_at INTEGER"+");"));

return logseq.db_sync.common.sql_exec(sql,(""+"create table if not exists sync_meta ("+"key TEXT primary key,"+"value TEXT"+");"));
});
logseq.db_sync.storage.select_one = (function logseq$db_sync$storage$select_one(var_args){
var args__5882__auto__ = [];
var len__5876__auto___40907 = arguments.length;
var i__5877__auto___40908 = (0);
while(true){
if((i__5877__auto___40908 < len__5876__auto___40907)){
args__5882__auto__.push((arguments[i__5877__auto___40908]));

var G__40910 = (i__5877__auto___40908 + (1));
i__5877__auto___40908 = G__40910;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((2) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((2)),(0),null)):null);
return logseq.db_sync.storage.select_one.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),(arguments[(1)]),argseq__5883__auto__);
});

(logseq.db_sync.storage.select_one.cljs$core$IFn$_invoke$arity$variadic = (function (sql,sql_str,args){
return cljs.core.first(logseq.db_sync.common.get_sql_rows(cljs.core.apply.cljs$core$IFn$_invoke$arity$4(logseq.db_sync.common.sql_exec,sql,sql_str,args)));
}));

(logseq.db_sync.storage.select_one.cljs$lang$maxFixedArity = (2));

/** @this {Function} */
(logseq.db_sync.storage.select_one.cljs$lang$applyTo = (function (seq40754){
var G__40756 = cljs.core.first(seq40754);
var seq40754__$1 = cljs.core.next(seq40754);
var G__40757 = cljs.core.first(seq40754__$1);
var seq40754__$2 = cljs.core.next(seq40754__$1);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__40756,G__40757,seq40754__$2);
}));

logseq.db_sync.storage.get_meta = (function logseq$db_sync$storage$get_meta(sql,k){
var temp__5804__auto__ = logseq.db_sync.storage.select_one.cljs$core$IFn$_invoke$arity$variadic(sql,"select value from sync_meta where key = ?",cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([cljs.core.name(k)], 0));
if(cljs.core.truth_(temp__5804__auto__)){
var row = temp__5804__auto__;
return (row["value"]);
} else {
return null;
}
});
logseq.db_sync.storage.set_meta_BANG_ = (function logseq$db_sync$storage$set_meta_BANG_(sql,k,v){
return logseq.db_sync.common.sql_exec.cljs$core$IFn$_invoke$arity$variadic(sql,(""+"insert into sync_meta (key, value) values (?, ?)"+" on conflict(key) do update set value = excluded.value"),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([cljs.core.name(k),(""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(v))], 0));
});
logseq.db_sync.storage.get_t = (function logseq$db_sync$storage$get_t(sql){
var value = logseq.db_sync.storage.get_meta(sql,new cljs.core.Keyword(null,"t","t",-1397832519));
if(typeof value === 'string'){
return parseInt(value,(10));
} else {
return (0);
}
});
logseq.db_sync.storage.set_t_BANG_ = (function logseq$db_sync$storage$set_t_BANG_(sql,t){
return logseq.db_sync.storage.set_meta_BANG_(sql,new cljs.core.Keyword(null,"t","t",-1397832519),t);
});
logseq.db_sync.storage.next_t_BANG_ = (function logseq$db_sync$storage$next_t_BANG_(sql){
var t = (logseq.db_sync.storage.get_t(sql) + (1));
logseq.db_sync.storage.set_t_BANG_(sql,t);

return t;
});
logseq.db_sync.storage.append_tx_BANG_ = (function logseq$db_sync$storage$append_tx_BANG_(sql,t,tx_str,created_at){
return logseq.db_sync.common.sql_exec.cljs$core$IFn$_invoke$arity$variadic(sql,(""+"insert into tx_log (t, tx, created_at) values (?, ?, ?)"+" on conflict(t) do update set tx = excluded.tx, created_at = excluded.created_at"),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([t,tx_str,created_at], 0));
});
logseq.db_sync.storage.fetch_tx_since = (function logseq$db_sync$storage$fetch_tx_since(sql,since_t){
var rows = logseq.db_sync.common.get_sql_rows(logseq.db_sync.common.sql_exec.cljs$core$IFn$_invoke$arity$variadic(sql,"select t, tx from tx_log where t > ? order by t asc",cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([since_t], 0)));
return cljs.core.mapv.cljs$core$IFn$_invoke$arity$2((function (row){
return new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"t","t",-1397832519),(row["t"]),new cljs.core.Keyword(null,"tx","tx",466630418),(row["tx"])], null);
}),rows);
});
logseq.db_sync.storage.delete_addrs_BANG_ = (function logseq$db_sync$storage$delete_addrs_BANG_(sql,addrs){
if(cljs.core.seq(addrs)){
var placeholders = clojure.string.join.cljs$core$IFn$_invoke$arity$2(",",cljs.core.map.cljs$core$IFn$_invoke$arity$2(cljs.core.constantly("?"),addrs));
return cljs.core.apply.cljs$core$IFn$_invoke$arity$4(logseq.db_sync.common.sql_exec,sql,(""+"delete from kvs where addr in ("+cljs.core.str.cljs$core$IFn$_invoke$arity$1(placeholders)+")"),addrs);
} else {
return null;
}
});
logseq.db_sync.storage.upsert_addr_content_BANG_ = (function logseq$db_sync$storage$upsert_addr_content_BANG_(sql,data){
var seq__40812 = cljs.core.seq(data);
var chunk__40813 = null;
var count__40814 = (0);
var i__40815 = (0);
while(true){
if((i__40815 < count__40814)){
var item = chunk__40813.cljs$core$IIndexed$_nth$arity$2(null,i__40815);
logseq.db_sync.common.sql_exec.cljs$core$IFn$_invoke$arity$variadic(sql,(""+"insert into kvs (addr, content, addresses) values (?, ?, ?)"+" on conflict(addr) do update set content = excluded.content, addresses = excluded.addresses"),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([(item["addr"]),(item["content"]),(item["addresses"])], 0));


var G__40931 = seq__40812;
var G__40932 = chunk__40813;
var G__40933 = count__40814;
var G__40934 = (i__40815 + (1));
seq__40812 = G__40931;
chunk__40813 = G__40932;
count__40814 = G__40933;
i__40815 = G__40934;
continue;
} else {
var temp__5804__auto__ = cljs.core.seq(seq__40812);
if(temp__5804__auto__){
var seq__40812__$1 = temp__5804__auto__;
if(cljs.core.chunked_seq_QMARK_(seq__40812__$1)){
var c__5673__auto__ = cljs.core.chunk_first(seq__40812__$1);
var G__40939 = cljs.core.chunk_rest(seq__40812__$1);
var G__40940 = c__5673__auto__;
var G__40941 = cljs.core.count(c__5673__auto__);
var G__40942 = (0);
seq__40812 = G__40939;
chunk__40813 = G__40940;
count__40814 = G__40941;
i__40815 = G__40942;
continue;
} else {
var item = cljs.core.first(seq__40812__$1);
logseq.db_sync.common.sql_exec.cljs$core$IFn$_invoke$arity$variadic(sql,(""+"insert into kvs (addr, content, addresses) values (?, ?, ?)"+" on conflict(addr) do update set content = excluded.content, addresses = excluded.addresses"),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([(item["addr"]),(item["content"]),(item["addresses"])], 0));


var G__40944 = cljs.core.next(seq__40812__$1);
var G__40945 = null;
var G__40946 = (0);
var G__40947 = (0);
seq__40812 = G__40944;
chunk__40813 = G__40945;
count__40814 = G__40946;
i__40815 = G__40947;
continue;
}
} else {
return null;
}
}
break;
}
});
logseq.db_sync.storage.restore_data_from_addr = (function logseq$db_sync$storage$restore_data_from_addr(sql,addr){
var temp__5804__auto__ = logseq.db_sync.storage.select_one.cljs$core$IFn$_invoke$arity$variadic(sql,"select content, addresses from kvs where addr = ?",cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([addr], 0));
if(cljs.core.truth_(temp__5804__auto__)){
var row = temp__5804__auto__;
var map__40838 = cljs_bean.core.__GT_clj.cljs$core$IFn$_invoke$arity$1(row);
var map__40838__$1 = cljs.core.__destructure_map(map__40838);
var content = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40838__$1,new cljs.core.Keyword(null,"content","content",15833224));
var addresses = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40838__$1,new cljs.core.Keyword(null,"addresses","addresses",-559529694));
var addresses__$1 = (cljs.core.truth_(addresses)?JSON.parse(addresses):null);
var data = logseq.db_sync.common.read_transit(content);
if(cljs.core.truth_((function (){var and__5140__auto__ = addresses__$1;
if(cljs.core.truth_(and__5140__auto__)){
return cljs.core.map_QMARK_(data);
} else {
return and__5140__auto__;
}
})())){
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(data,new cljs.core.Keyword(null,"addresses","addresses",-559529694),addresses__$1);
} else {
return data;
}
} else {
return null;
}
});

/**
* @constructor
 * @implements {cljs.core.IMeta}
 * @implements {cljs.core.IWithMeta}
 * @implements {datascript.storage.IStorage}
*/
logseq.db_sync.storage.t_logseq$db_sync$storage40861 = (function (sql,meta40862){
this.sql = sql;
this.meta40862 = meta40862;
this.cljs$lang$protocol_mask$partition0$ = 393216;
this.cljs$lang$protocol_mask$partition1$ = 0;
});
(logseq.db_sync.storage.t_logseq$db_sync$storage40861.prototype.cljs$core$IWithMeta$_with_meta$arity$2 = (function (_40863,meta40862__$1){
var self__ = this;
var _40863__$1 = this;
return (new logseq.db_sync.storage.t_logseq$db_sync$storage40861(self__.sql,meta40862__$1));
}));

(logseq.db_sync.storage.t_logseq$db_sync$storage40861.prototype.cljs$core$IMeta$_meta$arity$1 = (function (_40863){
var self__ = this;
var _40863__$1 = this;
return self__.meta40862;
}));

(logseq.db_sync.storage.t_logseq$db_sync$storage40861.prototype.datascript$storage$IStorage$ = cljs.core.PROTOCOL_SENTINEL);

(logseq.db_sync.storage.t_logseq$db_sync$storage40861.prototype.datascript$storage$IStorage$_store$arity$3 = (function (_,addr_PLUS_data_seq,delete_addrs){
var self__ = this;
var ___$1 = this;
var data = cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (p__40868){
var vec__40869 = p__40868;
var addr = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40869,(0),null);
var data = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40869,(1),null);
var data_SINGLEQUOTE_ = ((cljs.core.map_QMARK_(data))?cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(data,new cljs.core.Keyword(null,"addresses","addresses",-559529694)):data);
var addresses = ((cljs.core.map_QMARK_(data))?(function (){var temp__5804__auto__ = new cljs.core.Keyword(null,"addresses","addresses",-559529694).cljs$core$IFn$_invoke$arity$1(data);
if(cljs.core.truth_(temp__5804__auto__)){
var addresses = temp__5804__auto__;
return JSON.stringify(cljs_bean.core.__GT_js.cljs$core$IFn$_invoke$arity$1(addresses));
} else {
return null;
}
})():null);
return ({"addr": addr, "content": logseq.db_sync.common.write_transit(data_SINGLEQUOTE_), "addresses": addresses});
}),addr_PLUS_data_seq);
logseq.db_sync.storage.delete_addrs_BANG_(self__.sql,delete_addrs);

return logseq.db_sync.storage.upsert_addr_content_BANG_(self__.sql,data);
}));

(logseq.db_sync.storage.t_logseq$db_sync$storage40861.prototype.datascript$storage$IStorage$_restore$arity$2 = (function (_,addr){
var self__ = this;
var ___$1 = this;
return logseq.db_sync.storage.restore_data_from_addr(self__.sql,addr);
}));

(logseq.db_sync.storage.t_logseq$db_sync$storage40861.getBasis = (function (){
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"sql","sql",-1402986983,null),new cljs.core.Symbol(null,"meta40862","meta40862",-571034360,null)], null);
}));

(logseq.db_sync.storage.t_logseq$db_sync$storage40861.cljs$lang$type = true);

(logseq.db_sync.storage.t_logseq$db_sync$storage40861.cljs$lang$ctorStr = "logseq.db-sync.storage/t_logseq$db_sync$storage40861");

(logseq.db_sync.storage.t_logseq$db_sync$storage40861.cljs$lang$ctorPrWriter = (function (this__5434__auto__,writer__5435__auto__,opt__5436__auto__){
return cljs.core._write(writer__5435__auto__,"logseq.db-sync.storage/t_logseq$db_sync$storage40861");
}));

/**
 * Positional factory function for logseq.db-sync.storage/t_logseq$db_sync$storage40861.
 */
logseq.db_sync.storage.__GT_t_logseq$db_sync$storage40861 = (function logseq$db_sync$storage$__GT_t_logseq$db_sync$storage40861(sql,meta40862){
return (new logseq.db_sync.storage.t_logseq$db_sync$storage40861(sql,meta40862));
});


logseq.db_sync.storage.new_sqlite_storage = (function logseq$db_sync$storage$new_sqlite_storage(sql){
return (new logseq.db_sync.storage.t_logseq$db_sync$storage40861(sql,cljs.core.PersistentArrayMap.EMPTY));
});
logseq.db_sync.storage.append_tx_for_tx_report = (function logseq$db_sync$storage$append_tx_for_tx_report(sql,p__40875){
var map__40876 = p__40875;
var map__40876__$1 = cljs.core.__destructure_map(map__40876);
var db_after = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40876__$1,new cljs.core.Keyword(null,"db-after","db-after",-571884666));
var db_before = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40876__$1,new cljs.core.Keyword(null,"db-before","db-before",-553691536));
var tx_data = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40876__$1,new cljs.core.Keyword(null,"tx-data","tx-data",934159761));
var new_t = logseq.db_sync.storage.next_t_BANG_(sql);
var created_at = logseq.db_sync.common.now_ms();
var normalized_data = logseq.db.common.normalize.normalize_tx_data(db_after,db_before,tx_data);
var tx_str = logseq.db_sync.common.write_transit(normalized_data);
return logseq.db_sync.storage.append_tx_BANG_(sql,new_t,tx_str,created_at);
});
logseq.db_sync.storage.listen_db_updates_BANG_ = (function logseq$db_sync$storage$listen_db_updates_BANG_(sql,conn){
var G__40885 = conn;
var G__40886 = new cljs.core.Keyword("logseq.db-sync.storage","listen-db-updates","logseq.db-sync.storage/listen-db-updates",-324640931);
var G__40887 = (function (tx_report){
return logseq.db_sync.storage.append_tx_for_tx_report(sql,tx_report);
});
return (datascript.core.listen_BANG_.cljs$core$IFn$_invoke$arity$3 ? datascript.core.listen_BANG_.cljs$core$IFn$_invoke$arity$3(G__40885,G__40886,G__40887) : datascript.core.listen_BANG_.call(null,G__40885,G__40886,G__40887));
});
logseq.db_sync.storage.open_conn = (function logseq$db_sync$storage$open_conn(sql){
logseq.db_sync.storage.init_schema_BANG_(sql);

var storage = logseq.db_sync.storage.new_sqlite_storage(sql);
var schema = logseq.db.frontend.schema.schema;
var conn = logseq.db.common.sqlite.get_storage_conn(storage,schema);
logseq.db_sync.storage.listen_db_updates_BANG_(sql,conn);

return conn;
});

//# sourceMappingURL=logseq.db_sync.storage.js.map
