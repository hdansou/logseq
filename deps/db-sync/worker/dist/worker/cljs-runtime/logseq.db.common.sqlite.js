import "./cljs_env.js";
import "./cljs.core.js";
import "./shadow.esm.esm_import$path.js";
import "./clojure.string.js";
import "./datascript.core.js";
import "./logseq.db.sqlite.util.js";
goog.provide('logseq.db.common.sqlite');
/**
 * Creates a sqlite table for use with datascript.storage if one doesn't exist
 */
logseq.db.common.sqlite.create_kvs_table_BANG_ = (function logseq$db$common$sqlite$create_kvs_table_BANG_(sqlite_db){
return sqlite_db.exec("create table if not exists kvs (addr INTEGER primary key, content TEXT, addresses JSON)");
});
/**
 * Given a datascript storage, returns a datascript connection for it
 */
logseq.db.common.sqlite.get_storage_conn = (function logseq$db$common$sqlite$get_storage_conn(storage,schema){
var or__5142__auto__ = (datascript.core.restore_conn.cljs$core$IFn$_invoke$arity$1 ? datascript.core.restore_conn.cljs$core$IFn$_invoke$arity$1(storage) : datascript.core.restore_conn.call(null,storage));
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
var G__40703 = schema;
var G__40704 = new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"storage","storage",1867247511),storage], null);
return (datascript.core.create_conn.cljs$core$IFn$_invoke$arity$2 ? datascript.core.create_conn.cljs$core$IFn$_invoke$arity$2(G__40703,G__40704) : datascript.core.create_conn.call(null,G__40703,G__40704));
}
});
logseq.db.common.sqlite.sanitize_db_name = (function logseq$db$common$sqlite$sanitize_db_name(db_name){
return clojure.string.replace(clojure.string.replace(clojure.string.replace(clojure.string.replace(db_name,logseq.db.sqlite.util.db_version_prefix,""),"/","_"),"\\","_"),":","_");
});
logseq.db.common.sqlite.get_db_full_path = (function logseq$db$common$sqlite$get_db_full_path(graphs_dir,db_name){
var db_name_SINGLEQUOTE_ = logseq.db.common.sqlite.sanitize_db_name(db_name);
var graph_dir = shadow.esm.esm_import$path.join(graphs_dir,db_name_SINGLEQUOTE_);
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [db_name_SINGLEQUOTE_,shadow.esm.esm_import$path.join(graph_dir,"db.sqlite")], null);
});
logseq.db.common.sqlite.get_db_backups_path = (function logseq$db$common$sqlite$get_db_backups_path(graphs_dir,db_name){
var db_name_SINGLEQUOTE_ = logseq.db.common.sqlite.sanitize_db_name(db_name);
return shadow.esm.esm_import$path.join(graphs_dir,db_name_SINGLEQUOTE_,"backups");
});

//# sourceMappingURL=logseq.db.common.sqlite.js.map
