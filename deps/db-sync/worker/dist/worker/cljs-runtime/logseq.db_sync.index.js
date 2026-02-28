import "./cljs_env.js";
import "./cljs.core.js";
import "./logseq.db_sync.common.js";
import "./promesa.core.js";
goog.provide('logseq.db_sync.index');
logseq.db_sync.index.user_upsert_cache_ttl_ms = (((60) * (60)) * (1000));
logseq.db_sync.index.user_upsert_cache_max = (1024);
if((typeof logseq !== 'undefined') && (typeof logseq.db_sync !== 'undefined') && (typeof logseq.db_sync.index !== 'undefined') && (typeof logseq.db_sync.index._STAR_user_upsert_cache !== 'undefined')){
} else {
logseq.db_sync.index._STAR_user_upsert_cache = cljs.core.atom.cljs$core$IFn$_invoke$arity$1(cljs.core.PersistentArrayMap.EMPTY);
}
logseq.db_sync.index.prune_user_upsert_cache_BANG_ = (function logseq$db_sync$index$prune_user_upsert_cache_BANG_(now_ms){
return cljs.core.swap_BANG_.cljs$core$IFn$_invoke$arity$2(logseq.db_sync.index._STAR_user_upsert_cache,(function (cache){
var cache__$1 = cljs.core.into.cljs$core$IFn$_invoke$arity$3(cljs.core.PersistentArrayMap.EMPTY,cljs.core.remove.cljs$core$IFn$_invoke$arity$1((function (p__40573){
var vec__40574 = p__40573;
var _ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40574,(0),null);
var map__40577 = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40574,(1),null);
var map__40577__$1 = cljs.core.__destructure_map(map__40577);
var cached_at = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40577__$1,new cljs.core.Keyword(null,"cached-at","cached-at",2133976632));
return ((now_ms - cached_at) >= logseq.db_sync.index.user_upsert_cache_ttl_ms);
})),cache);
var count_cache = cljs.core.count(cache__$1);
if((count_cache > logseq.db_sync.index.user_upsert_cache_max)){
return cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,cljs.core.drop.cljs$core$IFn$_invoke$arity$2((count_cache - logseq.db_sync.index.user_upsert_cache_max),cljs.core.sort_by.cljs$core$IFn$_invoke$arity$2(cljs.core.comp.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"cached-at","cached-at",2133976632),cljs.core.val),cache__$1)));
} else {
return cache__$1;
}
}));
});
logseq.db_sync.index.cache_user_upsert_BANG_ = (function logseq$db_sync$index$cache_user_upsert_BANG_(user_id,email,email_verified,username,now_ms){
cljs.core.swap_BANG_.cljs$core$IFn$_invoke$arity$4(logseq.db_sync.index._STAR_user_upsert_cache,cljs.core.assoc,user_id,new cljs.core.PersistentArrayMap(null, 4, [new cljs.core.Keyword(null,"email","email",1415816706),email,new cljs.core.Keyword(null,"email-verified","email-verified",-1932659306),email_verified,new cljs.core.Keyword(null,"username","username",1605666410),username,new cljs.core.Keyword(null,"cached-at","cached-at",2133976632),now_ms], null));

return logseq.db_sync.index.prune_user_upsert_cache_BANG_(now_ms);
});
logseq.db_sync.index._LT_index_init_BANG_ = (function logseq$db_sync$index$_LT_index_init_BANG_(db){
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35464__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common._LT_d1_run(db,(""+"create table if not exists graphs ("+"graph_id TEXT primary key,"+"graph_name TEXT,"+"user_id TEXT,"+"schema_version TEXT,"+"created_at INTEGER,"+"updated_at INTEGER"+");"))),(function (___35454__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common._LT_d1_run(db,(""+"create table if not exists users ("+"id TEXT primary key,"+"email TEXT,"+"email_verified INTEGER,"+"username TEXT"+");"))),(function (___35454__auto____$1){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common._LT_d1_run(db,(""+"create table if not exists user_rsa_keys ("+"user_id TEXT primary key,"+"public_key TEXT,"+"encrypted_private_key TEXT,"+"created_at INTEGER,"+"updated_at INTEGER"+");"))),(function (___35454__auto____$2){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common._LT_d1_run(db,(""+"create table if not exists graph_members ("+"user_id TEXT,"+"graph_id TEXT,"+"role TEXT,"+"invited_by TEXT,"+"created_at INTEGER,"+"primary key (user_id, graph_id),"+"check (role in ('manager', 'member'))"+");"))),(function (___35454__auto____$3){
return promesa.protocols._promise(logseq.db_sync.common._LT_d1_run(db,(""+"create table if not exists graph_aes_keys ("+"graph_id TEXT,"+"user_id TEXT,"+"encrypted_aes_key TEXT,"+"created_at INTEGER,"+"updated_at INTEGER,"+"primary key (graph_id, user_id)"+");")));
}));
}));
}));
}));
}));
});
logseq.db_sync.index._LT_index_list = (function logseq$db_sync$index$_LT_index_list(db,user_id){
if(typeof user_id === 'string'){
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common._LT_d1_all.cljs$core$IFn$_invoke$arity$variadic(db,(""+"select g.graph_id, g.graph_name, g.schema_version, g.created_at, g.updated_at, "+"m.role, m.invited_by "+"from graphs g "+"left join graph_members m on g.graph_id = m.graph_id and m.user_id = ? "+"where g.user_id = ? or m.user_id = ? "+"order by g.updated_at desc"),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([user_id,user_id,user_id], 0))),(function (result){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common.get_sql_rows(result)),(function (rows){
return promesa.protocols._promise(cljs.core.mapv.cljs$core$IFn$_invoke$arity$2((function (row){
return new cljs.core.PersistentArrayMap(null, 7, [new cljs.core.Keyword(null,"graph-id","graph-id",-205404489),(row["graph_id"]),new cljs.core.Keyword(null,"graph-name","graph-name",416773857),(row["graph_name"]),new cljs.core.Keyword(null,"schema-version","schema-version",1117939594),(row["schema_version"]),new cljs.core.Keyword(null,"role","role",-736691072),(row["role"]),new cljs.core.Keyword(null,"invited-by","invited-by",-530277396),(row["invited_by"]),new cljs.core.Keyword(null,"created-at","created-at",-89248644),(row["created_at"]),new cljs.core.Keyword(null,"updated-at","updated-at",-1592622336),(row["updated_at"])], null);
}),rows));
}));
}));
}));
} else {
return cljs.core.PersistentVector.EMPTY;
}
});
logseq.db_sync.index._LT_index_upsert_BANG_ = (function logseq$db_sync$index$_LT_index_upsert_BANG_(db,graph_id,graph_name,user_id,schema_version){
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common.now_ms()),(function (now){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common._LT_d1_run.cljs$core$IFn$_invoke$arity$variadic(db,(""+"insert into graphs (graph_id, graph_name, user_id, schema_version, created_at, updated_at) "+"values (?, ?, ?, ?, ?, ?) "+"on conflict(graph_id) do update set "+"graph_name = excluded.graph_name, "+"user_id = excluded.user_id, "+"schema_version = excluded.schema_version, "+"updated_at = excluded.updated_at"),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([graph_id,graph_name,user_id,schema_version,now,now], 0))),(function (result){
return promesa.protocols._promise(result);
}));
}));
}));
});
logseq.db_sync.index._LT_index_delete_BANG_ = (function logseq$db_sync$index$_LT_index_delete_BANG_(db,graph_id){
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35464__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common._LT_d1_run.cljs$core$IFn$_invoke$arity$variadic(db,"delete from graph_members where graph_id = ?",cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([graph_id], 0))),(function (___35454__auto__){
return promesa.protocols._promise(logseq.db_sync.common._LT_d1_run.cljs$core$IFn$_invoke$arity$variadic(db,"delete from graphs where graph_id = ?",cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([graph_id], 0)));
}));
}));
});
logseq.db_sync.index._LT_graph_name_exists_QMARK_ = (function logseq$db_sync$index$_LT_graph_name_exists_QMARK_(db,graph_name,user_id){
if(((typeof graph_name === 'string') && (typeof user_id === 'string'))){
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common._LT_d1_all.cljs$core$IFn$_invoke$arity$variadic(db,"select graph_id from graphs where graph_name = ? and user_id = ?",cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([graph_name,user_id], 0))),(function (result){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common.get_sql_rows(result)),(function (rows){
return promesa.protocols._promise(cljs.core.boolean$(cljs.core.seq(rows)));
}));
}));
}));
} else {
return null;
}
});
logseq.db_sync.index._LT_user_upsert_BANG_ = (function logseq$db_sync$index$_LT_user_upsert_BANG_(db,claims){
var user_id = (claims["sub"]);
if(typeof user_id === 'string'){
var email = (claims["email"]);
var email_verified = (claims["email_verified"]);
var username = (claims["cognito:username"]);
var email_verified__$1 = ((email_verified === true)?(1):((email_verified === false)?(0):null
));
var now = logseq.db_sync.common.now_ms();
var cached = cljs.core.get.cljs$core$IFn$_invoke$arity$2(cljs.core.deref(logseq.db_sync.index._STAR_user_upsert_cache),user_id);
if(cljs.core.truth_((function (){var and__5140__auto__ = cached;
if(cljs.core.truth_(and__5140__auto__)){
return ((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(email,new cljs.core.Keyword(null,"email","email",1415816706).cljs$core$IFn$_invoke$arity$1(cached))) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(email_verified__$1,new cljs.core.Keyword(null,"email-verified","email-verified",-1932659306).cljs$core$IFn$_invoke$arity$1(cached))) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(username,new cljs.core.Keyword(null,"username","username",1605666410).cljs$core$IFn$_invoke$arity$1(cached))) && (((now - new cljs.core.Keyword(null,"cached-at","cached-at",2133976632).cljs$core$IFn$_invoke$arity$1(cached)) < logseq.db_sync.index.user_upsert_cache_ttl_ms)))))));
} else {
return and__5140__auto__;
}
})())){
return logseq.db_sync.index.cache_user_upsert_BANG_(user_id,email,email_verified__$1,username,now);
} else {
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common._LT_d1_run.cljs$core$IFn$_invoke$arity$variadic(db,(""+"insert into users (id, email, email_verified, username) "+"values (?, ?, ?, ?) "+"on conflict(id) do update set "+"email = excluded.email, "+"email_verified = excluded.email_verified, "+"username = excluded.username"),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([user_id,email,email_verified__$1,username], 0))),(function (result){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index.cache_user_upsert_BANG_(user_id,email,email_verified__$1,username,now)),(function (___35454__auto__){
return promesa.protocols._promise(result);
}));
}));
}));
}
} else {
return null;
}
});
logseq.db_sync.index._LT_user_id_by_email = (function logseq$db_sync$index$_LT_user_id_by_email(db,email){
if(typeof email === 'string'){
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common._LT_d1_all.cljs$core$IFn$_invoke$arity$variadic(db,new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"session","session",1008279103),"first-primary"], null),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2(["select id from users where email = ?",email], 0))),(function (result){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common.get_sql_rows(result)),(function (rows){
return promesa.protocols._mcat(promesa.protocols._promise(cljs.core.first(rows)),(function (row){
return promesa.protocols._promise((cljs.core.truth_(row)?(row["id"]):null));
}));
}));
}));
}));
} else {
return null;
}
});
logseq.db_sync.index._LT_user_rsa_key_pair_upsert_BANG_ = (function logseq$db_sync$index$_LT_user_rsa_key_pair_upsert_BANG_(db,user_id,public_key,encrypted_private_key){
if(typeof user_id === 'string'){
var now = logseq.db_sync.common.now_ms();
return logseq.db_sync.common._LT_d1_run.cljs$core$IFn$_invoke$arity$variadic(db,(""+"insert into user_rsa_keys (user_id, public_key, encrypted_private_key, created_at, updated_at) "+"values (?, ?, ?, ?, ?) "+"on conflict(user_id) do update set "+"public_key = excluded.public_key, "+"encrypted_private_key = excluded.encrypted_private_key, "+"updated_at = excluded.updated_at"),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([user_id,public_key,encrypted_private_key,now,now], 0));
} else {
return null;
}
});
logseq.db_sync.index._LT_user_rsa_key_pair = (function logseq$db_sync$index$_LT_user_rsa_key_pair(db,user_id){
if(typeof user_id === 'string'){
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common._LT_d1_all.cljs$core$IFn$_invoke$arity$variadic(db,new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"session","session",1008279103),"first-primary"], null),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2(["select public_key, encrypted_private_key from user_rsa_keys where user_id = ?",user_id], 0))),(function (result){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common.get_sql_rows(result)),(function (rows){
return promesa.protocols._mcat(promesa.protocols._promise(cljs.core.first(rows)),(function (row){
return promesa.protocols._promise((cljs.core.truth_(row)?new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"public-key","public-key",-2106850051),(row["public_key"]),new cljs.core.Keyword(null,"encrypted-private-key","encrypted-private-key",1069899594),(row["encrypted_private_key"])], null):null));
}));
}));
}));
}));
} else {
return null;
}
});
logseq.db_sync.index._LT_user_rsa_public_key_by_email = (function logseq$db_sync$index$_LT_user_rsa_public_key_by_email(db,email){
if(typeof email === 'string'){
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common._LT_d1_all.cljs$core$IFn$_invoke$arity$variadic(db,new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"session","session",1008279103),"first-primary"], null),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([(""+"select k.public_key from user_rsa_keys k "+"left join users u on k.user_id = u.id "+"where u.email = ?"),email], 0))),(function (result){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common.get_sql_rows(result)),(function (rows){
return promesa.protocols._mcat(promesa.protocols._promise(cljs.core.first(rows)),(function (row){
return promesa.protocols._promise((cljs.core.truth_(row)?(row["public_key"]):null));
}));
}));
}));
}));
} else {
return null;
}
});
logseq.db_sync.index._LT_graph_encrypted_aes_key_upsert_BANG_ = (function logseq$db_sync$index$_LT_graph_encrypted_aes_key_upsert_BANG_(db,graph_id,user_id,encrypted_aes_key){
if(((typeof graph_id === 'string') && (typeof user_id === 'string'))){
var now = logseq.db_sync.common.now_ms();
return logseq.db_sync.common._LT_d1_run.cljs$core$IFn$_invoke$arity$variadic(db,(""+"insert into graph_aes_keys (graph_id, user_id, encrypted_aes_key, created_at, updated_at) "+"values (?, ?, ?, ?, ?) "+"on conflict(graph_id, user_id) do update set "+"encrypted_aes_key = excluded.encrypted_aes_key, "+"updated_at = excluded.updated_at"),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([graph_id,user_id,encrypted_aes_key,now,now], 0));
} else {
return null;
}
});
logseq.db_sync.index._LT_graph_encrypted_aes_key = (function logseq$db_sync$index$_LT_graph_encrypted_aes_key(db,graph_id,user_id){
if(((typeof graph_id === 'string') && (typeof user_id === 'string'))){
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common._LT_d1_all.cljs$core$IFn$_invoke$arity$variadic(db,"select encrypted_aes_key from graph_aes_keys where graph_id = ? and user_id = ?",cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([graph_id,user_id], 0))),(function (result){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common.get_sql_rows(result)),(function (rows){
return promesa.protocols._mcat(promesa.protocols._promise(cljs.core.first(rows)),(function (row){
return promesa.protocols._promise((cljs.core.truth_(row)?(row["encrypted_aes_key"]):null));
}));
}));
}));
}));
} else {
return null;
}
});
logseq.db_sync.index._LT_graph_member_upsert_BANG_ = (function logseq$db_sync$index$_LT_graph_member_upsert_BANG_(db,graph_id,user_id,role,invited_by){
var now = logseq.db_sync.common.now_ms();
return logseq.db_sync.common._LT_d1_run.cljs$core$IFn$_invoke$arity$variadic(db,(""+"insert into graph_members (user_id, graph_id, role, invited_by, created_at) "+"values (?, ?, ?, ?, ?) "+"on conflict(user_id, graph_id) do update set "+"role = excluded.role, "+"invited_by = excluded.invited_by"),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([user_id,graph_id,role,invited_by,now], 0));
});
logseq.db_sync.index._LT_graph_members_list = (function logseq$db_sync$index$_LT_graph_members_list(db,graph_id){
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common._LT_d1_all.cljs$core$IFn$_invoke$arity$variadic(db,new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"session","session",1008279103),"first-primary"], null),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([(""+"select m.user_id, m.graph_id, m.role, m.invited_by, m.created_at, "+"u.email, u.username "+"from graph_members m "+"left join users u on m.user_id = u.id "+"where m.graph_id = ? order by m.created_at asc"),graph_id], 0))),(function (result){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common.get_sql_rows(result)),(function (rows){
return promesa.protocols._promise(cljs.core.mapv.cljs$core$IFn$_invoke$arity$2((function (row){
return new cljs.core.PersistentArrayMap(null, 7, [new cljs.core.Keyword(null,"user-id","user-id",-206822291),(row["user_id"]),new cljs.core.Keyword(null,"graph-id","graph-id",-205404489),(row["graph_id"]),new cljs.core.Keyword(null,"role","role",-736691072),(row["role"]),new cljs.core.Keyword(null,"invited-by","invited-by",-530277396),(row["invited_by"]),new cljs.core.Keyword(null,"created-at","created-at",-89248644),(row["created_at"]),new cljs.core.Keyword(null,"email","email",1415816706),(row["email"]),new cljs.core.Keyword(null,"username","username",1605666410),(row["username"])], null);
}),rows));
}));
}));
}));
});
logseq.db_sync.index._LT_graph_member_update_role_BANG_ = (function logseq$db_sync$index$_LT_graph_member_update_role_BANG_(db,graph_id,user_id,role){
return logseq.db_sync.common._LT_d1_run.cljs$core$IFn$_invoke$arity$variadic(db,(""+"update graph_members set role = ? "+"where graph_id = ? and user_id = ?"),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([role,graph_id,user_id], 0));
});
logseq.db_sync.index._LT_graph_member_delete_BANG_ = (function logseq$db_sync$index$_LT_graph_member_delete_BANG_(db,graph_id,user_id){
return logseq.db_sync.common._LT_d1_run.cljs$core$IFn$_invoke$arity$variadic(db,"delete from graph_members where graph_id = ? and user_id = ?",cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([graph_id,user_id], 0));
});
logseq.db_sync.index._LT_graph_member_role = (function logseq$db_sync$index$_LT_graph_member_role(db,graph_id,user_id){
if(((typeof graph_id === 'string') && (typeof user_id === 'string'))){
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common._LT_d1_all.cljs$core$IFn$_invoke$arity$variadic(db,"select role from graph_members where graph_id = ? and user_id = ?",cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([graph_id,user_id], 0))),(function (result){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common.get_sql_rows(result)),(function (rows){
return promesa.protocols._mcat(promesa.protocols._promise(cljs.core.first(rows)),(function (row){
return promesa.protocols._promise((cljs.core.truth_(row)?(row["role"]):null));
}));
}));
}));
}));
} else {
return null;
}
});
logseq.db_sync.index._LT_user_has_access_to_graph_QMARK_ = (function logseq$db_sync$index$_LT_user_has_access_to_graph_QMARK_(db,graph_id,user_id){
if(((typeof graph_id === 'string') && (typeof user_id === 'string'))){
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common._LT_d1_all.cljs$core$IFn$_invoke$arity$variadic(db,(""+"select graph_id from graphs where graph_id = ? and user_id = ? "+"union select graph_id from graph_members where graph_id = ? and user_id = ?"),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([graph_id,user_id,graph_id,user_id], 0))),(function (result){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common.get_sql_rows(result)),(function (rows){
return promesa.protocols._promise(cljs.core.boolean$(cljs.core.seq(rows)));
}));
}));
}));
} else {
return null;
}
});
logseq.db_sync.index._LT_user_is_manager_QMARK_ = (function logseq$db_sync$index$_LT_user_is_manager_QMARK_(db,graph_id,user_id){
if(((typeof graph_id === 'string') && (typeof user_id === 'string'))){
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common._LT_d1_all.cljs$core$IFn$_invoke$arity$variadic(db,(""+"select graph_id from graphs where graph_id = ? and user_id = ? "+"union select graph_id from graph_members where graph_id = ? and user_id = ? and role = 'manager'"),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([graph_id,user_id,graph_id,user_id], 0))),(function (result){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.common.get_sql_rows(result)),(function (rows){
return promesa.protocols._promise(cljs.core.boolean$(cljs.core.seq(rows)));
}));
}));
}));
} else {
return null;
}
});

//# sourceMappingURL=logseq.db_sync.index.js.map
