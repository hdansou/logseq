import "./cljs_env.js";
import "./cljs.core.js";
import "./clojure.data.js";
import "./datascript.core.js";
import "./datascript.impl.entity.js";
import "./logseq.common.util.date_time.js";
import "./logseq.db.frontend.content.js";
import "./logseq.db.frontend.entity_util.js";
import "./logseq.db.frontend.property.js";
goog.provide('logseq.db.common.entity_plus');
/**
 * No such entities with these :db/ident, but `(d/entity <db> <ident>)` has been called somewhere.
 */
logseq.db.common.entity_plus.nil_db_ident_entities = new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 17, [new cljs.core.Keyword("block","tx-id","block/tx-id",547556161),null,new cljs.core.Keyword("db","index","db/index",-1531680669),null,new cljs.core.Keyword("block","_refs","block/_refs",830218531),null,new cljs.core.Keyword("block","warning","block/warning",2131709542),null,new cljs.core.Keyword("block","uuid","block/uuid",-1991494552),null,new cljs.core.Keyword("db","valueType","db/valueType",1827971944),null,new cljs.core.Keyword("block.temp","ast-body","block.temp/ast-body",-464258035),null,new cljs.core.Keyword("block.temp","has-children?","block.temp/has-children?",935519725),null,new cljs.core.Keyword("block","journal-day","block/journal-day",-145748366),null,new cljs.core.Keyword("block","level","block/level",1182509971),null,new cljs.core.Keyword("db","cardinality","db/cardinality",-104975659),null,new cljs.core.Keyword("block.temp","load-status","block.temp/load-status",-1066723049),null,new cljs.core.Keyword("db","ident","db/ident",-737096),null,new cljs.core.Keyword("block","heading-level","block/heading-level",661361785),null,new cljs.core.Keyword("block","name","block/name",1619760316),null,new cljs.core.Keyword("block.temp","ast-title","block.temp/ast-title",-940352067),null,new cljs.core.Keyword("logseq.property","_query","logseq.property/_query",-1160583010),null], null), null);
/**
 * These db-ident entities are immutable,
 *   it means `(db/entity :block/title)` always return same result
 */
logseq.db.common.entity_plus.immutable_db_ident_entities = new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 17, [new cljs.core.Keyword("logseq.property.asset","type","logseq.property.asset/type",-1142083098),null,new cljs.core.Keyword("block","link","block/link",-1872399993),null,new cljs.core.Keyword("block","updated-at","block/updated-at",-1516550551),null,new cljs.core.Keyword("block","refs","block/refs",-1214495349),null,new cljs.core.Keyword("block","closed-value-property","block/closed-value-property",1157792813),null,new cljs.core.Keyword("logseq.property.asset","checksum","logseq.property.asset/checksum",-1011416979),null,new cljs.core.Keyword("logseq.property.node","display-type","logseq.property.node/display-type",442446189),null,new cljs.core.Keyword("block","created-at","block/created-at",1440015),null,new cljs.core.Keyword("block","collapsed?","block/collapsed?",2140210991),null,new cljs.core.Keyword("block","tags","block/tags",1814948340),null,new cljs.core.Keyword("block","title","block/title",710445684),null,new cljs.core.Keyword("logseq.property","created-from-property","logseq.property/created-from-property",-861892267),null,new cljs.core.Keyword("logseq.property","icon","logseq.property/icon",589123285),null,new cljs.core.Keyword("block","parent","block/parent",-918309064),null,new cljs.core.Keyword("block","order","block/order",-1429282437),null,new cljs.core.Keyword("block","page","block/page",822314108),null,new cljs.core.Keyword("logseq.kv","db-type","logseq.kv/db-type",1106888767),null], null), null);
if(cljs.core.empty_QMARK_(cljs.core.last(clojure.data.diff(logseq.db.common.entity_plus.immutable_db_ident_entities,logseq.db.common.entity_plus.nil_db_ident_entities)))){
} else {
throw (new Error("Assert failed: (empty? (last (data/diff immutable-db-ident-entities nil-db-ident-entities)))"));
}
logseq.db.common.entity_plus.lookup_entity = cljs.core.deref(new cljs.core.Var(function(){return datascript.impl.entity.lookup_entity;},new cljs.core.Symbol("datascript.impl.entity","lookup-entity","datascript.impl.entity/lookup-entity",243995956,null),cljs.core.PersistentHashMap.fromArrays([new cljs.core.Keyword(null,"private","private",-558947994),new cljs.core.Keyword(null,"ns","ns",441598760),new cljs.core.Keyword(null,"name","name",1843675177),new cljs.core.Keyword(null,"file","file",-1269645878),new cljs.core.Keyword(null,"end-column","end-column",1425389514),new cljs.core.Keyword(null,"top-fn","top-fn",-2056129173),new cljs.core.Keyword(null,"column","column",2078222095),new cljs.core.Keyword(null,"line","line",212345235),new cljs.core.Keyword(null,"end-line","end-line",1837326455),new cljs.core.Keyword(null,"arglists","arglists",1661989754),new cljs.core.Keyword(null,"doc","doc",1913296891),new cljs.core.Keyword(null,"test","test",577538877)],[true,new cljs.core.Symbol(null,"datascript.impl.entity","datascript.impl.entity",1561405407,null),new cljs.core.Symbol(null,"lookup-entity","lookup-entity",-936060612,null),"datascript/impl/entity.cljc",(21),new cljs.core.PersistentArrayMap(null, 6, [new cljs.core.Keyword(null,"variadic?","variadic?",584179762),false,new cljs.core.Keyword(null,"fixed-arity","fixed-arity",1586445869),(3),new cljs.core.Keyword(null,"max-fixed-arity","max-fixed-arity",-690205543),(3),new cljs.core.Keyword(null,"method-params","method-params",-980792179),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"this","this",1028897902,null),new cljs.core.Symbol(null,"attr","attr",1036399174,null)], null),new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"this","this",1028897902,null),new cljs.core.Symbol(null,"attr","attr",1036399174,null),new cljs.core.Symbol(null,"not-found","not-found",1011451547,null)], null)], null),new cljs.core.Keyword(null,"arglists","arglists",1661989754),cljs.core.list(new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"this","this",1028897902,null),new cljs.core.Symbol(null,"attr","attr",1036399174,null)], null),new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"this","this",1028897902,null),new cljs.core.Symbol(null,"attr","attr",1036399174,null),new cljs.core.Symbol(null,"not-found","not-found",1011451547,null)], null)),new cljs.core.Keyword(null,"arglists-meta","arglists-meta",1944829838),cljs.core.list(null,null)], null),(1),(165),(165),cljs.core.list(new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"this","this",1028897902,null),new cljs.core.Symbol(null,"attr","attr",1036399174,null)], null),new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"this","this",1028897902,null),new cljs.core.Symbol(null,"attr","attr",1036399174,null),new cljs.core.Symbol(null,"not-found","not-found",1011451547,null)], null)),null,(cljs.core.truth_(datascript.impl.entity.lookup_entity)?datascript.impl.entity.lookup_entity.cljs$lang$test:null)])));
logseq.db.common.entity_plus._STAR_seen_immutable_entities = cljs.core.volatile_BANG_(cljs.core.PersistentArrayMap.EMPTY);
logseq.db.common.entity_plus.reset_immutable_entities_cache_BANG_ = (function logseq$db$common$entity_plus$reset_immutable_entities_cache_BANG_(){
return cljs.core.vreset_BANG_(logseq.db.common.entity_plus._STAR_seen_immutable_entities,cljs.core.PersistentArrayMap.EMPTY);
});
if((typeof logseq !== 'undefined') && (typeof logseq.db !== 'undefined') && (typeof logseq.db.common !== 'undefined') && (typeof logseq.db.common.entity_plus !== 'undefined') && (typeof logseq.db.common.entity_plus._STAR_reset_cache_background_task_running_f !== 'undefined')){
} else {
logseq.db.common.entity_plus._STAR_reset_cache_background_task_running_f = cljs.core.atom.cljs$core$IFn$_invoke$arity$1(null);
}
logseq.db.common.entity_plus.entity_memoized = (function logseq$db$common$entity_plus$entity_memoized(db,eid){
if(((cljs.core.qualified_keyword_QMARK_(eid)) && ((!((typeof process !== 'undefined')))))){
if(cljs.core.contains_QMARK_(logseq.db.common.entity_plus.nil_db_ident_entities,eid)){
return null;
} else {
var f = cljs.core.deref(logseq.db.common.entity_plus._STAR_reset_cache_background_task_running_f);
if(cljs.core.truth_((function (){var and__5140__auto__ = cljs.core.fn_QMARK_(f);
if(and__5140__auto__){
var and__5140__auto____$1 = (f.cljs$core$IFn$_invoke$arity$1 ? f.cljs$core$IFn$_invoke$arity$1(new cljs.core.Keyword("logseq.db.common.entity-plus","reset-immutable-entities-cache!","logseq.db.common.entity-plus/reset-immutable-entities-cache!",876655830)) : f.call(null,new cljs.core.Keyword("logseq.db.common.entity-plus","reset-immutable-entities-cache!","logseq.db.common.entity-plus/reset-immutable-entities-cache!",876655830)));
if(cljs.core.truth_(and__5140__auto____$1)){
return cljs.core.contains_QMARK_(logseq.db.common.entity_plus.immutable_db_ident_entities,eid);
} else {
return and__5140__auto____$1;
}
} else {
return and__5140__auto__;
}
})())){
var or__5142__auto__ = cljs.core.get.cljs$core$IFn$_invoke$arity$2(cljs.core.deref(logseq.db.common.entity_plus._STAR_seen_immutable_entities),eid);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
var r = (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,eid) : datascript.core.entity.call(null,db,eid));
if(cljs.core.truth_(r)){
logseq.db.common.entity_plus._STAR_seen_immutable_entities.cljs$core$IVolatile$_vreset_BANG_$arity$2(null,cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(logseq.db.common.entity_plus._STAR_seen_immutable_entities.cljs$core$IDeref$_deref$arity$1(null),eid,r));
} else {
}

return r;
}
} else {
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,eid) : datascript.core.entity.call(null,db,eid));
}
}
} else {
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,eid) : datascript.core.entity.call(null,db,eid));
}
});
/**
 * Faster version of d/entity without checking e exists.
 *   Only use it in performance-critical areas and where the existence of 'e' is confirmed.
 */
logseq.db.common.entity_plus.unsafe__GT_Entity = (function logseq$db$common$entity_plus$unsafe__GT_Entity(db,e){
if(cljs.core.pos_int_QMARK_(e)){
} else {
throw (new Error("Assert failed: (pos-int? e)"));
}

return (new datascript.impl.entity.Entity(db,e,cljs.core.volatile_BANG_(false),cljs.core.volatile_BANG_(cljs.core.PersistentArrayMap.EMPTY)));
});
/**
 * Whether the current graph is db-only. This should only be used in contexts
 * where both DB and file graph databases are possible e.g. graph-parser
 */
logseq.db.common.entity_plus.db_based_graph_QMARK_ = (function logseq$db$common$entity_plus$db_based_graph_QMARK_(db){
if(cljs.core.truth_(db)){
return ("db" === new cljs.core.Keyword("kv","value","kv/value",305981670).cljs$core$IFn$_invoke$arity$1(logseq.db.common.entity_plus.entity_memoized(db,new cljs.core.Keyword("logseq.kv","db-type","logseq.kv/db-type",1106888767))));
} else {
return null;
}
});
logseq.db.common.entity_plus.get_journal_title = (function logseq$db$common$entity_plus$get_journal_title(db,e){
return logseq.common.util.date_time.int__GT_journal_title(new cljs.core.Keyword("block","journal-day","block/journal-day",-145748366).cljs$core$IFn$_invoke$arity$1(e),new cljs.core.Keyword("logseq.property.journal","title-format","logseq.property.journal/title-format",-1536497954).cljs$core$IFn$_invoke$arity$1(logseq.db.common.entity_plus.entity_memoized(db,new cljs.core.Keyword("logseq.class","Journal","logseq.class/Journal",1979741081))));
});
logseq.db.common.entity_plus.get_block_title = (function logseq$db$common$entity_plus$get_block_title(e,k,default_value){
var db = e.db;
var db_based_QMARK_ = logseq.db.common.entity_plus.db_based_graph_QMARK_(db);
if(cljs.core.truth_((function (){var and__5140__auto__ = db_based_QMARK_;
if(cljs.core.truth_(and__5140__auto__)){
return logseq.db.frontend.entity_util.journal_QMARK_(e);
} else {
return and__5140__auto__;
}
})())){
return logseq.db.common.entity_plus.get_journal_title(db,e);
} else {
var or__5142__auto__ = cljs.core.get.cljs$core$IFn$_invoke$arity$2(e.kv,k);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
if(cljs.core.truth_(db_based_QMARK_)){
var result = (logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3 ? logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3(e,k,default_value) : logseq.db.common.entity_plus.lookup_entity.call(null,e,k,default_value));
var refs = new cljs.core.Keyword("block","refs","block/refs",-1214495349).cljs$core$IFn$_invoke$arity$1(e);
var result_SINGLEQUOTE_ = (cljs.core.truth_((function (){var and__5140__auto__ = typeof result === 'string';
if(and__5140__auto__){
return refs;
} else {
return and__5140__auto__;
}
})())?logseq.db.frontend.content.id_ref__GT_title_ref.cljs$core$IFn$_invoke$arity$variadic(result,refs,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"db","db",993250759),db,new cljs.core.Keyword(null,"replace-pages-with-same-name?","replace-pages-with-same-name?",-679714117),false], null)], 0)):result);
var or__5142__auto____$1 = result_SINGLEQUOTE_;
if(cljs.core.truth_(or__5142__auto____$1)){
return or__5142__auto____$1;
} else {
return default_value;
}
} else {
return (logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3 ? logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3(e,k,default_value) : logseq.db.common.entity_plus.lookup_entity.call(null,e,k,default_value));
}
}
}
});
logseq.db.common.entity_plus.lookup_kv_with_default_value = (function logseq$db$common$entity_plus$lookup_kv_with_default_value(db,e,k,default_value){
var or__5142__auto__ = cljs.core.get.cljs$core$IFn$_invoke$arity$2(e.kv,k);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
var result = (logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3 ? logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3(e,k,default_value) : logseq.db.common.entity_plus.lookup_entity.call(null,e,k,default_value));
if((!((result == null)))){
return result;
} else {
if(cljs.core.qualified_keyword_QMARK_(k)){
var temp__5804__auto__ = logseq.db.common.entity_plus.entity_memoized(db,k);
if(cljs.core.truth_(temp__5804__auto__)){
var property = temp__5804__auto__;
var property_type = (logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3 ? logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3(property,new cljs.core.Keyword("logseq.property","type","logseq.property/type",83842404),null) : logseq.db.common.entity_plus.lookup_entity.call(null,property,new cljs.core.Keyword("logseq.property","type","logseq.property/type",83842404),null));
if(cljs.core.keyword_identical_QMARK_(new cljs.core.Keyword(null,"checkbox","checkbox",1612615655),property_type)){
return (logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3 ? logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3(property,new cljs.core.Keyword("logseq.property","scalar-default-value","logseq.property/scalar-default-value",1595723014),null) : logseq.db.common.entity_plus.lookup_entity.call(null,property,new cljs.core.Keyword("logseq.property","scalar-default-value","logseq.property/scalar-default-value",1595723014),null));
} else {
return (logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3 ? logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3(property,new cljs.core.Keyword("logseq.property","default-value","logseq.property/default-value",-892079662),null) : logseq.db.common.entity_plus.lookup_entity.call(null,property,new cljs.core.Keyword("logseq.property","default-value","logseq.property/default-value",-892079662),null));
}
} else {
return null;
}
} else {
return null;
}
}
}
});
logseq.db.common.entity_plus.get_property_keys = (function logseq$db$common$entity_plus$get_property_keys(e){
var db = e.db;
if(cljs.core.truth_(logseq.db.common.entity_plus.db_based_graph_QMARK_(db))){
return cljs.core.filter.cljs$core$IFn$_invoke$arity$2(logseq.db.frontend.property.property_QMARK_,cljs.core.distinct.cljs$core$IFn$_invoke$arity$1(cljs.core.map.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"a","a",-2123407586),datascript.core.datoms.cljs$core$IFn$_invoke$arity$3(db,new cljs.core.Keyword(null,"eavt","eavt",-666437073),e.eid))));
} else {
return cljs.core.keys((logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3 ? logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3(e,new cljs.core.Keyword("block","properties","block/properties",708347145),null) : logseq.db.common.entity_plus.lookup_entity.call(null,e,new cljs.core.Keyword("block","properties","block/properties",708347145),null)));
}
});
logseq.db.common.entity_plus.get_properties = (function logseq$db$common$entity_plus$get_properties(e){
var db = e.db;
if(cljs.core.truth_(logseq.db.common.entity_plus.db_based_graph_QMARK_(db))){
var G__40439 = e;
var G__40440 = new cljs.core.Keyword("block","properties","block/properties",708347145);
var G__40441 = cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,cljs.core.filter.cljs$core$IFn$_invoke$arity$2((function (p__40442){
var vec__40444 = p__40442;
var k = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40444,(0),null);
var _ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40444,(1),null);
return logseq.db.frontend.property.property_QMARK_(k);
}),cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,e)));
return (logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3 ? logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3(G__40439,G__40440,G__40441) : logseq.db.common.entity_plus.lookup_entity.call(null,G__40439,G__40440,G__40441));
} else {
return (logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3 ? logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3(e,new cljs.core.Keyword("block","properties","block/properties",708347145),null) : logseq.db.common.entity_plus.lookup_entity.call(null,e,new cljs.core.Keyword("block","properties","block/properties",708347145),null));
}
});
logseq.db.common.entity_plus.lookup_kv_then_entity = (function logseq$db$common$entity_plus$lookup_kv_then_entity(var_args){
var G__40449 = arguments.length;
switch (G__40449) {
case 2:
return logseq.db.common.entity_plus.lookup_kv_then_entity.cljs$core$IFn$_invoke$arity$2((arguments[(0)]),(arguments[(1)]));

break;
case 3:
return logseq.db.common.entity_plus.lookup_kv_then_entity.cljs$core$IFn$_invoke$arity$3((arguments[(0)]),(arguments[(1)]),(arguments[(2)]));

break;
default:
throw (new Error(["Invalid arity: ",arguments.length].join("")));

}
});

(logseq.db.common.entity_plus.lookup_kv_then_entity.cljs$core$IFn$_invoke$arity$2 = (function (e,k){
return logseq.db.common.entity_plus.lookup_kv_then_entity.cljs$core$IFn$_invoke$arity$3(e,k,null);
}));

(logseq.db.common.entity_plus.lookup_kv_then_entity.cljs$core$IFn$_invoke$arity$3 = (function (e,k,default_value){
try{if(cljs.core.truth_(k)){
var db = e.db;
var G__40455 = k;
var G__40455__$1 = (((G__40455 instanceof cljs.core.Keyword))?G__40455.fqn:null);
switch (G__40455__$1) {
case "block/raw-title":
if(cljs.core.truth_((function (){var and__5140__auto__ = logseq.db.common.entity_plus.db_based_graph_QMARK_(db);
if(cljs.core.truth_(and__5140__auto__)){
return logseq.db.frontend.entity_util.journal_QMARK_(e);
} else {
return and__5140__auto__;
}
})())){
return logseq.db.common.entity_plus.get_journal_title(db,e);
} else {
return (logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3 ? logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3(e,new cljs.core.Keyword("block","title","block/title",710445684),default_value) : logseq.db.common.entity_plus.lookup_entity.call(null,e,new cljs.core.Keyword("block","title","block/title",710445684),default_value));
}

break;
case "block/properties":
return logseq.db.common.entity_plus.get_properties(e);

break;
case "block.temp/property-keys":
return logseq.db.common.entity_plus.get_property_keys(e);

break;
case "block/title":
return logseq.db.common.entity_plus.get_block_title(e,k,default_value);

break;
case "block/_parent":
return cljs.core.seq(cljs.core.remove.cljs$core$IFn$_invoke$arity$2((function (e__$1){
var or__5142__auto__ = new cljs.core.Keyword("logseq.property","created-from-property","logseq.property/created-from-property",-861892267).cljs$core$IFn$_invoke$arity$1(e__$1);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return new cljs.core.Keyword("block","closed-value-property","block/closed-value-property",1157792813).cljs$core$IFn$_invoke$arity$1(e__$1);
}
}),(logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3 ? logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3(e,k,default_value) : logseq.db.common.entity_plus.lookup_entity.call(null,e,k,default_value))));

break;
case "block/_raw-parent":
return (logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3 ? logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3(e,new cljs.core.Keyword("block","_parent","block/_parent",-639389670),default_value) : logseq.db.common.entity_plus.lookup_entity.call(null,e,new cljs.core.Keyword("block","_parent","block/_parent",-639389670),default_value));

break;
case "property/closed-values":
var G__40461 = (logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3 ? logseq.db.common.entity_plus.lookup_entity.cljs$core$IFn$_invoke$arity$3(e,new cljs.core.Keyword("block","_closed-value-property","block/_closed-value-property",-1361451442),default_value) : logseq.db.common.entity_plus.lookup_entity.call(null,e,new cljs.core.Keyword("block","_closed-value-property","block/_closed-value-property",-1361451442),default_value));
if((G__40461 == null)){
return null;
} else {
return cljs.core.sort_by.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("block","order","block/order",-1429282437),G__40461);
}

break;
default:
return logseq.db.common.entity_plus.lookup_kv_with_default_value(db,e,k,default_value);

}
} else {
return null;
}
}catch (e40454){var e__$1 = e40454;
return console.error(e__$1);
}}));

(logseq.db.common.entity_plus.lookup_kv_then_entity.cljs$lang$maxFixedArity = 3);

logseq.db.common.entity_plus.cache_with_kv = (function logseq$db$common$entity_plus$cache_with_kv(this$){
var v = cljs.core.deref(this$.cache);
return cljs.core.concat.cljs$core$IFn$_invoke$arity$2(cljs.core.seq(v),cljs.core.seq(this$.kv));
});
logseq.db.common.entity_plus.entity_ish_QMARK_ = (function logseq$db$common$entity_plus$entity_ish_QMARK_(x){
return (x instanceof datascript.impl.entity.Entity);
});
/**
 * Convert values so printing won't recurse forever:
 * - Entity => {:db/id eid}
 * - coll of Entity => coll of {:db/id ...}
 * - maps are walked (rare but safe)
 */
logseq.db.common.entity_plus.__GT_printable = (function logseq$db$common$entity_plus$__GT_printable(x){
if(logseq.db.common.entity_plus.entity_ish_QMARK_(x)){
return new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword("db","id","db/id",-1388397098),x.eid], null);
} else {
if(cljs.core.map_QMARK_(x)){
return cljs.core.reduce_kv((function (m,k,v){
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(m,k,(logseq.db.common.entity_plus.__GT_printable.cljs$core$IFn$_invoke$arity$1 ? logseq.db.common.entity_plus.__GT_printable.cljs$core$IFn$_invoke$arity$1(v) : logseq.db.common.entity_plus.__GT_printable.call(null,v)));
}),cljs.core.PersistentArrayMap.EMPTY,x);
} else {
if(cljs.core.sequential_QMARK_(x)){
return cljs.core.map.cljs$core$IFn$_invoke$arity$2(logseq.db.common.entity_plus.__GT_printable,x);
} else {
if(cljs.core.set_QMARK_(x)){
return cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentHashSet.EMPTY,cljs.core.map.cljs$core$IFn$_invoke$arity$2(logseq.db.common.entity_plus.__GT_printable,x));
} else {
return x;

}
}
}
}
});
(datascript.impl.entity.Entity.prototype.cljs$core$IEncodeJS$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.impl.entity.Entity.prototype.cljs$core$IEncodeJS$_clj__GT_js$arity$1 = (function (_this){
var _this__$1 = this;
return null;
}));

(datascript.impl.entity.Entity.prototype.cljs$core$IAssociative$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.impl.entity.Entity.prototype.cljs$core$IAssociative$_assoc$arity$3 = (function (this$,k,v){
var this$__$1 = this;
if((k instanceof cljs.core.Keyword)){
} else {
throw (new Error((""+"Assert failed: "+"attribute must be keyword"+"\n"+"(keyword? k)")));
}

(this$__$1.kv = cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(this$__$1.kv,k,v));

return this$__$1;
}));

(datascript.impl.entity.Entity.prototype.cljs$core$IAssociative$_contains_key_QMARK_$arity$2 = (function (e,k){
var e__$1 = this;
return cljs.core.not_EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("logseq.db.common.entity-plus","nf","logseq.db.common.entity-plus/nf",512087449),logseq.db.common.entity_plus.lookup_kv_then_entity.cljs$core$IFn$_invoke$arity$3(e__$1,k,new cljs.core.Keyword("logseq.db.common.entity-plus","nf","logseq.db.common.entity-plus/nf",512087449)));
}));

(datascript.impl.entity.Entity.prototype.cljs$core$IMap$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.impl.entity.Entity.prototype.cljs$core$IMap$_dissoc$arity$2 = (function (this$,k){
var this$__$1 = this;
if((k instanceof cljs.core.Keyword)){
} else {
throw (new Error((""+"Assert failed: "+cljs.core.str.cljs$core$IFn$_invoke$arity$1((""+"attribute must be keyword: "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(k)))+"\n"+"(keyword? k)")));
}

(this$__$1.kv = cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(this$__$1.kv,k));

return this$__$1;
}));

(datascript.impl.entity.Entity.prototype.cljs$core$ISeqable$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.impl.entity.Entity.prototype.cljs$core$ISeqable$_seq$arity$1 = (function (this$){
var this$__$1 = this;
datascript.impl.entity.touch(this$__$1);

return logseq.db.common.entity_plus.cache_with_kv(this$__$1);
}));

(datascript.impl.entity.Entity.prototype.cljs$core$IPrintWithWriter$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.impl.entity.Entity.prototype.cljs$core$IPrintWithWriter$_pr_writer$arity$3 = (function (this$,writer,opts){
var this$__$1 = this;
datascript.impl.entity.touch(this$__$1);

var m0 = cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,logseq.db.common.entity_plus.cache_with_kv(this$__$1));
var m = cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(cljs.core.reduce_kv((function (m,k,v){
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(m,k,logseq.db.common.entity_plus.__GT_printable(v));
}),cljs.core.PersistentArrayMap.EMPTY,m0),new cljs.core.Keyword("db","id","db/id",-1388397098),this$__$1.eid);
return cljs.core._pr_writer(m,writer,opts);
}));

(datascript.impl.entity.Entity.prototype.cljs$core$ICollection$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.impl.entity.Entity.prototype.cljs$core$ICollection$_conj$arity$2 = (function (this$,entry){
var this$__$1 = this;
if(cljs.core.vector_QMARK_(entry)){
var vec__40508 = entry;
var k = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40508,(0),null);
var v = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40508,(1),null);
return this$__$1.cljs$core$IAssociative$_assoc$arity$3(null,k,v);
} else {
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (this$__$2,p__40511){
var vec__40516 = p__40511;
var k = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40516,(0),null);
var v = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40516,(1),null);
return cljs.core._assoc(this$__$2,k,v);
}),this$__$1,entry);
}
}));

(datascript.impl.entity.Entity.prototype.cljs$core$ILookup$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.impl.entity.Entity.prototype.cljs$core$ILookup$_lookup$arity$2 = (function (this$,attr){
var this$__$1 = this;
return logseq.db.common.entity_plus.lookup_kv_then_entity.cljs$core$IFn$_invoke$arity$2(this$__$1,attr);
}));

(datascript.impl.entity.Entity.prototype.cljs$core$ILookup$_lookup$arity$3 = (function (this$,attr,not_found){
var this$__$1 = this;
return logseq.db.common.entity_plus.lookup_kv_then_entity.cljs$core$IFn$_invoke$arity$3(this$__$1,attr,not_found);
}));

//# sourceMappingURL=logseq.db.common.entity_plus.js.map
