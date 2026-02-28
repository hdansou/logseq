import "./cljs_env.js";
import "./cljs.core.js";
import "./clojure.string.js";
import "./datascript.core.js";
import "./datascript.impl.entity.js";
import "./logseq.common.config.js";
import "./logseq.common.util.js";
import "./logseq.common.util.date_time.js";
import "./logseq.db.common.order.js";
import "./logseq.db.frontend.class.js";
import "./logseq.db.frontend.db.js";
import "./logseq.db.frontend.entity_util.js";
import "./logseq.db.frontend.rules.js";
goog.provide('logseq.db.common.initial_data');
/**
 * Return the oldest page's db id for :block/name
 */
logseq.db.common.initial_data.get_first_page_by_name = (function logseq$db$common$initial_data$get_first_page_by_name(db,page_name){
if(cljs.core.truth_((function (){var and__5140__auto__ = db;
if(cljs.core.truth_(and__5140__auto__)){
return typeof page_name === 'string';
} else {
return and__5140__auto__;
}
})())){
return cljs.core.first(cljs.core.sort.cljs$core$IFn$_invoke$arity$1(cljs.core.map.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"e","e",1381269198),logseq.db.frontend.entity_util.get_pages_by_name(db,page_name))));
} else {
return null;
}
});
/**
 * Return the oldest page's db id for :block/title
 */
logseq.db.common.initial_data.get_first_page_by_title = (function logseq$db$common$initial_data$get_first_page_by_title(db,page_name){
if(typeof page_name === 'string'){
} else {
throw (new Error("Assert failed: (string? page-name)"));
}

return cljs.core.first(cljs.core.sort.cljs$core$IFn$_invoke$arity$1(cljs.core.map.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"e","e",1381269198),cljs.core.filter.cljs$core$IFn$_invoke$arity$2((function (d){
var e = (function (){var G__40631 = db;
var G__40632 = new cljs.core.Keyword(null,"e","e",1381269198).cljs$core$IFn$_invoke$arity$1(d);
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(G__40631,G__40632) : datascript.core.entity.call(null,G__40631,G__40632));
})();
return logseq.db.frontend.entity_util.page_QMARK_(e);
}),datascript.core.datoms.cljs$core$IFn$_invoke$arity$4(db,new cljs.core.Keyword(null,"avet","avet",1383857032),new cljs.core.Keyword("block","title","block/title",710445684),page_name)))));
});
logseq.db.common.initial_data.get_block_alias = (function logseq$db$common$initial_data$get_block_alias(db,eid){
return cljs.core.distinct.cljs$core$IFn$_invoke$arity$1((function (){var G__40633 = new cljs.core.PersistentVector(null, 8, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"find","find",496279456),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?e","?e",-1194391683,null),new cljs.core.Symbol(null,"...","...",-1926939749,null)], null),new cljs.core.Keyword(null,"in","in",-1531184865),new cljs.core.Symbol(null,"$","$",-1580747756,null),new cljs.core.Symbol(null,"?eid","?eid",1087837141,null),new cljs.core.Symbol(null,"%","%",-950237169,null),new cljs.core.Keyword(null,"where","where",-2044795965),cljs.core.list(new cljs.core.Symbol(null,"alias","alias",-399220103,null),new cljs.core.Symbol(null,"?eid","?eid",1087837141,null),new cljs.core.Symbol(null,"?e","?e",-1194391683,null))], null);
var G__40634 = db;
var G__40635 = eid;
var G__40636 = new cljs.core.Keyword(null,"alias","alias",-2039751630).cljs$core$IFn$_invoke$arity$1(logseq.db.frontend.rules.rules);
return (datascript.core.q.cljs$core$IFn$_invoke$arity$4 ? datascript.core.q.cljs$core$IFn$_invoke$arity$4(G__40633,G__40634,G__40635,G__40636) : datascript.core.q.call(null,G__40633,G__40634,G__40635,G__40636));
})());
});
logseq.db.common.initial_data.get_all_files = (function logseq$db$common$initial_data$get_all_files(db){
return cljs.core.mapcat.cljs$core$IFn$_invoke$arity$variadic((function (e){
return datascript.core.datoms.cljs$core$IFn$_invoke$arity$3(db,new cljs.core.Keyword(null,"eavt","eavt",-666437073),new cljs.core.Keyword(null,"e","e",1381269198).cljs$core$IFn$_invoke$arity$1(e));
}),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([datascript.core.datoms.cljs$core$IFn$_invoke$arity$3(db,new cljs.core.Keyword(null,"avet","avet",1383857032),new cljs.core.Keyword("file","path","file/path",-191335748))], 0));
});
logseq.db.common.initial_data.with_block_refs = (function logseq$db$common$initial_data$with_block_refs(db,block){
return cljs.core.update.cljs$core$IFn$_invoke$arity$3(block,new cljs.core.Keyword("block","refs","block/refs",-1214495349),(function (refs){
return cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (ref){
var G__40638 = db;
var G__40639 = new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"*","*",345799209,null)], null);
var G__40640 = new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(ref);
return (datascript.core.pull.cljs$core$IFn$_invoke$arity$3 ? datascript.core.pull.cljs$core$IFn$_invoke$arity$3(G__40638,G__40639,G__40640) : datascript.core.pull.call(null,G__40638,G__40639,G__40640));
}),refs);
}));
});
logseq.db.common.initial_data.with_parent = (function logseq$db$common$initial_data$with_parent(db,block){
if(cljs.core.truth_(new cljs.core.Keyword("block","page","block/page",822314108).cljs$core$IFn$_invoke$arity$1(block))){
var parent = (function (){var temp__5804__auto__ = (function (){var G__40646 = db;
var G__40647 = new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(new cljs.core.Keyword("block","parent","block/parent",-918309064).cljs$core$IFn$_invoke$arity$1(block));
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(G__40646,G__40647) : datascript.core.entity.call(null,G__40646,G__40647));
})();
if(cljs.core.truth_(temp__5804__auto__)){
var e = temp__5804__auto__;
return cljs.core.select_keys(e,new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("db","id","db/id",-1388397098),new cljs.core.Keyword("block","uuid","block/uuid",-1991494552)], null));
} else {
return null;
}
})();
return logseq.db.common.initial_data.with_block_refs(db,logseq.common.util.remove_nils_non_nested(cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(block,new cljs.core.Keyword("block","parent","block/parent",-918309064),parent)));
} else {
return block;

}
});
/**
 * Returns children ids, notice the result doesn't include property value children ids.
 */
logseq.db.common.initial_data.get_block_children_ids = (function logseq$db$common$initial_data$get_block_children_ids(var_args){
var args__5882__auto__ = [];
var len__5876__auto___40782 = arguments.length;
var i__5877__auto___40783 = (0);
while(true){
if((i__5877__auto___40783 < len__5876__auto___40782)){
args__5882__auto__.push((arguments[i__5877__auto___40783]));

var G__40784 = (i__5877__auto___40783 + (1));
i__5877__auto___40783 = G__40784;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((2) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((2)),(0),null)):null);
return logseq.db.common.initial_data.get_block_children_ids.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),(arguments[(1)]),argseq__5883__auto__);
});

(logseq.db.common.initial_data.get_block_children_ids.cljs$core$IFn$_invoke$arity$variadic = (function (db,block_eid,p__40654){
var map__40655 = p__40654;
var map__40655__$1 = cljs.core.__destructure_map(map__40655);
var include_collapsed_children_QMARK_ = cljs.core.get.cljs$core$IFn$_invoke$arity$3(map__40655__$1,new cljs.core.Keyword(null,"include-collapsed-children?","include-collapsed-children?",2120012437),true);
var temp__5804__auto__ = new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1((datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,block_eid) : datascript.core.entity.call(null,db,block_eid)));
if(cljs.core.truth_(temp__5804__auto__)){
var eid = temp__5804__auto__;
var seen = cljs.core.volatile_BANG_(cljs.core.PersistentHashSet.EMPTY);
var eids_to_expand_40786 = new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [eid], null);
while(true){
if(cljs.core.seq(eids_to_expand_40786)){
var children_40787 = cljs.core.mapcat.cljs$core$IFn$_invoke$arity$variadic(((function (eids_to_expand_40786,seen,eid,temp__5804__auto__,map__40655,map__40655__$1,include_collapsed_children_QMARK_){
return (function (eid__$1){
var e = (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,eid__$1) : datascript.core.entity.call(null,db,eid__$1));
if(cljs.core.truth_((function (){var or__5142__auto__ = include_collapsed_children_QMARK_;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
var or__5142__auto____$1 = cljs.core.not(new cljs.core.Keyword("block","collapsed?","block/collapsed?",2140210991).cljs$core$IFn$_invoke$arity$1(e));
if(or__5142__auto____$1){
return or__5142__auto____$1;
} else {
return logseq.db.frontend.entity_util.page_QMARK_(e);
}
}
})())){
return new cljs.core.Keyword("block","_parent","block/_parent",-639389670).cljs$core$IFn$_invoke$arity$1(e);
} else {
return null;
}
});})(eids_to_expand_40786,seen,eid,temp__5804__auto__,map__40655,map__40655__$1,include_collapsed_children_QMARK_))
,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([eids_to_expand_40786], 0));
var ids_to_add_40788 = cljs.core.keep.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db","id","db/id",-1388397098),children_40787);
seen.cljs$core$IVolatile$_vreset_BANG_$arity$2(null,cljs.core.partial.cljs$core$IFn$_invoke$arity$2(cljs.core.apply,cljs.core.conj)(seen.cljs$core$IDeref$_deref$arity$1(null),ids_to_add_40788));

var G__40791 = cljs.core.keep.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db","id","db/id",-1388397098),children_40787);
eids_to_expand_40786 = G__40791;
continue;
} else {
}
break;
}

return cljs.core.deref(seen);
} else {
return null;
}
}));

(logseq.db.common.initial_data.get_block_children_ids.cljs$lang$maxFixedArity = (2));

/** @this {Function} */
(logseq.db.common.initial_data.get_block_children_ids.cljs$lang$applyTo = (function (seq40651){
var G__40652 = cljs.core.first(seq40651);
var seq40651__$1 = cljs.core.next(seq40651);
var G__40653 = cljs.core.first(seq40651__$1);
var seq40651__$2 = cljs.core.next(seq40651__$1);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__40652,G__40653,seq40651__$2);
}));

/**
 * Including nested children, notice the result doesn't include property values.
 */
logseq.db.common.initial_data.get_block_children = (function logseq$db$common$initial_data$get_block_children(var_args){
var args__5882__auto__ = [];
var len__5876__auto___40800 = arguments.length;
var i__5877__auto___40801 = (0);
while(true){
if((i__5877__auto___40801 < len__5876__auto___40800)){
args__5882__auto__.push((arguments[i__5877__auto___40801]));

var G__40803 = (i__5877__auto___40801 + (1));
i__5877__auto___40801 = G__40803;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((2) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((2)),(0),null)):null);
return logseq.db.common.initial_data.get_block_children.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),(arguments[(1)]),argseq__5883__auto__);
});

(logseq.db.common.initial_data.get_block_children.cljs$core$IFn$_invoke$arity$variadic = (function (db,eid,p__40662){
var map__40663 = p__40662;
var map__40663__$1 = cljs.core.__destructure_map(map__40663);
var opts = map__40663__$1;
var ids = logseq.db.common.initial_data.get_block_children_ids.cljs$core$IFn$_invoke$arity$variadic(db,eid,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([opts], 0));
if(cljs.core.seq(ids)){
return cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (id){
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,id) : datascript.core.entity.call(null,db,id));
}),ids);
} else {
return null;
}
}));

(logseq.db.common.initial_data.get_block_children.cljs$lang$maxFixedArity = (2));

/** @this {Function} */
(logseq.db.common.initial_data.get_block_children.cljs$lang$applyTo = (function (seq40659){
var G__40660 = cljs.core.first(seq40659);
var seq40659__$1 = cljs.core.next(seq40659);
var G__40661 = cljs.core.first(seq40659__$1);
var seq40659__$2 = cljs.core.next(seq40659__$1);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__40660,G__40661,seq40659__$2);
}));

/**
 * Including nested, collapsed and property value children.
 */
logseq.db.common.initial_data.get_block_full_children_ids = (function logseq$db$common$initial_data$get_block_full_children_ids(db,block_eid){
var G__40665 = new cljs.core.PersistentVector(null, 8, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"find","find",496279456),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?c","?c",870679775,null),new cljs.core.Symbol(null,"...","...",-1926939749,null)], null),new cljs.core.Keyword(null,"in","in",-1531184865),new cljs.core.Symbol(null,"$","$",-1580747756,null),new cljs.core.Symbol(null,"?id","?id",928433279,null),new cljs.core.Symbol(null,"%","%",-950237169,null),new cljs.core.Keyword(null,"where","where",-2044795965),cljs.core.list(new cljs.core.Symbol(null,"parent","parent",761652748,null),new cljs.core.Symbol(null,"?id","?id",928433279,null),new cljs.core.Symbol(null,"?c","?c",870679775,null))], null);
var G__40666 = db;
var G__40667 = block_eid;
var G__40668 = new cljs.core.Keyword(null,"parent","parent",-878878779).cljs$core$IFn$_invoke$arity$1(logseq.db.frontend.rules.rules);
return (datascript.core.q.cljs$core$IFn$_invoke$arity$4 ? datascript.core.q.cljs$core$IFn$_invoke$arity$4(G__40665,G__40666,G__40667,G__40668) : datascript.core.q.call(null,G__40665,G__40666,G__40667,G__40668));
});
logseq.db.common.initial_data.with_raw_title = (function logseq$db$common$initial_data$with_raw_title(m,entity){
var temp__5802__auto__ = new cljs.core.Keyword("block","raw-title","block/raw-title",1833669090).cljs$core$IFn$_invoke$arity$1(entity);
if(cljs.core.truth_(temp__5802__auto__)){
var raw_title = temp__5802__auto__;
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(m,new cljs.core.Keyword("block","title","block/title",710445684),raw_title);
} else {
return m;
}
});
logseq.db.common.initial_data.entity__GT_map = (function logseq$db$common$initial_data$entity__GT_map(var_args){
var args__5882__auto__ = [];
var len__5876__auto___40809 = arguments.length;
var i__5877__auto___40810 = (0);
while(true){
if((i__5877__auto___40810 < len__5876__auto___40809)){
args__5882__auto__.push((arguments[i__5877__auto___40810]));

var G__40811 = (i__5877__auto___40810 + (1));
i__5877__auto___40810 = G__40811;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((1) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((1)),(0),null)):null);
return logseq.db.common.initial_data.entity__GT_map.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),argseq__5883__auto__);
});

(logseq.db.common.initial_data.entity__GT_map.cljs$core$IFn$_invoke$arity$variadic = (function (entity,p__40676){
var map__40677 = p__40676;
var map__40677__$1 = cljs.core.__destructure_map(map__40677);
var level = cljs.core.get.cljs$core$IFn$_invoke$arity$3(map__40677__$1,new cljs.core.Keyword(null,"level","level",1290497552),(0));
var properties = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40677__$1,new cljs.core.Keyword(null,"properties","properties",685819552));
var opts = new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"level","level",1290497552),(level + (1))], null);
var f = (((level > (0)))?cljs.core.identity:(function (e){
return cljs.core.keep.cljs$core$IFn$_invoke$arity$2((function (p__40679){
var vec__40680 = p__40679;
var k = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40680,(0),null);
var v = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40680,(1),null);
if(cljs.core.truth_((function (){var or__5142__auto__ = cljs.core.empty_QMARK_(properties);
if(or__5142__auto__){
return or__5142__auto__;
} else {
return (properties.cljs$core$IFn$_invoke$arity$1 ? properties.cljs$core$IFn$_invoke$arity$1(k) : properties.call(null,k));
}
})())){
var v_SINGLEQUOTE_ = ((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(k,new cljs.core.Keyword("block","parent","block/parent",-918309064)))?new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(v):((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(k,new cljs.core.Keyword("block","tags","block/tags",1814948340)))?cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (p1__40671_SHARP_){
return cljs.core.select_keys(p1__40671_SHARP_,new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("db","id","db/id",-1388397098)], null));
}),v):((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(k,new cljs.core.Keyword("logseq.property","created-by-ref","logseq.property/created-by-ref",854433908)))?new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(v):((datascript.impl.entity.entity_QMARK_(v))?logseq.db.common.initial_data.entity__GT_map.cljs$core$IFn$_invoke$arity$variadic(v,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([opts], 0)):((((cljs.core.coll_QMARK_(v)) && (cljs.core.every_QMARK_(datascript.impl.entity.entity_QMARK_,v))))?cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (p1__40672_SHARP_){
return logseq.db.common.initial_data.entity__GT_map.cljs$core$IFn$_invoke$arity$variadic(p1__40672_SHARP_,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([opts], 0));
}),v):v
)))));
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [k,v_SINGLEQUOTE_], null);
} else {
return null;
}
}),e);
}));
var m = cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,(f.cljs$core$IFn$_invoke$arity$1 ? f.cljs$core$IFn$_invoke$arity$1(entity) : f.call(null,entity)));
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(logseq.db.common.initial_data.with_raw_title(m,entity),new cljs.core.Keyword("db","id","db/id",-1388397098),new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(entity));
}));

(logseq.db.common.initial_data.entity__GT_map.cljs$lang$maxFixedArity = (1));

/** @this {Function} */
(logseq.db.common.initial_data.entity__GT_map.cljs$lang$applyTo = (function (seq40673){
var G__40674 = cljs.core.first(seq40673);
var seq40673__$1 = cljs.core.next(seq40673);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__40674,seq40673__$1);
}));

/**
 * Whether ref-block (for block with the `id`) should be hidden.
 */
logseq.db.common.initial_data.hidden_ref_QMARK_ = (function logseq$db$common$initial_data$hidden_ref_QMARK_(db,ref_block,id){
var entity = (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,id) : datascript.core.entity.call(null,db,id));
var or__5142__auto__ = cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(ref_block),id);
if(or__5142__auto__){
return or__5142__auto__;
} else {
var or__5142__auto____$1 = cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(id,new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(new cljs.core.Keyword("block","page","block/page",822314108).cljs$core$IFn$_invoke$arity$1(ref_block)));
if(or__5142__auto____$1){
return or__5142__auto____$1;
} else {
var or__5142__auto____$2 = cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(id,new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(new cljs.core.Keyword("logseq.property","view-for","logseq.property/view-for",-36274319).cljs$core$IFn$_invoke$arity$1(ref_block)));
if(or__5142__auto____$2){
return or__5142__auto____$2;
} else {
var or__5142__auto____$3 = logseq.db.frontend.entity_util.hidden_QMARK_(new cljs.core.Keyword("block","page","block/page",822314108).cljs$core$IFn$_invoke$arity$1(ref_block));
if(or__5142__auto____$3){
return or__5142__auto____$3;
} else {
var or__5142__auto____$4 = logseq.db.frontend.entity_util.hidden_QMARK_(ref_block);
if(or__5142__auto____$4){
return or__5142__auto____$4;
} else {
var or__5142__auto____$5 = (function (){var and__5140__auto__ = logseq.db.frontend.entity_util.class_QMARK_(entity);
if(cljs.core.truth_(and__5140__auto__)){
var children = logseq.db.frontend.class$.get_structured_children(db,id);
var class_ids = cljs.core.set(cljs.core.conj.cljs$core$IFn$_invoke$arity$2(children,id));
return cljs.core.some(class_ids,cljs.core.map.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db","id","db/id",-1388397098),new cljs.core.Keyword("block","tags","block/tags",1814948340).cljs$core$IFn$_invoke$arity$1(ref_block)));
} else {
return and__5140__auto__;
}
})();
if(cljs.core.truth_(or__5142__auto____$5)){
return or__5142__auto____$5;
} else {
return (!((cljs.core.get.cljs$core$IFn$_invoke$arity$2(ref_block,new cljs.core.Keyword("db","ident","db/ident",-737096).cljs$core$IFn$_invoke$arity$1(entity)) == null)));
}
}
}
}
}
}
});
logseq.db.common.initial_data.get_block_refs = (function logseq$db$common$initial_data$get_block_refs(db,id){
var with_alias = cljs.core.distinct.cljs$core$IFn$_invoke$arity$1(cljs.core.cons(id,logseq.db.common.initial_data.get_block_alias(db,id)));
var G__40685 = with_alias;
var G__40685__$1 = (((G__40685 == null))?null:cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (p1__40684_SHARP_){
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,p1__40684_SHARP_) : datascript.core.entity.call(null,db,p1__40684_SHARP_));
}),G__40685));
var G__40685__$2 = (((G__40685__$1 == null))?null:cljs.core.mapcat.cljs$core$IFn$_invoke$arity$variadic(new cljs.core.Keyword("block","_refs","block/_refs",830218531),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([G__40685__$1], 0)));
if((G__40685__$2 == null)){
return null;
} else {
return cljs.core.remove.cljs$core$IFn$_invoke$arity$2((function (ref_block){
return logseq.db.common.initial_data.hidden_ref_QMARK_(db,ref_block,id);
}),G__40685__$2);
}
});
logseq.db.common.initial_data.get_block_refs_count = (function logseq$db$common$initial_data$get_block_refs_count(db,id){
return cljs.core.count(logseq.db.common.initial_data.get_block_refs(db,id));
});
logseq.db.common.initial_data.get_block_and_children = (function logseq$db$common$initial_data$get_block_and_children(db,id_or_page_name,p__40689){
var map__40690 = p__40689;
var map__40690__$1 = cljs.core.__destructure_map(map__40690);
var children_QMARK_ = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40690__$1,new cljs.core.Keyword(null,"children?","children?",-1199594108));
var properties = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40690__$1,new cljs.core.Keyword(null,"properties","properties",685819552));
var include_collapsed_children_QMARK_ = cljs.core.get.cljs$core$IFn$_invoke$arity$3(map__40690__$1,new cljs.core.Keyword(null,"include-collapsed-children?","include-collapsed-children?",2120012437),false);
var block = (function (){var eid = ((cljs.core.uuid_QMARK_(id_or_page_name))?new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("block","uuid","block/uuid",-1991494552),id_or_page_name], null):((cljs.core.integer_QMARK_(id_or_page_name))?id_or_page_name:null
));
if(cljs.core.truth_(eid)){
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,eid) : datascript.core.entity.call(null,db,eid));
} else {
if(typeof id_or_page_name === 'string'){
var G__40691 = db;
var G__40692 = logseq.db.common.initial_data.get_first_page_by_name(db,cljs.core.name(id_or_page_name));
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(G__40691,G__40692) : datascript.core.entity.call(null,G__40691,G__40692));
} else {
return null;

}
}
})();
var block_refs_count_QMARK_ = cljs.core.some(new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword("block.temp","refs-count","block.temp/refs-count",-598132499),null], null), null),properties);
if(cljs.core.truth_(block)){
var children = (cljs.core.truth_(children_QMARK_)?(function (){var children_blocks = logseq.db.common.initial_data.get_block_children.cljs$core$IFn$_invoke$arity$variadic(db,new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(block),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"include-collapsed-children?","include-collapsed-children?",2120012437),include_collapsed_children_QMARK_], null)], 0));
var large_page_QMARK_ = (cljs.core.count(children_blocks) >= (100));
var children = (function (){var children_SINGLEQUOTE_ = ((large_page_QMARK_)?new cljs.core.Keyword("block","_parent","block/_parent",-639389670).cljs$core$IFn$_invoke$arity$1(block):children_blocks);
return cljs.core.remove.cljs$core$IFn$_invoke$arity$2((function (e){
return new cljs.core.Keyword("block","closed-value-property","block/closed-value-property",1157792813).cljs$core$IFn$_invoke$arity$1(e);
}),children_SINGLEQUOTE_);
})();
var children_ids = cljs.core.set(cljs.core.map.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db","id","db/id",-1388397098),children));
return cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (block__$1){
var collapsed_QMARK_ = new cljs.core.Keyword("block","collapsed?","block/collapsed?",2140210991).cljs$core$IFn$_invoke$arity$1(block__$1);
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$variadic(logseq.db.common.initial_data.entity__GT_map(block__$1),new cljs.core.Keyword("block.temp","has-children?","block.temp/has-children?",935519725),(!((new cljs.core.Keyword("block","_parent","block/_parent",-639389670).cljs$core$IFn$_invoke$arity$1(block__$1) == null))),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword("block.temp","load-status","block.temp/load-status",-1066723049),((((cljs.core.not(collapsed_QMARK_)) && (((((large_page_QMARK_) && (cljs.core.every_QMARK_(children_ids,cljs.core.map.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db","id","db/id",-1388397098),new cljs.core.Keyword("block","_parent","block/_parent",-639389670).cljs$core$IFn$_invoke$arity$1(block__$1)))))) || ((!(large_page_QMARK_)))))))?new cljs.core.Keyword(null,"full","full",436801220):new cljs.core.Keyword(null,"self","self",-1547428899))], 0));
}),children);
})():null);
var block_SINGLEQUOTE_ = (function (){var G__40696 = logseq.db.common.initial_data.entity__GT_map.cljs$core$IFn$_invoke$arity$variadic(block,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"properties","properties",685819552),cljs.core.set(properties)], null)], 0));
var G__40696__$1 = (cljs.core.truth_(block_refs_count_QMARK_)?cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__40696,new cljs.core.Keyword("block.temp","refs-count","block.temp/refs-count",-598132499),logseq.db.common.initial_data.get_block_refs_count(db,new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(block))):G__40696);
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__40696__$1,new cljs.core.Keyword("block.temp","load-status","block.temp/load-status",-1066723049),(cljs.core.truth_((function (){var and__5140__auto__ = children_QMARK_;
if(cljs.core.truth_(and__5140__auto__)){
var and__5140__auto____$1 = include_collapsed_children_QMARK_;
if(cljs.core.truth_(and__5140__auto____$1)){
return cljs.core.empty_QMARK_(properties);
} else {
return and__5140__auto____$1;
}
} else {
return and__5140__auto__;
}
})())?new cljs.core.Keyword(null,"full","full",436801220):(cljs.core.truth_((function (){var and__5140__auto__ = children_QMARK_;
if(cljs.core.truth_(and__5140__auto__)){
return cljs.core.empty_QMARK_(properties);
} else {
return and__5140__auto__;
}
})())?new cljs.core.Keyword(null,"children","children",-940561982):new cljs.core.Keyword(null,"self","self",-1547428899)
)));

})();
var G__40699 = new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"block","block",664686210),block_SINGLEQUOTE_], null);
if(cljs.core.truth_(children_QMARK_)){
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__40699,new cljs.core.Keyword(null,"children","children",-940561982),children);
} else {
return G__40699;
}
} else {
return null;
}
});
logseq.db.common.initial_data.get_latest_journals = (function logseq$db$common$initial_data$get_latest_journals(db){
var today = logseq.common.util.date_time.date__GT_int((new Date()));
return cljs.core.keep.cljs$core$IFn$_invoke$arity$2((function (d){
if((new cljs.core.Keyword(null,"v","v",21465059).cljs$core$IFn$_invoke$arity$1(d) <= today)){
var e = (function (){var G__40705 = db;
var G__40706 = new cljs.core.Keyword(null,"e","e",1381269198).cljs$core$IFn$_invoke$arity$1(d);
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(G__40705,G__40706) : datascript.core.entity.call(null,G__40705,G__40706));
})();
if(cljs.core.truth_((function (){var and__5140__auto__ = logseq.db.frontend.entity_util.journal_QMARK_(e);
if(cljs.core.truth_(and__5140__auto__)){
return new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(e);
} else {
return and__5140__auto__;
}
})())){
return e;
} else {
return null;
}
} else {
return null;
}
}),cljs.core.rseq(cljs.core.vec(datascript.core.datoms.cljs$core$IFn$_invoke$arity$3(db,new cljs.core.Keyword(null,"avet","avet",1383857032),new cljs.core.Keyword("block","journal-day","block/journal-day",-145748366)))));
});
logseq.db.common.initial_data.get_structured_datoms = (function logseq$db$common$initial_data$get_structured_datoms(db){
var class_property_id = new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1((datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,new cljs.core.Keyword("logseq.class","Property","logseq.class/Property",1038767048)) : datascript.core.entity.call(null,db,new cljs.core.Keyword("logseq.class","Property","logseq.class/Property",1038767048))));
return cljs.core.mapcat.cljs$core$IFn$_invoke$arity$variadic((function (d){
var block_datoms = datascript.core.datoms.cljs$core$IFn$_invoke$arity$3(db,new cljs.core.Keyword(null,"eavt","eavt",-666437073),new cljs.core.Keyword(null,"e","e",1381269198).cljs$core$IFn$_invoke$arity$1(d));
var properties_of_property_datoms = ((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"v","v",21465059).cljs$core$IFn$_invoke$arity$1(d),class_property_id))?(function (){var temp__5804__auto__ = new cljs.core.Keyword("logseq.property","default-value","logseq.property/default-value",-892079662).cljs$core$IFn$_invoke$arity$1((function (){var G__40707 = db;
var G__40708 = new cljs.core.Keyword(null,"e","e",1381269198).cljs$core$IFn$_invoke$arity$1(d);
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(G__40707,G__40708) : datascript.core.entity.call(null,G__40707,G__40708));
})());
if(cljs.core.truth_(temp__5804__auto__)){
var desc = temp__5804__auto__;
return datascript.core.datoms.cljs$core$IFn$_invoke$arity$3(db,new cljs.core.Keyword(null,"eavt","eavt",-666437073),new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(desc));
} else {
return null;
}
})():null);
if(cljs.core.seq(properties_of_property_datoms)){
return cljs.core.concat.cljs$core$IFn$_invoke$arity$2(block_datoms,properties_of_property_datoms);
} else {
return block_datoms;
}
}),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([cljs.core.concat.cljs$core$IFn$_invoke$arity$variadic(datascript.core.datoms.cljs$core$IFn$_invoke$arity$4(db,new cljs.core.Keyword(null,"avet","avet",1383857032),new cljs.core.Keyword("block","tags","block/tags",1814948340),new cljs.core.Keyword("logseq.class","Tag","logseq.class/Tag",-538902083)),datascript.core.datoms.cljs$core$IFn$_invoke$arity$4(db,new cljs.core.Keyword(null,"avet","avet",1383857032),new cljs.core.Keyword("block","tags","block/tags",1814948340),new cljs.core.Keyword("logseq.class","Property","logseq.class/Property",1038767048)),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([datascript.core.datoms.cljs$core$IFn$_invoke$arity$3(db,new cljs.core.Keyword(null,"avet","avet",1383857032),new cljs.core.Keyword("block","closed-value-property","block/closed-value-property",1157792813))], 0))], 0));
});
/**
 * Favorites page and its blocks
 */
logseq.db.common.initial_data.get_favorites = (function logseq$db$common$initial_data$get_favorites(db){
var page_id = logseq.db.common.initial_data.get_first_page_by_name(db,logseq.common.config.favorites_page_name);
var block = (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,page_id) : datascript.core.entity.call(null,db,page_id));
var children = new cljs.core.Keyword("block","_page","block/_page",1150043350).cljs$core$IFn$_invoke$arity$1(block);
if(cljs.core.truth_(block)){
return cljs.core.concat.cljs$core$IFn$_invoke$arity$variadic(datascript.core.datoms.cljs$core$IFn$_invoke$arity$3(db,new cljs.core.Keyword(null,"eavt","eavt",-666437073),new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(block)),cljs.core.mapcat.cljs$core$IFn$_invoke$arity$variadic((function (l){
return datascript.core.datoms.cljs$core$IFn$_invoke$arity$3(db,new cljs.core.Keyword(null,"eavt","eavt",-666437073),new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(l));
}),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([cljs.core.keep.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("block","link","block/link",-1872399993),children)], 0)),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([cljs.core.mapcat.cljs$core$IFn$_invoke$arity$variadic((function (child){
return datascript.core.datoms.cljs$core$IFn$_invoke$arity$3(db,new cljs.core.Keyword(null,"eavt","eavt",-666437073),new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(child));
}),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([children], 0))], 0));
} else {
return null;
}
});
logseq.db.common.initial_data.get_recent_updated_pages = (function logseq$db$common$initial_data$get_recent_updated_pages(db){
if(cljs.core.truth_(db)){
var G__40736 = datascript.core.datoms.cljs$core$IFn$_invoke$arity$3(db,new cljs.core.Keyword(null,"avet","avet",1383857032),new cljs.core.Keyword("block","updated-at","block/updated-at",-1516550551));
var G__40736__$1 = (((G__40736 == null))?null:cljs.core.rseq(G__40736));
var G__40736__$2 = (((G__40736__$1 == null))?null:cljs.core.keep.cljs$core$IFn$_invoke$arity$2((function (datom){
var page = cljs.core.first(datascript.core.datoms.cljs$core$IFn$_invoke$arity$4(db,new cljs.core.Keyword(null,"eavt","eavt",-666437073),new cljs.core.Keyword(null,"e","e",1381269198).cljs$core$IFn$_invoke$arity$1(datom),new cljs.core.Keyword("block","page","block/page",822314108)));
if(cljs.core.truth_((function (){var or__5142__auto__ = page;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
var title = new cljs.core.Keyword(null,"v","v",21465059).cljs$core$IFn$_invoke$arity$1(cljs.core.first(datascript.core.datoms.cljs$core$IFn$_invoke$arity$4(db,new cljs.core.Keyword(null,"eavt","eavt",-666437073),new cljs.core.Keyword(null,"e","e",1381269198).cljs$core$IFn$_invoke$arity$1(datom),new cljs.core.Keyword("block","title","block/title",710445684))));
return clojure.string.blank_QMARK_(title);
}
})())){
return null;
} else {
var e = (function (){var G__40740 = db;
var G__40741 = new cljs.core.Keyword(null,"e","e",1381269198).cljs$core$IFn$_invoke$arity$1(datom);
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(G__40740,G__40741) : datascript.core.entity.call(null,G__40740,G__40741));
})();
if(cljs.core.truth_((function (){var and__5140__auto__ = logseq.db.frontend.entity_util.page_QMARK_(e);
if(cljs.core.truth_(and__5140__auto__)){
return (!(logseq.db.frontend.entity_util.hidden_QMARK_(e)));
} else {
return and__5140__auto__;
}
})())){
return e;
} else {
return null;
}
}
}),G__40736__$1));
if((G__40736__$2 == null)){
return null;
} else {
return cljs.core.take.cljs$core$IFn$_invoke$arity$2((15),G__40736__$2);
}
} else {
return null;
}
});
logseq.db.common.initial_data.get_all_user_datoms = (function logseq$db$common$initial_data$get_all_user_datoms(db){
if(cljs.core.truth_((datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,new cljs.core.Keyword("logseq.property.user","email","logseq.property.user/email",-1655206063)) : datascript.core.entity.call(null,db,new cljs.core.Keyword("logseq.property.user","email","logseq.property.user/email",-1655206063))))){
return cljs.core.mapcat.cljs$core$IFn$_invoke$arity$variadic((function (d){
return datascript.core.datoms.cljs$core$IFn$_invoke$arity$3(db,new cljs.core.Keyword(null,"eavt","eavt",-666437073),new cljs.core.Keyword(null,"e","e",1381269198).cljs$core$IFn$_invoke$arity$1(d));
}),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([datascript.core.datoms.cljs$core$IFn$_invoke$arity$3(db,new cljs.core.Keyword(null,"avet","avet",1383857032),new cljs.core.Keyword("logseq.property.user","email","logseq.property.user/email",-1655206063))], 0));
} else {
return null;
}
});
/**
 * Returns current database schema and initial data
 */
logseq.db.common.initial_data.get_initial_data = (function logseq$db$common$initial_data$get_initial_data(db){
var _ = cljs.core.reset_BANG_(logseq.db.common.order._STAR_max_key,logseq.db.common.order.get_max_order(db));
var schema = new cljs.core.Keyword(null,"schema","schema",-1582001791).cljs$core$IFn$_invoke$arity$1(db);
var idents = cljs.core.mapcat.cljs$core$IFn$_invoke$arity$variadic((function (id){
var temp__5804__auto__ = (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,id) : datascript.core.entity.call(null,db,id));
if(cljs.core.truth_(temp__5804__auto__)){
var e = temp__5804__auto__;
return datascript.core.datoms.cljs$core$IFn$_invoke$arity$3(db,new cljs.core.Keyword(null,"eavt","eavt",-666437073),new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(e));
} else {
return null;
}
}),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.PersistentVector(null, 9, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("logseq.kv","db-type","logseq.kv/db-type",1106888767),new cljs.core.Keyword("logseq.kv","schema-version","logseq.kv/schema-version",-1467606676),new cljs.core.Keyword("logseq.kv","graph-uuid","logseq.kv/graph-uuid",339522676),new cljs.core.Keyword("logseq.kv","local-graph-uuid","logseq.kv/local-graph-uuid",1789980105),new cljs.core.Keyword("logseq.kv","graph-rtc-e2ee?","logseq.kv/graph-rtc-e2ee?",1988827753),new cljs.core.Keyword("logseq.kv","latest-code-lang","logseq.kv/latest-code-lang",-73570328),new cljs.core.Keyword("logseq.kv","graph-backup-folder","logseq.kv/graph-backup-folder",-1042314532),new cljs.core.Keyword("logseq.kv","graph-text-embedding-model-name","logseq.kv/graph-text-embedding-model-name",-342634885),new cljs.core.Keyword("logseq.property","empty-placeholder","logseq.property/empty-placeholder",-1595021837)], null)], 0));
var favorites = logseq.db.common.initial_data.get_favorites(db);
var recent_updated_pages = (function (){var pages = logseq.db.common.initial_data.get_recent_updated_pages(db);
return cljs.core.mapcat.cljs$core$IFn$_invoke$arity$variadic((function (p){
return datascript.core.datoms.cljs$core$IFn$_invoke$arity$3(db,new cljs.core.Keyword(null,"eavt","eavt",-666437073),new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(p));
}),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([pages], 0));
})();
var all_files = logseq.db.common.initial_data.get_all_files(db);
var structured_datoms = logseq.db.common.initial_data.get_structured_datoms(db);
var user_datoms = logseq.db.common.initial_data.get_all_user_datoms(db);
var pages_datoms = (function (){var contents_id = logseq.db.common.initial_data.get_first_page_by_title(db,"Contents");
var capture_page_id = new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(logseq.db.frontend.db.get_built_in_page(db,logseq.common.config.quick_add_page_name));
var views_id = logseq.db.common.initial_data.get_first_page_by_title(db,logseq.common.config.views_page_name);
return cljs.core.mapcat.cljs$core$IFn$_invoke$arity$variadic((function (p1__40760_SHARP_){
return datascript.core.datoms.cljs$core$IFn$_invoke$arity$3(db,new cljs.core.Keyword(null,"eavt","eavt",-666437073),p1__40760_SHARP_);
}),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([cljs.core.remove.cljs$core$IFn$_invoke$arity$2(cljs.core.nil_QMARK_,new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [contents_id,capture_page_id,views_id], null))], 0));
})();
var data = cljs.core.remove.cljs$core$IFn$_invoke$arity$2((function (d){
return cljs.core.contains_QMARK_(new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 4, [new cljs.core.Keyword("block","tx-id","block/tx-id",547556161),null,new cljs.core.Keyword("block","updated-at","block/updated-at",-1516550551),null,new cljs.core.Keyword("block","created-at","block/created-at",1440015),null,new cljs.core.Keyword("logseq.property","created-by-ref","logseq.property/created-by-ref",854433908),null], null), null),new cljs.core.Keyword(null,"a","a",-2123407586).cljs$core$IFn$_invoke$arity$1(d));
}),cljs.core.distinct.cljs$core$IFn$_invoke$arity$1(cljs.core.concat.cljs$core$IFn$_invoke$arity$variadic(idents,structured_datoms,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([user_datoms,favorites,recent_updated_pages,all_files,pages_datoms], 0))));
return new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"schema","schema",-1582001791),schema,new cljs.core.Keyword(null,"initial-data","initial-data",-1315709804),data], null);
});

//# sourceMappingURL=logseq.db.common.initial_data.js.map
