import "./cljs_env.js";
import "./cljs.core.js";
import "./datascript.core.js";
goog.provide('logseq.db.common.normalize');
logseq.db.common.normalize.remove_retract_entity_ref = (function logseq$db$common$normalize$remove_retract_entity_ref(db,tx_data){
var retracted = cljs.core.set(cljs.core.keep.cljs$core$IFn$_invoke$arity$2((function (p__50147){
var vec__50148 = p__50147;
var op = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50148,(0),null);
var value = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50148,(1),null);
if(((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(op,new cljs.core.Keyword("db","retractEntity","db/retractEntity",-1452737935))) && (((datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,value) : datascript.core.entity.call(null,db,value)) == null)))){
return value;
} else {
return null;
}
}),tx_data));
var retract_uuids = cljs.core.set(cljs.core.map.cljs$core$IFn$_invoke$arity$2(cljs.core.second,retracted));
var retracted_ids = cljs.core.into.cljs$core$IFn$_invoke$arity$2(retract_uuids,cljs.core.map.cljs$core$IFn$_invoke$arity$2(cljs.core.str,retract_uuids));
if(cljs.core.seq(retracted)){
return cljs.core.remove.cljs$core$IFn$_invoke$arity$2((function (item){
return ((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2((5),cljs.core.count(item))) && (((cljs.core.contains_QMARK_(retracted,cljs.core.second(item))) || (((cljs.core.contains_QMARK_(retracted_ids,cljs.core.second(item))) || (((cljs.core.contains_QMARK_(retracted,cljs.core.nth.cljs$core$IFn$_invoke$arity$2(item,(3)))) || (cljs.core.contains_QMARK_(retracted_ids,cljs.core.nth.cljs$core$IFn$_invoke$arity$2(item,(3)))))))))));
}),tx_data);
} else {
return tx_data;
}
});
logseq.db.common.normalize.replace_attr_retract_with_retract_entity_v2 = (function logseq$db$common$normalize$replace_attr_retract_with_retract_entity_v2(db,normalized_tx_data){
return logseq.db.common.normalize.remove_retract_entity_ref(db,cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (p__50181){
var vec__50182 = p__50181;
var op = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50182,(0),null);
var eid = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50182,(1),null);
var a = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50182,(2),null);
var v = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50182,(3),null);
var t = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50182,(4),null);
if(((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(op,new cljs.core.Keyword("db","retract","db/retract",-1549825231))) && (cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(a,new cljs.core.Keyword("block","uuid","block/uuid",-1991494552))))){
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("db","retractEntity","db/retractEntity",-1452737935),eid], null);
} else {
if(cljs.core.truth_((function (){var and__5140__auto__ = a;
if(cljs.core.truth_(and__5140__auto__)){
return (!((v == null)));
} else {
return and__5140__auto__;
}
})())){
return new cljs.core.PersistentVector(null, 5, 5, cljs.core.PersistentVector.EMPTY_NODE, [op,eid,a,v,t], null);
} else {
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [op,eid], null);

}
}
}),normalized_tx_data));
});
logseq.db.common.normalize.replace_attr_retract_with_retract_entity = (function logseq$db$common$normalize$replace_attr_retract_with_retract_entity(db_after,tx_data){
var e_datoms = cljs.core.sort_by.cljs$core$IFn$_invoke$arity$2(cljs.core.first,cljs.core.group_by(cljs.core.first,tx_data));
return cljs.core.mapcat.cljs$core$IFn$_invoke$arity$variadic((function (p__50222){
var vec__50223 = p__50222;
var _e = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50223,(0),null);
var datoms = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50223,(1),null);
var temp__5802__auto__ = cljs.core.some((function (d){
if(((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("block","uuid","block/uuid",-1991494552),new cljs.core.Keyword(null,"a","a",-2123407586).cljs$core$IFn$_invoke$arity$1(d))) && (((new cljs.core.Keyword(null,"added","added",2057651688).cljs$core$IFn$_invoke$arity$1(d) === false) && (((function (){var G__50228 = db_after;
var G__50229 = new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("block","uuid","block/uuid",-1991494552),new cljs.core.Keyword(null,"v","v",21465059).cljs$core$IFn$_invoke$arity$1(d)], null);
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(G__50228,G__50229) : datascript.core.entity.call(null,G__50228,G__50229));
})() == null)))))){
return d;
} else {
return null;
}
}),datoms);
if(cljs.core.truth_(temp__5802__auto__)){
var d = temp__5802__auto__;
return new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("db","retractEntity","db/retractEntity",-1452737935),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("block","uuid","block/uuid",-1991494552),new cljs.core.Keyword(null,"v","v",21465059).cljs$core$IFn$_invoke$arity$1(d)], null)], null)], null);
} else {
return datoms;
}
}),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([e_datoms], 0));
});
logseq.db.common.normalize.eid__GT_lookup = (function logseq$db$common$normalize$eid__GT_lookup(db,e){
var temp__5804__auto__ = (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,e) : datascript.core.entity.call(null,db,e));
if(cljs.core.truth_(temp__5804__auto__)){
var entity = temp__5804__auto__;
var temp__5802__auto__ = new cljs.core.Keyword("block","uuid","block/uuid",-1991494552).cljs$core$IFn$_invoke$arity$1(entity);
if(cljs.core.truth_(temp__5802__auto__)){
var id = temp__5802__auto__;
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("block","uuid","block/uuid",-1991494552),id], null);
} else {
return new cljs.core.Keyword("db","ident","db/ident",-737096).cljs$core$IFn$_invoke$arity$1(entity);
}
} else {
return null;
}
});
logseq.db.common.normalize.eid__GT_tempid = (function logseq$db$common$normalize$eid__GT_tempid(db,e){
var temp__5804__auto__ = (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,e) : datascript.core.entity.call(null,db,e));
if(cljs.core.truth_(temp__5804__auto__)){
var entity = temp__5804__auto__;
var temp__5804__auto____$1 = (function (){var temp__5802__auto__ = new cljs.core.Keyword("block","uuid","block/uuid",-1991494552).cljs$core$IFn$_invoke$arity$1(entity);
if(cljs.core.truth_(temp__5802__auto__)){
var id = temp__5802__auto__;
return id;
} else {
return new cljs.core.Keyword("db","ident","db/ident",-737096).cljs$core$IFn$_invoke$arity$1(entity);
}
})();
if(cljs.core.truth_(temp__5804__auto____$1)){
var id = temp__5804__auto____$1;
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(id));
} else {
return null;
}
} else {
return null;
}
});
/**
 * Properties first
 */
logseq.db.common.normalize.sort_datoms = (function logseq$db$common$normalize$sort_datoms(datoms){
return cljs.core.sort_by.cljs$core$IFn$_invoke$arity$2((function (item){
var G__50240 = cljs.core.nth.cljs$core$IFn$_invoke$arity$2(item,(1));
var G__50240__$1 = (((G__50240 instanceof cljs.core.Keyword))?G__50240.fqn:null);
switch (G__50240__$1) {
case "db/ident":
return (0);

break;
case "db/valueType":
return (1);

break;
case "db/cardinality":
return (2);

break;
default:
return (3);

}
}),datoms);
});
logseq.db.common.normalize.remove_conflict_datoms = (function logseq$db$common$normalize$remove_conflict_datoms(datoms){
return cljs.core.sort_by.cljs$core$IFn$_invoke$arity$2((function (p1__50248_SHARP_){
return cljs.core.nth.cljs$core$IFn$_invoke$arity$2(p1__50248_SHARP_,(3));
}),cljs.core.keep.cljs$core$IFn$_invoke$arity$2((function (p__50252){
var vec__50254 = p__50252;
var _eavt = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50254,(0),null);
var same_eavt_datoms = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50254,(1),null);
return cljs.core.first(cljs.core.rseq(same_eavt_datoms));
}),cljs.core.group_by((function (d){
return cljs.core.take.cljs$core$IFn$_invoke$arity$2((4),d);
}),datoms)));
});
logseq.db.common.normalize.normalize_tx_data = (function logseq$db$common$normalize$normalize_tx_data(db_after,db_before,tx_data){
return cljs.core.distinct.cljs$core$IFn$_invoke$arity$1(logseq.db.common.normalize.remove_retract_entity_ref(db_after,cljs.core.keep.cljs$core$IFn$_invoke$arity$2((function (d){
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(cljs.core.count(d),(5))){
var vec__50268 = d;
var e = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50268,(0),null);
var a = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50268,(1),null);
var v = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50268,(2),null);
var t = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50268,(3),null);
var added = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50268,(4),null);
var retract_QMARK_ = cljs.core.not(added);
if(((retract_QMARK_) && (cljs.core.contains_QMARK_(new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword("block","updated-at","block/updated-at",-1516550551),null,new cljs.core.Keyword("block","created-at","block/created-at",1440015),null,new cljs.core.Keyword("block","title","block/title",710445684),null], null), null),a)))){
return null;
} else {
var e_SINGLEQUOTE_ = ((retract_QMARK_)?logseq.db.common.normalize.eid__GT_lookup(db_before,e):(function (){var or__5142__auto__ = logseq.db.common.normalize.eid__GT_lookup(db_before,e);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return logseq.db.common.normalize.eid__GT_tempid(db_after,e);
}
})());
var v_SINGLEQUOTE_ = ((((cljs.core.integer_QMARK_(v)) && ((((v > (0))) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db.type","ref","db.type/ref",-1728373079),new cljs.core.Keyword("db","valueType","db/valueType",1827971944).cljs$core$IFn$_invoke$arity$1((datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db_after,a) : datascript.core.entity.call(null,db_after,a))))) || (cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db.type","ref","db.type/ref",-1728373079),new cljs.core.Keyword("db","valueType","db/valueType",1827971944).cljs$core$IFn$_invoke$arity$1((datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db_before,a) : datascript.core.entity.call(null,db_before,a)))))))))))?((retract_QMARK_)?logseq.db.common.normalize.eid__GT_lookup(db_before,v):(function (){var or__5142__auto__ = logseq.db.common.normalize.eid__GT_lookup(db_before,v);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return logseq.db.common.normalize.eid__GT_tempid(db_after,v);
}
})()):v);
if((((!((e_SINGLEQUOTE_ == null)))) && ((!((v_SINGLEQUOTE_ == null)))))){
if(cljs.core.truth_(added)){
return new cljs.core.PersistentVector(null, 5, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("db","add","db/add",235286841),e_SINGLEQUOTE_,a,v_SINGLEQUOTE_,t], null);
} else {
return new cljs.core.PersistentVector(null, 5, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("db","retract","db/retract",-1549825231),e_SINGLEQUOTE_,a,v_SINGLEQUOTE_,t], null);
}
} else {
return null;
}
}
} else {
return d;
}
}),logseq.db.common.normalize.sort_datoms(logseq.db.common.normalize.replace_attr_retract_with_retract_entity(db_after,logseq.db.common.normalize.remove_conflict_datoms(tx_data))))));
});

//# sourceMappingURL=logseq.db.common.normalize.js.map
