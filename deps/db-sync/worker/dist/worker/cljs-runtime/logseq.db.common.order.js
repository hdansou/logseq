import "./cljs_env.js";
import "./cljs.core.js";
import "./logseq.clj_fractional_indexing.js";
import "./datascript.core.js";
goog.provide('logseq.db.common.order');
if((typeof logseq !== 'undefined') && (typeof logseq.db !== 'undefined') && (typeof logseq.db.common !== 'undefined') && (typeof logseq.db.common.order !== 'undefined') && (typeof logseq.db.common.order._STAR_max_key !== 'undefined')){
} else {
logseq.db.common.order._STAR_max_key = cljs.core.atom.cljs$core$IFn$_invoke$arity$1(null);
}
logseq.db.common.order.reset_max_key_BANG_ = (function logseq$db$common$order$reset_max_key_BANG_(var_args){
var G__47799 = arguments.length;
switch (G__47799) {
case 1:
return logseq.db.common.order.reset_max_key_BANG_.cljs$core$IFn$_invoke$arity$1((arguments[(0)]));

break;
case 2:
return logseq.db.common.order.reset_max_key_BANG_.cljs$core$IFn$_invoke$arity$2((arguments[(0)]),(arguments[(1)]));

break;
default:
throw (new Error(["Invalid arity: ",arguments.length].join("")));

}
});

(logseq.db.common.order.reset_max_key_BANG_.cljs$core$IFn$_invoke$arity$1 = (function (key){
return logseq.db.common.order.reset_max_key_BANG_.cljs$core$IFn$_invoke$arity$2(logseq.db.common.order._STAR_max_key,key);
}));

(logseq.db.common.order.reset_max_key_BANG_.cljs$core$IFn$_invoke$arity$2 = (function (max_key_atom,key){
if(cljs.core.truth_((function (){var and__5140__auto__ = key;
if(cljs.core.truth_(and__5140__auto__)){
return (((cljs.core.deref(max_key_atom) == null)) || ((cljs.core.compare(key,cljs.core.deref(max_key_atom)) > (0))));
} else {
return and__5140__auto__;
}
})())){
return cljs.core.reset_BANG_(max_key_atom,key);
} else {
return null;
}
}));

(logseq.db.common.order.reset_max_key_BANG_.cljs$lang$maxFixedArity = 2);

logseq.db.common.order.gen_key = (function logseq$db$common$order$gen_key(var_args){
var G__47808 = arguments.length;
switch (G__47808) {
case 0:
return logseq.db.common.order.gen_key.cljs$core$IFn$_invoke$arity$0();

break;
case 1:
return logseq.db.common.order.gen_key.cljs$core$IFn$_invoke$arity$1((arguments[(0)]));

break;
default:
var args_arr__5901__auto__ = [];
var len__5876__auto___47837 = arguments.length;
var i__5877__auto___47838 = (0);
while(true){
if((i__5877__auto___47838 < len__5876__auto___47837)){
args_arr__5901__auto__.push((arguments[i__5877__auto___47838]));

var G__47839 = (i__5877__auto___47838 + (1));
i__5877__auto___47838 = G__47839;
continue;
} else {
}
break;
}

var argseq__5902__auto__ = ((((2) < args_arr__5901__auto__.length))?(new cljs.core.IndexedSeq(args_arr__5901__auto__.slice((2)),(0),null)):null);
return logseq.db.common.order.gen_key.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),(arguments[(1)]),argseq__5902__auto__);

}
});

(logseq.db.common.order.gen_key.cljs$core$IFn$_invoke$arity$0 = (function (){
return logseq.db.common.order.gen_key(cljs.core.deref(logseq.db.common.order._STAR_max_key),null);
}));

(logseq.db.common.order.gen_key.cljs$core$IFn$_invoke$arity$1 = (function (end){
return logseq.db.common.order.gen_key(cljs.core.deref(logseq.db.common.order._STAR_max_key),end);
}));

(logseq.db.common.order.gen_key.cljs$core$IFn$_invoke$arity$variadic = (function (start,end,p__47818){
var map__47819 = p__47818;
var map__47819__$1 = cljs.core.__destructure_map(map__47819);
var max_key_atom = cljs.core.get.cljs$core$IFn$_invoke$arity$3(map__47819__$1,new cljs.core.Keyword(null,"max-key-atom","max-key-atom",-1459331700),logseq.db.common.order._STAR_max_key);
var k = logseq.clj_fractional_indexing.generate_key_between(start,end);
logseq.db.common.order.reset_max_key_BANG_.cljs$core$IFn$_invoke$arity$2(max_key_atom,k);

return k;
}));

/** @this {Function} */
(logseq.db.common.order.gen_key.cljs$lang$applyTo = (function (seq47803){
var G__47804 = cljs.core.first(seq47803);
var seq47803__$1 = cljs.core.next(seq47803);
var G__47805 = cljs.core.first(seq47803__$1);
var seq47803__$2 = cljs.core.next(seq47803__$1);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__47804,G__47805,seq47803__$2);
}));

(logseq.db.common.order.gen_key.cljs$lang$maxFixedArity = (2));

logseq.db.common.order.get_max_order = (function logseq$db$common$order$get_max_order(db){
return new cljs.core.Keyword(null,"v","v",21465059).cljs$core$IFn$_invoke$arity$1(cljs.core.first(datascript.core.rseek_datoms.cljs$core$IFn$_invoke$arity$3(db,new cljs.core.Keyword(null,"avet","avet",1383857032),new cljs.core.Keyword("block","order","block/order",-1429282437))));
});
logseq.db.common.order.gen_n_keys = (function logseq$db$common$order$gen_n_keys(var_args){
var args__5882__auto__ = [];
var len__5876__auto___47840 = arguments.length;
var i__5877__auto___47841 = (0);
while(true){
if((i__5877__auto___47841 < len__5876__auto___47840)){
args__5882__auto__.push((arguments[i__5877__auto___47841]));

var G__47842 = (i__5877__auto___47841 + (1));
i__5877__auto___47841 = G__47842;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((3) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((3)),(0),null)):null);
return logseq.db.common.order.gen_n_keys.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),(arguments[(1)]),(arguments[(2)]),argseq__5883__auto__);
});

(logseq.db.common.order.gen_n_keys.cljs$core$IFn$_invoke$arity$variadic = (function (n,start,end,p__47826){
var map__47827 = p__47826;
var map__47827__$1 = cljs.core.__destructure_map(map__47827);
var max_key_atom = cljs.core.get.cljs$core$IFn$_invoke$arity$3(map__47827__$1,new cljs.core.Keyword(null,"max-key-atom","max-key-atom",-1459331700),logseq.db.common.order._STAR_max_key);
var ks = logseq.clj_fractional_indexing.generate_n_keys_between(start,end,n);
logseq.db.common.order.reset_max_key_BANG_.cljs$core$IFn$_invoke$arity$2(max_key_atom,cljs.core.last(ks));

return ks;
}));

(logseq.db.common.order.gen_n_keys.cljs$lang$maxFixedArity = (3));

/** @this {Function} */
(logseq.db.common.order.gen_n_keys.cljs$lang$applyTo = (function (seq47822){
var G__47823 = cljs.core.first(seq47822);
var seq47822__$1 = cljs.core.next(seq47822);
var G__47824 = cljs.core.first(seq47822__$1);
var seq47822__$2 = cljs.core.next(seq47822__$1);
var G__47825 = cljs.core.first(seq47822__$2);
var seq47822__$3 = cljs.core.next(seq47822__$2);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__47823,G__47824,G__47825,seq47822__$3);
}));

logseq.db.common.order.validate_order_key_QMARK_ = (function logseq$db$common$order$validate_order_key_QMARK_(key){
logseq.clj_fractional_indexing.validate_order_key(key,logseq.clj_fractional_indexing.base_62_digits);

return true;
});
logseq.db.common.order.get_prev_order = (function logseq$db$common$order$get_prev_order(db,property,value_id){
var value = (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,value_id) : datascript.core.entity.call(null,db,value_id));
if(cljs.core.truth_(property)){
var values = cljs.core.reverse(new cljs.core.Keyword("property","closed-values","property/closed-values",1261280952).cljs$core$IFn$_invoke$arity$1(property));
return cljs.core.some((function (e){
if((((cljs.core.compare(new cljs.core.Keyword("block","order","block/order",-1429282437).cljs$core$IFn$_invoke$arity$1(e),new cljs.core.Keyword("block","order","block/order",-1429282437).cljs$core$IFn$_invoke$arity$1(value)) < (0))) && (cljs.core.not_EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(e),new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(value))))){
return new cljs.core.Keyword("block","order","block/order",-1429282437).cljs$core$IFn$_invoke$arity$1(e);
} else {
return null;
}
}),values);
} else {
var properties = cljs.core.reverse(cljs.core.sort_by.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("block","order","block/order",-1429282437),cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (d){
var G__47831 = db;
var G__47832 = new cljs.core.Keyword(null,"e","e",1381269198).cljs$core$IFn$_invoke$arity$1(d);
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(G__47831,G__47832) : datascript.core.entity.call(null,G__47831,G__47832));
}),datascript.core.datoms.cljs$core$IFn$_invoke$arity$4(db,new cljs.core.Keyword(null,"avet","avet",1383857032),new cljs.core.Keyword("block","tags","block/tags",1814948340),new cljs.core.Keyword("logseq.class","Property","logseq.class/Property",1038767048)))));
return cljs.core.some((function (property__$1){
if((((cljs.core.compare(new cljs.core.Keyword("block","order","block/order",-1429282437).cljs$core$IFn$_invoke$arity$1(property__$1),new cljs.core.Keyword("block","order","block/order",-1429282437).cljs$core$IFn$_invoke$arity$1(value)) < (0))) && (cljs.core.not_EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(property__$1),new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(value))))){
return new cljs.core.Keyword("block","order","block/order",-1429282437).cljs$core$IFn$_invoke$arity$1(property__$1);
} else {
return null;
}
}),properties);
}
});
logseq.db.common.order.get_next_order = (function logseq$db$common$order$get_next_order(db,property,value_id){
var value = (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,value_id) : datascript.core.entity.call(null,db,value_id));
if(cljs.core.truth_(property)){
var values = new cljs.core.Keyword("property","closed-values","property/closed-values",1261280952).cljs$core$IFn$_invoke$arity$1(property);
return cljs.core.some((function (e){
if((((cljs.core.compare(new cljs.core.Keyword("block","order","block/order",-1429282437).cljs$core$IFn$_invoke$arity$1(e),new cljs.core.Keyword("block","order","block/order",-1429282437).cljs$core$IFn$_invoke$arity$1(value)) > (0))) && (cljs.core.not_EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(e),new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(value))))){
return new cljs.core.Keyword("block","order","block/order",-1429282437).cljs$core$IFn$_invoke$arity$1(e);
} else {
return null;
}
}),values);
} else {
var properties = cljs.core.sort_by.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("block","order","block/order",-1429282437),cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (d){
var G__47833 = db;
var G__47834 = new cljs.core.Keyword(null,"e","e",1381269198).cljs$core$IFn$_invoke$arity$1(d);
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(G__47833,G__47834) : datascript.core.entity.call(null,G__47833,G__47834));
}),datascript.core.datoms.cljs$core$IFn$_invoke$arity$4(db,new cljs.core.Keyword(null,"avet","avet",1383857032),new cljs.core.Keyword("block","tags","block/tags",1814948340),new cljs.core.Keyword("logseq.class","Property","logseq.class/Property",1038767048))));
return cljs.core.some((function (property__$1){
if((((cljs.core.compare(new cljs.core.Keyword("block","order","block/order",-1429282437).cljs$core$IFn$_invoke$arity$1(property__$1),new cljs.core.Keyword("block","order","block/order",-1429282437).cljs$core$IFn$_invoke$arity$1(value)) > (0))) && (cljs.core.not_EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(property__$1),new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(value))))){
return new cljs.core.Keyword("block","order","block/order",-1429282437).cljs$core$IFn$_invoke$arity$1(property__$1);
} else {
return null;
}
}),properties);
}
});

//# sourceMappingURL=logseq.db.common.order.js.map
