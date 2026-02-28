import "./cljs_env.js";
import "./cljs.core.js";
import "./clojure.string.js";
import "./datascript.db.js";
import "./datascript.impl.entity.js";
import "./datascript.util.js";
goog.provide('datascript.built_ins');
datascript.built_ins._differ_QMARK_ = (function datascript$built_ins$_differ_QMARK_(var_args){
var args__5882__auto__ = [];
var len__5876__auto___40185 = arguments.length;
var i__5877__auto___40186 = (0);
while(true){
if((i__5877__auto___40186 < len__5876__auto___40185)){
args__5882__auto__.push((arguments[i__5877__auto___40186]));

var G__40187 = (i__5877__auto___40186 + (1));
i__5877__auto___40186 = G__40187;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((0) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((0)),(0),null)):null);
return datascript.built_ins._differ_QMARK_.cljs$core$IFn$_invoke$arity$variadic(argseq__5883__auto__);
});

(datascript.built_ins._differ_QMARK_.cljs$core$IFn$_invoke$arity$variadic = (function (xs){
var l = cljs.core.count(xs);
return cljs.core.not_EQ_.cljs$core$IFn$_invoke$arity$2(cljs.core.take.cljs$core$IFn$_invoke$arity$2((l / (2)),xs),cljs.core.drop.cljs$core$IFn$_invoke$arity$2((l / (2)),xs));
}));

(datascript.built_ins._differ_QMARK_.cljs$lang$maxFixedArity = (0));

/** @this {Function} */
(datascript.built_ins._differ_QMARK_.cljs$lang$applyTo = (function (seq40025){
var self__5862__auto__ = this;
return self__5862__auto__.cljs$core$IFn$_invoke$arity$variadic(cljs.core.seq(seq40025));
}));

datascript.built_ins._get_else = (function datascript$built_ins$_get_else(db,e,a,else_val){
if((else_val == null)){
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2((""+"get-else: nil default value is not supported"),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"error","error",-978969032),new cljs.core.Keyword("query","where","query/where",-1935159429)], null));
} else {
}

var temp__5806__auto__ = cljs.core.first(datascript.db._search(db,new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [datascript.db.entid(db,e),a], null)));
if((temp__5806__auto__ == null)){
return else_val;
} else {
var datom = temp__5806__auto__;
return new cljs.core.Keyword(null,"v","v",21465059).cljs$core$IFn$_invoke$arity$1(datom);
}
});
datascript.built_ins._get_some = (function datascript$built_ins$_get_some(var_args){
var args__5882__auto__ = [];
var len__5876__auto___40188 = arguments.length;
var i__5877__auto___40189 = (0);
while(true){
if((i__5877__auto___40189 < len__5876__auto___40188)){
args__5882__auto__.push((arguments[i__5877__auto___40189]));

var G__40190 = (i__5877__auto___40189 + (1));
i__5877__auto___40189 = G__40190;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((2) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((2)),(0),null)):null);
return datascript.built_ins._get_some.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),(arguments[(1)]),argseq__5883__auto__);
});

(datascript.built_ins._get_some.cljs$core$IFn$_invoke$arity$variadic = (function (db,e,as){
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (_,a){
var temp__5808__auto__ = cljs.core.first(datascript.db._search(db,new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [datascript.db.entid(db,e),a], null)));
if((temp__5808__auto__ == null)){
return null;
} else {
var datom = temp__5808__auto__;
return cljs.core.reduced(new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"a","a",-2123407586).cljs$core$IFn$_invoke$arity$1(datom),new cljs.core.Keyword(null,"v","v",21465059).cljs$core$IFn$_invoke$arity$1(datom)], null));
}
}),null,as);
}));

(datascript.built_ins._get_some.cljs$lang$maxFixedArity = (2));

/** @this {Function} */
(datascript.built_ins._get_some.cljs$lang$applyTo = (function (seq40036){
var G__40037 = cljs.core.first(seq40036);
var seq40036__$1 = cljs.core.next(seq40036);
var G__40038 = cljs.core.first(seq40036__$1);
var seq40036__$2 = cljs.core.next(seq40036__$1);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__40037,G__40038,seq40036__$2);
}));

datascript.built_ins._missing_QMARK_ = (function datascript$built_ins$_missing_QMARK_(db,e,a){
return (cljs.core.get.cljs$core$IFn$_invoke$arity$2(datascript.impl.entity.entity(db,e),a) == null);
});
datascript.built_ins.and_fn = (function datascript$built_ins$and_fn(var_args){
var args__5882__auto__ = [];
var len__5876__auto___40191 = arguments.length;
var i__5877__auto___40192 = (0);
while(true){
if((i__5877__auto___40192 < len__5876__auto___40191)){
args__5882__auto__.push((arguments[i__5877__auto___40192]));

var G__40193 = (i__5877__auto___40192 + (1));
i__5877__auto___40192 = G__40193;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((0) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((0)),(0),null)):null);
return datascript.built_ins.and_fn.cljs$core$IFn$_invoke$arity$variadic(argseq__5883__auto__);
});

(datascript.built_ins.and_fn.cljs$core$IFn$_invoke$arity$variadic = (function (args){
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (a,b){
if(cljs.core.truth_(b)){
return b;
} else {
return cljs.core.reduced(b);
}
}),true,args);
}));

(datascript.built_ins.and_fn.cljs$lang$maxFixedArity = (0));

/** @this {Function} */
(datascript.built_ins.and_fn.cljs$lang$applyTo = (function (seq40064){
var self__5862__auto__ = this;
return self__5862__auto__.cljs$core$IFn$_invoke$arity$variadic(cljs.core.seq(seq40064));
}));

datascript.built_ins.or_fn = (function datascript$built_ins$or_fn(var_args){
var args__5882__auto__ = [];
var len__5876__auto___40194 = arguments.length;
var i__5877__auto___40195 = (0);
while(true){
if((i__5877__auto___40195 < len__5876__auto___40194)){
args__5882__auto__.push((arguments[i__5877__auto___40195]));

var G__40196 = (i__5877__auto___40195 + (1));
i__5877__auto___40195 = G__40196;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((0) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((0)),(0),null)):null);
return datascript.built_ins.or_fn.cljs$core$IFn$_invoke$arity$variadic(argseq__5883__auto__);
});

(datascript.built_ins.or_fn.cljs$core$IFn$_invoke$arity$variadic = (function (args){
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (a,b){
if(cljs.core.truth_(b)){
return cljs.core.reduced(b);
} else {
return b;
}
}),null,args);
}));

(datascript.built_ins.or_fn.cljs$lang$maxFixedArity = (0));

/** @this {Function} */
(datascript.built_ins.or_fn.cljs$lang$applyTo = (function (seq40072){
var self__5862__auto__ = this;
return self__5862__auto__.cljs$core$IFn$_invoke$arity$variadic(cljs.core.seq(seq40072));
}));

datascript.built_ins.less = (function datascript$built_ins$less(var_args){
var G__40095 = arguments.length;
switch (G__40095) {
case 1:
return datascript.built_ins.less.cljs$core$IFn$_invoke$arity$1((arguments[(0)]));

break;
case 2:
return datascript.built_ins.less.cljs$core$IFn$_invoke$arity$2((arguments[(0)]),(arguments[(1)]));

break;
default:
var args_arr__5901__auto__ = [];
var len__5876__auto___40199 = arguments.length;
var i__5877__auto___40200 = (0);
while(true){
if((i__5877__auto___40200 < len__5876__auto___40199)){
args_arr__5901__auto__.push((arguments[i__5877__auto___40200]));

var G__40201 = (i__5877__auto___40200 + (1));
i__5877__auto___40200 = G__40201;
continue;
} else {
}
break;
}

var argseq__5902__auto__ = ((((2) < args_arr__5901__auto__.length))?(new cljs.core.IndexedSeq(args_arr__5901__auto__.slice((2)),(0),null)):null);
return datascript.built_ins.less.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),(arguments[(1)]),argseq__5902__auto__);

}
});

(datascript.built_ins.less.cljs$core$IFn$_invoke$arity$1 = (function (x){
return true;
}));

(datascript.built_ins.less.cljs$core$IFn$_invoke$arity$2 = (function (x,y){
return (datascript.db.value_compare(x,y) < (0));
}));

(datascript.built_ins.less.cljs$core$IFn$_invoke$arity$variadic = (function (x,y,more){
while(true){
if(datascript.built_ins.less.cljs$core$IFn$_invoke$arity$2(x,y)){
if(cljs.core.next(more)){
var G__40205 = y;
var G__40206 = cljs.core.first(more);
var G__40207 = cljs.core.next(more);
x = G__40205;
y = G__40206;
more = G__40207;
continue;
} else {
return datascript.built_ins.less.cljs$core$IFn$_invoke$arity$2(y,cljs.core.first(more));
}
} else {
return false;
}
break;
}
}));

/** @this {Function} */
(datascript.built_ins.less.cljs$lang$applyTo = (function (seq40084){
var G__40088 = cljs.core.first(seq40084);
var seq40084__$1 = cljs.core.next(seq40084);
var G__40089 = cljs.core.first(seq40084__$1);
var seq40084__$2 = cljs.core.next(seq40084__$1);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__40088,G__40089,seq40084__$2);
}));

(datascript.built_ins.less.cljs$lang$maxFixedArity = (2));

datascript.built_ins.greater = (function datascript$built_ins$greater(var_args){
var G__40106 = arguments.length;
switch (G__40106) {
case 1:
return datascript.built_ins.greater.cljs$core$IFn$_invoke$arity$1((arguments[(0)]));

break;
case 2:
return datascript.built_ins.greater.cljs$core$IFn$_invoke$arity$2((arguments[(0)]),(arguments[(1)]));

break;
default:
var args_arr__5901__auto__ = [];
var len__5876__auto___40209 = arguments.length;
var i__5877__auto___40210 = (0);
while(true){
if((i__5877__auto___40210 < len__5876__auto___40209)){
args_arr__5901__auto__.push((arguments[i__5877__auto___40210]));

var G__40211 = (i__5877__auto___40210 + (1));
i__5877__auto___40210 = G__40211;
continue;
} else {
}
break;
}

var argseq__5902__auto__ = ((((2) < args_arr__5901__auto__.length))?(new cljs.core.IndexedSeq(args_arr__5901__auto__.slice((2)),(0),null)):null);
return datascript.built_ins.greater.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),(arguments[(1)]),argseq__5902__auto__);

}
});

(datascript.built_ins.greater.cljs$core$IFn$_invoke$arity$1 = (function (x){
return true;
}));

(datascript.built_ins.greater.cljs$core$IFn$_invoke$arity$2 = (function (x,y){
return (datascript.db.value_compare(x,y) > (0));
}));

(datascript.built_ins.greater.cljs$core$IFn$_invoke$arity$variadic = (function (x,y,more){
while(true){
if(datascript.built_ins.greater.cljs$core$IFn$_invoke$arity$2(x,y)){
if(cljs.core.next(more)){
var G__40212 = y;
var G__40213 = cljs.core.first(more);
var G__40214 = cljs.core.next(more);
x = G__40212;
y = G__40213;
more = G__40214;
continue;
} else {
return datascript.built_ins.greater.cljs$core$IFn$_invoke$arity$2(y,cljs.core.first(more));
}
} else {
return false;
}
break;
}
}));

/** @this {Function} */
(datascript.built_ins.greater.cljs$lang$applyTo = (function (seq40103){
var G__40104 = cljs.core.first(seq40103);
var seq40103__$1 = cljs.core.next(seq40103);
var G__40105 = cljs.core.first(seq40103__$1);
var seq40103__$2 = cljs.core.next(seq40103__$1);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__40104,G__40105,seq40103__$2);
}));

(datascript.built_ins.greater.cljs$lang$maxFixedArity = (2));

datascript.built_ins.less_equal = (function datascript$built_ins$less_equal(var_args){
var G__40127 = arguments.length;
switch (G__40127) {
case 1:
return datascript.built_ins.less_equal.cljs$core$IFn$_invoke$arity$1((arguments[(0)]));

break;
case 2:
return datascript.built_ins.less_equal.cljs$core$IFn$_invoke$arity$2((arguments[(0)]),(arguments[(1)]));

break;
default:
var args_arr__5901__auto__ = [];
var len__5876__auto___40216 = arguments.length;
var i__5877__auto___40217 = (0);
while(true){
if((i__5877__auto___40217 < len__5876__auto___40216)){
args_arr__5901__auto__.push((arguments[i__5877__auto___40217]));

var G__40218 = (i__5877__auto___40217 + (1));
i__5877__auto___40217 = G__40218;
continue;
} else {
}
break;
}

var argseq__5902__auto__ = ((((2) < args_arr__5901__auto__.length))?(new cljs.core.IndexedSeq(args_arr__5901__auto__.slice((2)),(0),null)):null);
return datascript.built_ins.less_equal.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),(arguments[(1)]),argseq__5902__auto__);

}
});

(datascript.built_ins.less_equal.cljs$core$IFn$_invoke$arity$1 = (function (x){
return true;
}));

(datascript.built_ins.less_equal.cljs$core$IFn$_invoke$arity$2 = (function (x,y){
return (!((datascript.db.value_compare(x,y) > (0))));
}));

(datascript.built_ins.less_equal.cljs$core$IFn$_invoke$arity$variadic = (function (x,y,more){
while(true){
if(datascript.built_ins.less_equal.cljs$core$IFn$_invoke$arity$2(x,y)){
if(cljs.core.next(more)){
var G__40219 = y;
var G__40220 = cljs.core.first(more);
var G__40221 = cljs.core.next(more);
x = G__40219;
y = G__40220;
more = G__40221;
continue;
} else {
return datascript.built_ins.less_equal.cljs$core$IFn$_invoke$arity$2(y,cljs.core.first(more));
}
} else {
return false;
}
break;
}
}));

/** @this {Function} */
(datascript.built_ins.less_equal.cljs$lang$applyTo = (function (seq40124){
var G__40125 = cljs.core.first(seq40124);
var seq40124__$1 = cljs.core.next(seq40124);
var G__40126 = cljs.core.first(seq40124__$1);
var seq40124__$2 = cljs.core.next(seq40124__$1);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__40125,G__40126,seq40124__$2);
}));

(datascript.built_ins.less_equal.cljs$lang$maxFixedArity = (2));

datascript.built_ins.greater_equal = (function datascript$built_ins$greater_equal(var_args){
var G__40137 = arguments.length;
switch (G__40137) {
case 1:
return datascript.built_ins.greater_equal.cljs$core$IFn$_invoke$arity$1((arguments[(0)]));

break;
case 2:
return datascript.built_ins.greater_equal.cljs$core$IFn$_invoke$arity$2((arguments[(0)]),(arguments[(1)]));

break;
default:
var args_arr__5901__auto__ = [];
var len__5876__auto___40223 = arguments.length;
var i__5877__auto___40224 = (0);
while(true){
if((i__5877__auto___40224 < len__5876__auto___40223)){
args_arr__5901__auto__.push((arguments[i__5877__auto___40224]));

var G__40225 = (i__5877__auto___40224 + (1));
i__5877__auto___40224 = G__40225;
continue;
} else {
}
break;
}

var argseq__5902__auto__ = ((((2) < args_arr__5901__auto__.length))?(new cljs.core.IndexedSeq(args_arr__5901__auto__.slice((2)),(0),null)):null);
return datascript.built_ins.greater_equal.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),(arguments[(1)]),argseq__5902__auto__);

}
});

(datascript.built_ins.greater_equal.cljs$core$IFn$_invoke$arity$1 = (function (x){
return true;
}));

(datascript.built_ins.greater_equal.cljs$core$IFn$_invoke$arity$2 = (function (x,y){
return (!((datascript.db.value_compare(x,y) < (0))));
}));

(datascript.built_ins.greater_equal.cljs$core$IFn$_invoke$arity$variadic = (function (x,y,more){
while(true){
if(datascript.built_ins.greater_equal.cljs$core$IFn$_invoke$arity$2(x,y)){
if(cljs.core.next(more)){
var G__40226 = y;
var G__40227 = cljs.core.first(more);
var G__40228 = cljs.core.next(more);
x = G__40226;
y = G__40227;
more = G__40228;
continue;
} else {
return datascript.built_ins.greater_equal.cljs$core$IFn$_invoke$arity$2(y,cljs.core.first(more));
}
} else {
return false;
}
break;
}
}));

/** @this {Function} */
(datascript.built_ins.greater_equal.cljs$lang$applyTo = (function (seq40134){
var G__40135 = cljs.core.first(seq40134);
var seq40134__$1 = cljs.core.next(seq40134);
var G__40136 = cljs.core.first(seq40134__$1);
var seq40134__$2 = cljs.core.next(seq40134__$1);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__40135,G__40136,seq40134__$2);
}));

(datascript.built_ins.greater_equal.cljs$lang$maxFixedArity = (2));

datascript.built_ins.query_fns = cljs.core.PersistentHashMap.fromArrays([new cljs.core.Symbol(null,"true?","true?",-1600332395,null),new cljs.core.Symbol(null,"and","and",668631710,null),new cljs.core.Symbol(null,"odd?","odd?",-1458588199,null),new cljs.core.Symbol(null,"get-else","get-else",1312024065,null),new cljs.core.Symbol(null,">=",">=",1016916022,null),new cljs.core.Symbol(null,"print-str","print-str",-699700354,null),new cljs.core.Symbol(null,">",">",1085014381,null),new cljs.core.Symbol(null,"count","count",-514511684,null),new cljs.core.Symbol(null,"get-some","get-some",409442058,null),new cljs.core.Symbol(null,"nil?","nil?",1612038930,null),new cljs.core.Symbol(null,"untuple","untuple",-606149900,null),new cljs.core.Symbol(null,"inc","inc",324505433,null),new cljs.core.Symbol(null,"/","/",-1371932971,null),new cljs.core.Symbol(null,"quot","quot",-1125214196,null),new cljs.core.Symbol(null,"false?","false?",-1522377573,null),new cljs.core.Symbol(null,"not","not",1044554643,null),new cljs.core.Symbol(null,"identity","identity",-1007039734,null),new cljs.core.Symbol(null,"-differ?","-differ?",1465687357,null),new cljs.core.Symbol(null,"tuple","tuple",1167864243,null),new cljs.core.Symbol(null,"re-seq","re-seq",-2105554076,null),new cljs.core.Symbol(null,"<=","<=",1244895369,null),new cljs.core.Symbol("clojure.string","starts-with?","clojure.string/starts-with?",656256322,null),new cljs.core.Symbol(null,"=","=",-1501502141,null),new cljs.core.Symbol(null,"min","min",2085523049,null),new cljs.core.Symbol(null,"+","+",-740910886,null),new cljs.core.Symbol(null,"name","name",-810760592,null),new cljs.core.Symbol(null,"missing?","missing?",-1710383910,null),new cljs.core.Symbol(null,"ground","ground",-1460862835,null),new cljs.core.Symbol(null,"rand-int","rand-int",-495012157,null),new cljs.core.Symbol(null,"complement","complement",-913606051,null),new cljs.core.Symbol(null,"==","==",-234118149,null),new cljs.core.Symbol(null,"hash-map","hash-map",-439030950,null),new cljs.core.Symbol(null,"keyword?","keyword?",1917797069,null),new cljs.core.Symbol(null,"compare","compare",1109853757,null),new cljs.core.Symbol(null,"range","range",-1014743483,null),new cljs.core.Symbol(null,"max","max",1701898075,null),new cljs.core.Symbol(null,"empty?","empty?",76408555,null),new cljs.core.Symbol(null,"integer?","integer?",1303791671,null),new cljs.core.Symbol(null,"*","*",345799209,null),new cljs.core.Symbol(null,"pr-str","pr-str",-2066912145,null),new cljs.core.Symbol(null,"println-str","println-str",-2049216703,null),new cljs.core.Symbol(null,"meta","meta",-1154898805,null),new cljs.core.Symbol(null,"zero?","zero?",325758897,null),new cljs.core.Symbol(null,"!=","!=",-201205829,null),new cljs.core.Symbol(null,"prn-str","prn-str",-145225943,null),new cljs.core.Symbol(null,"dec","dec",-766002333,null),new cljs.core.Symbol(null,"string?","string?",-1129175764,null),new cljs.core.Symbol(null,"re-pattern","re-pattern",1047705161,null),new cljs.core.Symbol(null,"vector","vector",-751469611,null),new cljs.core.Symbol(null,"not=","not=",1466536204,null),new cljs.core.Symbol(null,"get","get",-971253014,null),new cljs.core.Symbol(null,"str","str",-1564826950,null),new cljs.core.Symbol(null,"<","<",993667236,null),new cljs.core.Symbol(null,"namespace","namespace",1263021155,null),new cljs.core.Symbol(null,"keyword","keyword",-1843046022,null),new cljs.core.Symbol(null,"-","-",-471816912,null),new cljs.core.Symbol(null,"some?","some?",234752293,null),new cljs.core.Symbol(null,"pos?","pos?",-244377722,null),new cljs.core.Symbol(null,"rand","rand",-1745930995,null),new cljs.core.Symbol(null,"re-matches","re-matches",-1865705768,null),new cljs.core.Symbol(null,"boolean?","boolean?",1790940868,null),new cljs.core.Symbol(null,"list","list",-1889078086,null),new cljs.core.Symbol(null,"contains?","contains?",-1676812576,null),new cljs.core.Symbol("clojure.string","ends-with?","clojure.string/ends-with?",-745964149,null),new cljs.core.Symbol(null,"subs","subs",1453849536,null),new cljs.core.Symbol(null,"mod","mod",1510044207,null),new cljs.core.Symbol(null,"number?","number?",-1747282210,null),new cljs.core.Symbol("clojure.string","includes?","clojure.string/includes?",-1842146260,null),new cljs.core.Symbol(null,"array-map","array-map",1555038968,null),new cljs.core.Symbol(null,"rem","rem",664046770,null),new cljs.core.Symbol(null,"even?","even?",-1827825394,null),new cljs.core.Symbol(null,"type","type",-1480165421,null),new cljs.core.Symbol(null,"neg?","neg?",-1902175577,null),new cljs.core.Symbol("clojure.string","blank?","clojure.string/blank?",1772874244,null),new cljs.core.Symbol(null,"not-empty","not-empty",2029453590,null),new cljs.core.Symbol(null,"or","or",1876275696,null),new cljs.core.Symbol(null,"identical?","identical?",-745864205,null),new cljs.core.Symbol(null,"set","set",1945134081,null),new cljs.core.Symbol(null,"re-find","re-find",1143444147,null)],[cljs.core.true_QMARK_,datascript.built_ins.and_fn,cljs.core.odd_QMARK_,datascript.built_ins._get_else,datascript.built_ins.greater_equal,cljs.core.print_str,datascript.built_ins.greater,cljs.core.count,datascript.built_ins._get_some,cljs.core.nil_QMARK_,cljs.core.identity,cljs.core.inc,cljs.core._SLASH_,cljs.core.quot,cljs.core.false_QMARK_,cljs.core.not,cljs.core.identity,datascript.built_ins._differ_QMARK_,cljs.core.vector,cljs.core.re_seq,datascript.built_ins.less_equal,clojure.string.starts_with_QMARK_,cljs.core._EQ_,cljs.core.min,cljs.core._PLUS_,cljs.core.name,datascript.built_ins._missing_QMARK_,cljs.core.identity,cljs.core.rand_int,cljs.core.complement,cljs.core._EQ__EQ_,cljs.core.hash_map,cljs.core.keyword_QMARK_,cljs.core.compare,cljs.core.range,cljs.core.max,cljs.core.empty_QMARK_,cljs.core.integer_QMARK_,cljs.core._STAR_,cljs.core.pr_str,cljs.core.println_str,cljs.core.meta,cljs.core.zero_QMARK_,cljs.core.not_EQ_,cljs.core.prn_str,cljs.core.dec,cljs.core.string_QMARK_,cljs.core.re_pattern,cljs.core.vector,cljs.core.not_EQ_,cljs.core.get,cljs.core.str,datascript.built_ins.less,cljs.core.namespace,cljs.core.keyword,cljs.core._,cljs.core.some_QMARK_,cljs.core.pos_QMARK_,cljs.core.rand,cljs.core.re_matches,cljs.core.boolean_QMARK_,cljs.core.list,cljs.core.contains_QMARK_,clojure.string.ends_with_QMARK_,cljs.core.subs,cljs.core.mod,cljs.core.number_QMARK_,clojure.string.includes_QMARK_,cljs.core.array_map,cljs.core.rem,cljs.core.even_QMARK_,cljs.core.type,cljs.core.neg_QMARK_,clojure.string.blank_QMARK_,cljs.core.not_empty,datascript.built_ins.or_fn,cljs.core.identical_QMARK_,cljs.core.set,cljs.core.re_find]);
datascript.built_ins.aggregate_sum = (function datascript$built_ins$aggregate_sum(coll){
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(cljs.core._PLUS_,(0),coll);
});
datascript.built_ins.aggregate_avg = (function datascript$built_ins$aggregate_avg(coll){
return (datascript.built_ins.aggregate_sum(coll) / cljs.core.count(coll));
});
datascript.built_ins.aggregate_median = (function datascript$built_ins$aggregate_median(coll){
var terms = cljs.core.sort.cljs$core$IFn$_invoke$arity$1(coll);
var size = cljs.core.count(coll);
var med = (size >> (1));
var G__40151 = cljs.core.nth.cljs$core$IFn$_invoke$arity$2(terms,med);
if(cljs.core.even_QMARK_(size)){
return ((G__40151 + cljs.core.nth.cljs$core$IFn$_invoke$arity$2(terms,(med - (1)))) / (2));
} else {
return G__40151;
}
});
datascript.built_ins.aggregate_variance = (function datascript$built_ins$aggregate_variance(coll){
var mean = datascript.built_ins.aggregate_avg(coll);
var sum = datascript.built_ins.aggregate_sum((function (){var iter__5628__auto__ = (function datascript$built_ins$aggregate_variance_$_iter__40152(s__40153){
return (new cljs.core.LazySeq(null,(function (){
var s__40153__$1 = s__40153;
while(true){
var temp__5804__auto__ = cljs.core.seq(s__40153__$1);
if(temp__5804__auto__){
var s__40153__$2 = temp__5804__auto__;
if(cljs.core.chunked_seq_QMARK_(s__40153__$2)){
var c__5626__auto__ = cljs.core.chunk_first(s__40153__$2);
var size__5627__auto__ = cljs.core.count(c__5626__auto__);
var b__40155 = cljs.core.chunk_buffer(size__5627__auto__);
if((function (){var i__40154 = (0);
while(true){
if((i__40154 < size__5627__auto__)){
var x = cljs.core._nth(c__5626__auto__,i__40154);
var delta = (x - mean);
cljs.core.chunk_append(b__40155,(delta * delta));

var G__40239 = (i__40154 + (1));
i__40154 = G__40239;
continue;
} else {
return true;
}
break;
}
})()){
return cljs.core.chunk_cons(cljs.core.chunk(b__40155),datascript$built_ins$aggregate_variance_$_iter__40152(cljs.core.chunk_rest(s__40153__$2)));
} else {
return cljs.core.chunk_cons(cljs.core.chunk(b__40155),null);
}
} else {
var x = cljs.core.first(s__40153__$2);
var delta = (x - mean);
return cljs.core.cons((delta * delta),datascript$built_ins$aggregate_variance_$_iter__40152(cljs.core.rest(s__40153__$2)));
}
} else {
return null;
}
break;
}
}),null,null));
});
return iter__5628__auto__(coll);
})());
return (sum / cljs.core.count(coll));
});
datascript.built_ins.aggregate_stddev = (function datascript$built_ins$aggregate_stddev(coll){
return Math.sqrt(datascript.built_ins.aggregate_variance(coll));
});
datascript.built_ins.aggregate_min = (function datascript$built_ins$aggregate_min(var_args){
var G__40162 = arguments.length;
switch (G__40162) {
case 1:
return datascript.built_ins.aggregate_min.cljs$core$IFn$_invoke$arity$1((arguments[(0)]));

break;
case 2:
return datascript.built_ins.aggregate_min.cljs$core$IFn$_invoke$arity$2((arguments[(0)]),(arguments[(1)]));

break;
default:
throw (new Error(["Invalid arity: ",arguments.length].join("")));

}
});

(datascript.built_ins.aggregate_min.cljs$core$IFn$_invoke$arity$1 = (function (coll){
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (acc,x){
if((cljs.core.compare(x,acc) < (0))){
return x;
} else {
return acc;
}
}),cljs.core.first(coll),cljs.core.next(coll));
}));

(datascript.built_ins.aggregate_min.cljs$core$IFn$_invoke$arity$2 = (function (n,coll){
return cljs.core.vec(cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (acc,x){
if((cljs.core.count(acc) < n)){
return cljs.core.sort.cljs$core$IFn$_invoke$arity$2(cljs.core.compare,cljs.core.conj.cljs$core$IFn$_invoke$arity$2(acc,x));
} else {
if((cljs.core.compare(x,cljs.core.last(acc)) < (0))){
return cljs.core.sort.cljs$core$IFn$_invoke$arity$2(cljs.core.compare,cljs.core.conj.cljs$core$IFn$_invoke$arity$2(cljs.core.butlast(acc),x));
} else {
return acc;

}
}
}),cljs.core.PersistentVector.EMPTY,coll));
}));

(datascript.built_ins.aggregate_min.cljs$lang$maxFixedArity = 2);

datascript.built_ins.aggregate_max = (function datascript$built_ins$aggregate_max(var_args){
var G__40168 = arguments.length;
switch (G__40168) {
case 1:
return datascript.built_ins.aggregate_max.cljs$core$IFn$_invoke$arity$1((arguments[(0)]));

break;
case 2:
return datascript.built_ins.aggregate_max.cljs$core$IFn$_invoke$arity$2((arguments[(0)]),(arguments[(1)]));

break;
default:
throw (new Error(["Invalid arity: ",arguments.length].join("")));

}
});

(datascript.built_ins.aggregate_max.cljs$core$IFn$_invoke$arity$1 = (function (coll){
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (acc,x){
if((cljs.core.compare(x,acc) > (0))){
return x;
} else {
return acc;
}
}),cljs.core.first(coll),cljs.core.next(coll));
}));

(datascript.built_ins.aggregate_max.cljs$core$IFn$_invoke$arity$2 = (function (n,coll){
return cljs.core.vec(cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (acc,x){
if((cljs.core.count(acc) < n)){
return cljs.core.sort.cljs$core$IFn$_invoke$arity$2(cljs.core.compare,cljs.core.conj.cljs$core$IFn$_invoke$arity$2(acc,x));
} else {
if((cljs.core.compare(x,cljs.core.first(acc)) > (0))){
return cljs.core.sort.cljs$core$IFn$_invoke$arity$2(cljs.core.compare,cljs.core.conj.cljs$core$IFn$_invoke$arity$2(cljs.core.next(acc),x));
} else {
return acc;

}
}
}),cljs.core.PersistentVector.EMPTY,coll));
}));

(datascript.built_ins.aggregate_max.cljs$lang$maxFixedArity = 2);

datascript.built_ins.aggregate_rand = (function datascript$built_ins$aggregate_rand(var_args){
var G__40170 = arguments.length;
switch (G__40170) {
case 1:
return datascript.built_ins.aggregate_rand.cljs$core$IFn$_invoke$arity$1((arguments[(0)]));

break;
case 2:
return datascript.built_ins.aggregate_rand.cljs$core$IFn$_invoke$arity$2((arguments[(0)]),(arguments[(1)]));

break;
default:
throw (new Error(["Invalid arity: ",arguments.length].join("")));

}
});

(datascript.built_ins.aggregate_rand.cljs$core$IFn$_invoke$arity$1 = (function (coll){
return cljs.core.rand_nth(coll);
}));

(datascript.built_ins.aggregate_rand.cljs$core$IFn$_invoke$arity$2 = (function (n,coll){
return cljs.core.vec(cljs.core.repeatedly.cljs$core$IFn$_invoke$arity$2(n,(function (){
return cljs.core.rand_nth(coll);
})));
}));

(datascript.built_ins.aggregate_rand.cljs$lang$maxFixedArity = 2);

datascript.built_ins.aggregate_sample = (function datascript$built_ins$aggregate_sample(n,coll){
return cljs.core.vec(cljs.core.take.cljs$core$IFn$_invoke$arity$2(n,cljs.core.shuffle(coll)));
});
datascript.built_ins.aggregate_count_distinct = (function datascript$built_ins$aggregate_count_distinct(coll){
return cljs.core.count(cljs.core.distinct.cljs$core$IFn$_invoke$arity$1(coll));
});
datascript.built_ins.aggregates = cljs.core.PersistentHashMap.fromArrays([new cljs.core.Symbol(null,"variance","variance",-1522424942,null),new cljs.core.Symbol(null,"count","count",-514511684,null),new cljs.core.Symbol(null,"median","median",-2084869638,null),new cljs.core.Symbol(null,"sum","sum",1777518341,null),new cljs.core.Symbol(null,"min","min",2085523049,null),new cljs.core.Symbol(null,"max","max",1701898075,null),new cljs.core.Symbol(null,"count-distinct","count-distinct",-1566572514,null),new cljs.core.Symbol(null,"distinct","distinct",-148347594,null),new cljs.core.Symbol(null,"avg","avg",1837937727,null),new cljs.core.Symbol(null,"stddev","stddev",775056588,null),new cljs.core.Symbol(null,"rand","rand",-1745930995,null),new cljs.core.Symbol(null,"sample","sample",1719555128,null)],[datascript.built_ins.aggregate_variance,cljs.core.count,datascript.built_ins.aggregate_median,datascript.built_ins.aggregate_sum,datascript.built_ins.aggregate_min,datascript.built_ins.aggregate_max,datascript.built_ins.aggregate_count_distinct,cljs.core.set,datascript.built_ins.aggregate_avg,datascript.built_ins.aggregate_stddev,datascript.built_ins.aggregate_rand,datascript.built_ins.aggregate_sample]);

//# sourceMappingURL=datascript.built_ins.js.map
