import "./cljs_env.js";
import "./cljs.core.js";
goog.provide('logseq.clj_fractional_indexing');
logseq.clj_fractional_indexing.base_62_digits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
logseq.clj_fractional_indexing.char__GT_int = (function logseq$clj_fractional_indexing$char__GT_int(c){
return c.charCodeAt((0));
});
logseq.clj_fractional_indexing.get_integer_length = (function logseq$clj_fractional_indexing$get_integer_length(head){
var head_char = logseq.clj_fractional_indexing.char__GT_int(head);
if((((head_char >= logseq.clj_fractional_indexing.char__GT_int("a"))) && ((head_char <= logseq.clj_fractional_indexing.char__GT_int("z"))))){
return ((head_char - logseq.clj_fractional_indexing.char__GT_int("a")) + (2));
} else {
if((((head_char >= logseq.clj_fractional_indexing.char__GT_int("A"))) && ((head_char <= logseq.clj_fractional_indexing.char__GT_int("Z"))))){
return ((logseq.clj_fractional_indexing.char__GT_int("Z") - head_char) + (2));
} else {
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2((""+"invalid order key head: "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(head)),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"head","head",-771383919),head], null));

}
}
});
logseq.clj_fractional_indexing.validate_integer = (function logseq$clj_fractional_indexing$validate_integer(int$){
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(cljs.core.count(int$),logseq.clj_fractional_indexing.get_integer_length(cljs.core.first(int$)))){
return null;
} else {
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2((""+"invalid integer part of order key: "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(int$)),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"int","int",-1741416922),int$], null));
}
});
logseq.clj_fractional_indexing.str_slice = (function logseq$clj_fractional_indexing$str_slice(var_args){
var G__37353 = arguments.length;
switch (G__37353) {
case 2:
return logseq.clj_fractional_indexing.str_slice.cljs$core$IFn$_invoke$arity$2((arguments[(0)]),(arguments[(1)]));

break;
case 3:
return logseq.clj_fractional_indexing.str_slice.cljs$core$IFn$_invoke$arity$3((arguments[(0)]),(arguments[(1)]),(arguments[(2)]));

break;
default:
throw (new Error(["Invalid arity: ",arguments.length].join("")));

}
});

(logseq.clj_fractional_indexing.str_slice.cljs$core$IFn$_invoke$arity$2 = (function (s,i){
return logseq.clj_fractional_indexing.str_slice.cljs$core$IFn$_invoke$arity$3(s,i,cljs.core.count(s));
}));

(logseq.clj_fractional_indexing.str_slice.cljs$core$IFn$_invoke$arity$3 = (function (s,start,end){
if(cljs.core.truth_((function (){var and__5140__auto__ = s;
if(cljs.core.truth_(and__5140__auto__)){
return (((cljs.core.count(s) >= start)) && ((((cljs.core.count(s) >= end)) && ((end > start)))));
} else {
return and__5140__auto__;
}
})())){
return cljs.core.subs.cljs$core$IFn$_invoke$arity$3(s,start,end);
} else {
return "";
}
}));

(logseq.clj_fractional_indexing.str_slice.cljs$lang$maxFixedArity = 3);

logseq.clj_fractional_indexing.get_integer_part = (function logseq$clj_fractional_indexing$get_integer_part(key){
var integer_part_length = logseq.clj_fractional_indexing.get_integer_length(cljs.core.first(key));
if((integer_part_length > cljs.core.count(key))){
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2((""+"invalid order key: "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(key)),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"key","key",-1516042587),key], null));
} else {
}

return logseq.clj_fractional_indexing.str_slice.cljs$core$IFn$_invoke$arity$3(key,(0),integer_part_length);
});
logseq.clj_fractional_indexing.validate_order_key = (function logseq$clj_fractional_indexing$validate_order_key(key,digits){
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(key,(""+"A"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.apply.cljs$core$IFn$_invoke$arity$2(cljs.core.str,cljs.core.repeat.cljs$core$IFn$_invoke$arity$2((26),cljs.core.first(digits))))))){
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2((""+"invalid order key: "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(key)),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"key","key",-1516042587),key], null));
} else {
}

var i = logseq.clj_fractional_indexing.get_integer_part(key);
var f = logseq.clj_fractional_indexing.str_slice.cljs$core$IFn$_invoke$arity$2(key,((i).length));
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(cljs.core.last(f),cljs.core.first(digits))){
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2((""+"invalid order key: "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(key)),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"key","key",-1516042587),key], null));
} else {
return null;
}
});
logseq.clj_fractional_indexing.increment_integer = (function logseq$clj_fractional_indexing$increment_integer(x,digits){
logseq.clj_fractional_indexing.validate_integer(x);

var vec__37377 = cljs.core.seq(x);
var seq__37378 = cljs.core.seq(vec__37377);
var first__37379 = cljs.core.first(seq__37378);
var seq__37378__$1 = cljs.core.next(seq__37378);
var head = first__37379;
var digs = seq__37378__$1;
var vec__37380 = cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (p__37389,dig){
var vec__37390 = p__37389;
var carry_QMARK_ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__37390,(0),null);
var digs__$1 = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__37390,(1),null);
if(cljs.core.truth_(carry_QMARK_)){
var d = (digits.indexOf((""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(dig))) + (1));
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(d,cljs.core.count(digits))){
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [true,cljs.core.conj.cljs$core$IFn$_invoke$arity$2(digs__$1,cljs.core.first(digits))], null);
} else {
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [false,cljs.core.conj.cljs$core$IFn$_invoke$arity$2(digs__$1,cljs.core.nth.cljs$core$IFn$_invoke$arity$2(digits,d))], null);
}
} else {
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [carry_QMARK_,digs__$1], null);
}
}),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [true,cljs.core.PersistentVector.EMPTY], null),cljs.core.reverse(digs));
var carry_QMARK_ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__37380,(0),null);
var diff = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__37380,(1),null);
var digs__$1 = cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.subvec.cljs$core$IFn$_invoke$arity$3(cljs.core.vec(digs),(0),(cljs.core.count(digs) - cljs.core.count(diff))),cljs.core.reverse(diff));
if(cljs.core.truth_(carry_QMARK_)){
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(head,"Z")){
return (""+"a"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.first(digits)));
} else {
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(head,"z")){
return null;
} else {
var h = cljs.core.char$((logseq.clj_fractional_indexing.char__GT_int(head) + (1)));
var digs__$2 = (((cljs.core.compare(h,"a") > (0)))?cljs.core.conj.cljs$core$IFn$_invoke$arity$2(digs__$1,cljs.core.first(digits)):cljs.core.pop(digs__$1));
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(h)+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.apply.cljs$core$IFn$_invoke$arity$2(cljs.core.str,digs__$2)));

}
}
} else {
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(head)+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.apply.cljs$core$IFn$_invoke$arity$2(cljs.core.str,digs__$1)));
}
});
logseq.clj_fractional_indexing.decrement_integer = (function logseq$clj_fractional_indexing$decrement_integer(x,digits){
logseq.clj_fractional_indexing.validate_integer(x);

var vec__37408 = cljs.core.seq(x);
var seq__37409 = cljs.core.seq(vec__37408);
var first__37410 = cljs.core.first(seq__37409);
var seq__37409__$1 = cljs.core.next(seq__37409);
var head = first__37410;
var digs = seq__37409__$1;
var vec__37411 = cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (p__37417,dig){
var vec__37418 = p__37417;
var borrow_QMARK_ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__37418,(0),null);
var acc = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__37418,(1),null);
if(cljs.core.not(borrow_QMARK_)){
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [false,cljs.core.conj.cljs$core$IFn$_invoke$arity$2(acc,dig)], null);
} else {
var d = (digits.indexOf((""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(dig))) - (1));
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(d,(-1))){
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [true,cljs.core.conj.cljs$core$IFn$_invoke$arity$2(acc,cljs.core.last(digits))], null);
} else {
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [false,cljs.core.conj.cljs$core$IFn$_invoke$arity$2(acc,cljs.core.nth.cljs$core$IFn$_invoke$arity$2(digits,d))], null);
}
}
}),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [true,cljs.core.PersistentVector.EMPTY], null),cljs.core.reverse(digs));
var borrow = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__37411,(0),null);
var new_digs = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__37411,(1),null);
var new_digs__$1 = cljs.core.vec(cljs.core.reverse(new_digs));
if(cljs.core.truth_(borrow)){
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(head,"a")){
return (""+"Z"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.last(digits)));
} else {
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(head,"A")){
return null;
} else {
var h = cljs.core.char$((logseq.clj_fractional_indexing.char__GT_int(head) - (1)));
var new_digs__$2 = (((cljs.core.compare(h,"Z") < (0)))?cljs.core.conj.cljs$core$IFn$_invoke$arity$2(new_digs__$1,cljs.core.last(digits)):cljs.core.pop(new_digs__$1));
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(h)+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.apply.cljs$core$IFn$_invoke$arity$2(cljs.core.str,new_digs__$2)));

}
}
} else {
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(head)+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.apply.cljs$core$IFn$_invoke$arity$2(cljs.core.str,new_digs__$1)));
}
});
logseq.clj_fractional_indexing.midpoint = (function logseq$clj_fractional_indexing$midpoint(a,b,digits){
var zero = cljs.core.first(digits);
if(cljs.core.truth_((function (){var and__5140__auto__ = b;
if(cljs.core.truth_(and__5140__auto__)){
return (cljs.core.compare(a,b) >= (0));
} else {
return and__5140__auto__;
}
})())){
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2((""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(a)+" >= "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(b)),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"a","a",-2123407586),a,new cljs.core.Keyword(null,"b","b",1482224470),b], null));
} else {
}

if(((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(cljs.core.last(a),zero)) || (cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(cljs.core.last(b),zero)))){
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2(" trailing zero",new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"a","a",-2123407586),a,new cljs.core.Keyword(null,"b","b",1482224470),b], null));
} else {
}

var n = (cljs.core.truth_(b)?cljs.core.first(cljs.core.keep_indexed.cljs$core$IFn$_invoke$arity$2((function (i,_c){
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(cljs.core.nth.cljs$core$IFn$_invoke$arity$3(a,i,zero),cljs.core.nth.cljs$core$IFn$_invoke$arity$2(b,i))){
return null;
} else {
return i;
}
}),b)):null);
if(cljs.core.truth_((function (){var and__5140__auto__ = n;
if(cljs.core.truth_(and__5140__auto__)){
return (n > (0));
} else {
return and__5140__auto__;
}
})())){
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(logseq.clj_fractional_indexing.str_slice.cljs$core$IFn$_invoke$arity$3(b,(0),n))+cljs.core.str.cljs$core$IFn$_invoke$arity$1((function (){var G__37445 = logseq.clj_fractional_indexing.str_slice.cljs$core$IFn$_invoke$arity$2(a,n);
var G__37446 = logseq.clj_fractional_indexing.str_slice.cljs$core$IFn$_invoke$arity$2(b,n);
var G__37447 = digits;
return (logseq.clj_fractional_indexing.midpoint.cljs$core$IFn$_invoke$arity$3 ? logseq.clj_fractional_indexing.midpoint.cljs$core$IFn$_invoke$arity$3(G__37445,G__37446,G__37447) : logseq.clj_fractional_indexing.midpoint.call(null,G__37445,G__37446,G__37447));
})()));
} else {
var digit_a = ((cljs.core.seq(a))?digits.indexOf((""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.first(a)))):(0));
var digit_b = (cljs.core.truth_(b)?digits.indexOf((""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.first(b)))):cljs.core.count(digits));
if(((digit_b - digit_a) > (1))){
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.nth.cljs$core$IFn$_invoke$arity$2(digits,(Math.round((0.5 * (digit_a + digit_b))) | 0))));
} else {
if(((cljs.core.seq(b)) && ((cljs.core.count(b) > (1))))){
return logseq.clj_fractional_indexing.str_slice.cljs$core$IFn$_invoke$arity$3(b,(0),(1));
} else {
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.nth.cljs$core$IFn$_invoke$arity$2(digits,digit_a))+cljs.core.str.cljs$core$IFn$_invoke$arity$1((function (){var G__37451 = logseq.clj_fractional_indexing.str_slice.cljs$core$IFn$_invoke$arity$2(a,(1));
var G__37452 = null;
var G__37453 = digits;
return (logseq.clj_fractional_indexing.midpoint.cljs$core$IFn$_invoke$arity$3 ? logseq.clj_fractional_indexing.midpoint.cljs$core$IFn$_invoke$arity$3(G__37451,G__37452,G__37453) : logseq.clj_fractional_indexing.midpoint.call(null,G__37451,G__37452,G__37453));
})()));
}
}
}
});
logseq.clj_fractional_indexing.generate_key_between = (function logseq$clj_fractional_indexing$generate_key_between(var_args){
var args__5882__auto__ = [];
var len__5876__auto___37466 = arguments.length;
var i__5877__auto___37467 = (0);
while(true){
if((i__5877__auto___37467 < len__5876__auto___37466)){
args__5882__auto__.push((arguments[i__5877__auto___37467]));

var G__37468 = (i__5877__auto___37467 + (1));
i__5877__auto___37467 = G__37468;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((2) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((2)),(0),null)):null);
return logseq.clj_fractional_indexing.generate_key_between.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),(arguments[(1)]),argseq__5883__auto__);
});

(logseq.clj_fractional_indexing.generate_key_between.cljs$core$IFn$_invoke$arity$variadic = (function (a,b,p__37457){
var map__37458 = p__37457;
var map__37458__$1 = cljs.core.__destructure_map(map__37458);
var digits = cljs.core.get.cljs$core$IFn$_invoke$arity$3(map__37458__$1,new cljs.core.Keyword(null,"digits","digits",-1134635061),logseq.clj_fractional_indexing.base_62_digits);
if(cljs.core.truth_(a)){
logseq.clj_fractional_indexing.validate_order_key(a,digits);
} else {
}

if(cljs.core.truth_(b)){
logseq.clj_fractional_indexing.validate_order_key(b,digits);
} else {
}

if(cljs.core.truth_((function (){var and__5140__auto__ = a;
if(cljs.core.truth_(and__5140__auto__)){
var and__5140__auto____$1 = b;
if(cljs.core.truth_(and__5140__auto____$1)){
return (cljs.core.compare(a,b) >= (0));
} else {
return and__5140__auto____$1;
}
} else {
return and__5140__auto__;
}
})())){
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2((""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(a)+" >= "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(b)),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"a","a",-2123407586),a,new cljs.core.Keyword(null,"b","b",1482224470),b], null));
} else {
}

var result = (((a == null))?(((b == null))?(""+"a"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.first(digits))):(function (){var ib = logseq.clj_fractional_indexing.get_integer_part(b);
var fb = logseq.clj_fractional_indexing.str_slice.cljs$core$IFn$_invoke$arity$2(b,((ib).length));
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(ib,(""+"A"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.apply.cljs$core$IFn$_invoke$arity$2(cljs.core.str,cljs.core.repeat.cljs$core$IFn$_invoke$arity$2((26),cljs.core.first(digits))))))){
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(ib)+cljs.core.str.cljs$core$IFn$_invoke$arity$1(logseq.clj_fractional_indexing.midpoint("",fb,digits)));
} else {
if((cljs.core.compare((""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(ib)),b) < (0))){
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(ib)+cljs.core.str.cljs$core$IFn$_invoke$arity$1(logseq.clj_fractional_indexing.midpoint("",fb,digits)));
} else {
var res = logseq.clj_fractional_indexing.decrement_integer(ib,digits);
if((res == null)){
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2("cannot decrement any more",new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"a","a",-2123407586),a,new cljs.core.Keyword(null,"b","b",1482224470),b,new cljs.core.Keyword(null,"ib","ib",220610727),ib], null));
} else {
return res;
}
}
}
})()):(((b == null))?(function (){var ia = logseq.clj_fractional_indexing.get_integer_part(a);
var fa = logseq.clj_fractional_indexing.str_slice.cljs$core$IFn$_invoke$arity$2(a,((ia).length));
var i = logseq.clj_fractional_indexing.increment_integer(ia,digits);
if((i == null)){
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(ia)+cljs.core.str.cljs$core$IFn$_invoke$arity$1(logseq.clj_fractional_indexing.midpoint(fa,null,digits)));
} else {
return i;
}
})():(function (){var ia = logseq.clj_fractional_indexing.get_integer_part(a);
var fa = logseq.clj_fractional_indexing.str_slice.cljs$core$IFn$_invoke$arity$2(a,((ia).length));
var ib = logseq.clj_fractional_indexing.get_integer_part(b);
var fb = logseq.clj_fractional_indexing.str_slice.cljs$core$IFn$_invoke$arity$2(b,((ib).length));
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(ia,ib)){
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(ia)+cljs.core.str.cljs$core$IFn$_invoke$arity$1(logseq.clj_fractional_indexing.midpoint(fa,fb,digits)));
} else {
var i = logseq.clj_fractional_indexing.increment_integer(ia,digits);
if((i == null)){
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2("cannot increment any more",new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"a","a",-2123407586),a,new cljs.core.Keyword(null,"b","b",1482224470),b,new cljs.core.Keyword(null,"ia","ia",1614010639),ia], null));
} else {
if((cljs.core.compare(i,b) < (0))){
return i;
} else {
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(ia)+cljs.core.str.cljs$core$IFn$_invoke$arity$1(logseq.clj_fractional_indexing.midpoint(fa,null,digits)));
}
}
}
})()
));
if(cljs.core.truth_((function (){var or__5142__auto__ = (function (){var and__5140__auto__ = a;
if(cljs.core.truth_(and__5140__auto__)){
return (cljs.core.compare(a,result) >= (0));
} else {
return and__5140__auto__;
}
})();
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
var and__5140__auto__ = b;
if(cljs.core.truth_(and__5140__auto__)){
return (cljs.core.compare(result,b) >= (0));
} else {
return and__5140__auto__;
}
}
})())){
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2("generate-key-between failed",new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"a","a",-2123407586),a,new cljs.core.Keyword(null,"b","b",1482224470),b,new cljs.core.Keyword(null,"between","between",1131099276),result], null));
} else {
return result;
}
}));

(logseq.clj_fractional_indexing.generate_key_between.cljs$lang$maxFixedArity = (2));

/** @this {Function} */
(logseq.clj_fractional_indexing.generate_key_between.cljs$lang$applyTo = (function (seq37454){
var G__37455 = cljs.core.first(seq37454);
var seq37454__$1 = cljs.core.next(seq37454);
var G__37456 = cljs.core.first(seq37454__$1);
var seq37454__$2 = cljs.core.next(seq37454__$1);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__37455,G__37456,seq37454__$2);
}));

logseq.clj_fractional_indexing.generate_n_keys_between = (function logseq$clj_fractional_indexing$generate_n_keys_between(var_args){
var args__5882__auto__ = [];
var len__5876__auto___37472 = arguments.length;
var i__5877__auto___37473 = (0);
while(true){
if((i__5877__auto___37473 < len__5876__auto___37472)){
args__5882__auto__.push((arguments[i__5877__auto___37473]));

var G__37474 = (i__5877__auto___37473 + (1));
i__5877__auto___37473 = G__37474;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((3) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((3)),(0),null)):null);
return logseq.clj_fractional_indexing.generate_n_keys_between.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),(arguments[(1)]),(arguments[(2)]),argseq__5883__auto__);
});

(logseq.clj_fractional_indexing.generate_n_keys_between.cljs$core$IFn$_invoke$arity$variadic = (function (a,b,n,p__37463){
var map__37464 = p__37463;
var map__37464__$1 = cljs.core.__destructure_map(map__37464);
var digits = cljs.core.get.cljs$core$IFn$_invoke$arity$3(map__37464__$1,new cljs.core.Keyword(null,"digits","digits",-1134635061),logseq.clj_fractional_indexing.base_62_digits);
var result = ((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(n,(0)))?cljs.core.PersistentVector.EMPTY:((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(n,(1)))?new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [logseq.clj_fractional_indexing.generate_key_between.cljs$core$IFn$_invoke$arity$variadic(a,b,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"digits","digits",-1134635061),digits], null)], 0))], null):(((b == null))?cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (col,_){
var k = logseq.clj_fractional_indexing.generate_key_between.cljs$core$IFn$_invoke$arity$variadic((function (){var or__5142__auto__ = cljs.core.last(col);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return a;
}
})(),b,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"digits","digits",-1134635061),digits], null)], 0));
return cljs.core.conj.cljs$core$IFn$_invoke$arity$2(col,k);
}),cljs.core.PersistentVector.EMPTY,cljs.core.range.cljs$core$IFn$_invoke$arity$1(n)):(((a == null))?cljs.core.vec(cljs.core.reverse(cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (col,_){
var k = logseq.clj_fractional_indexing.generate_key_between.cljs$core$IFn$_invoke$arity$variadic(a,(function (){var or__5142__auto__ = cljs.core.last(col);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return b;
}
})(),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"digits","digits",-1134635061),digits], null)], 0));
return cljs.core.conj.cljs$core$IFn$_invoke$arity$2(col,k);
}),cljs.core.PersistentVector.EMPTY,cljs.core.range.cljs$core$IFn$_invoke$arity$1(n)))):(function (){var mid = (Math.floor((n / (2))) | 0);
var c = logseq.clj_fractional_indexing.generate_key_between.cljs$core$IFn$_invoke$arity$variadic(a,b,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"digits","digits",-1134635061),digits], null)], 0));
return cljs.core.concat.cljs$core$IFn$_invoke$arity$variadic(logseq.clj_fractional_indexing.generate_n_keys_between.cljs$core$IFn$_invoke$arity$variadic(a,c,mid,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"digits","digits",-1134635061),digits], null)], 0)),new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [c], null),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([logseq.clj_fractional_indexing.generate_n_keys_between.cljs$core$IFn$_invoke$arity$variadic(c,b,((n - mid) - (1)),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"digits","digits",-1134635061),digits], null)], 0))], 0));
})()
))));
return cljs.core.vec(cljs.core.take.cljs$core$IFn$_invoke$arity$2(n,result));
}));

(logseq.clj_fractional_indexing.generate_n_keys_between.cljs$lang$maxFixedArity = (3));

/** @this {Function} */
(logseq.clj_fractional_indexing.generate_n_keys_between.cljs$lang$applyTo = (function (seq37459){
var G__37460 = cljs.core.first(seq37459);
var seq37459__$1 = cljs.core.next(seq37459);
var G__37461 = cljs.core.first(seq37459__$1);
var seq37459__$2 = cljs.core.next(seq37459__$1);
var G__37462 = cljs.core.first(seq37459__$2);
var seq37459__$3 = cljs.core.next(seq37459__$2);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__37460,G__37461,G__37462,seq37459__$3);
}));


//# sourceMappingURL=logseq.clj_fractional_indexing.js.map
