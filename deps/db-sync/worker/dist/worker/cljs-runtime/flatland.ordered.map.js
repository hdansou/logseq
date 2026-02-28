import "./cljs_env.js";
import "./cljs.core.js";
goog.provide('flatland.ordered.map');
flatland.ordered.map.print_ordered_map = (function flatland$ordered$map$print_ordered_map(writer,kvs,ks,opts){
return cljs.core.pr_sequential_writer(writer,(function (k,w,opts__$1){
cljs.core._write(w,"[");

cljs.core._write(w,cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([k], 0)));

cljs.core._write(w," ");

cljs.core._write(w,cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([cljs.core.get.cljs$core$IFn$_invoke$arity$2(kvs,k)], 0)));

return cljs.core._write(w,"]");
}),"("," ",")",opts,ks);
});

/**
* @constructor
 * @implements {cljs.core.IReversible}
 * @implements {cljs.core.IKVReduce}
 * @implements {cljs.core.IEquiv}
 * @implements {cljs.core.IHash}
 * @implements {cljs.core.IFn}
 * @implements {cljs.core.ICollection}
 * @implements {cljs.core.IFind}
 * @implements {cljs.core.IEmptyableCollection}
 * @implements {cljs.core.ICounted}
 * @implements {cljs.core.ISeqable}
 * @implements {cljs.core.IMeta}
 * @implements {cljs.core.ICloneable}
 * @implements {cljs.core.IPrintWithWriter}
 * @implements {cljs.core.IWithMeta}
 * @implements {cljs.core.IAssociative}
 * @implements {cljs.core.IMap}
 * @implements {cljs.core.ILookup}
*/
flatland.ordered.map.OrderedMap = (function (kvs,ks){
this.kvs = kvs;
this.ks = ks;
this.cljs$lang$protocol_mask$partition0$ = 2297825039;
this.cljs$lang$protocol_mask$partition1$ = 8192;
});
(flatland.ordered.map.OrderedMap.prototype.cljs$core$IFind$ = cljs.core.PROTOCOL_SENTINEL);

(flatland.ordered.map.OrderedMap.prototype.cljs$core$IFind$_find$arity$2 = (function (this$,k){
var self__ = this;
var this$__$1 = this;
return cljs.core._find(self__.kvs,k);
}));

(flatland.ordered.map.OrderedMap.prototype.entry_set = (function (){
var self__ = this;
var this$ = this;
return cljs.core.to_array(cljs.core.map.cljs$core$IFn$_invoke$arity$2(cljs.core.to_array,self__.kvs));
}));

(flatland.ordered.map.OrderedMap.prototype.forEach = (function() {
var G__37643 = null;
var G__37643__1 = (function (f){
var self__ = this;
var this$ = this;
var seq__37521 = cljs.core.seq(self__.ks);
var chunk__37522 = null;
var count__37523 = (0);
var i__37524 = (0);
while(true){
if((i__37524 < count__37523)){
var k = chunk__37522.cljs$core$IIndexed$_nth$arity$2(null,i__37524);
var G__37533_37644 = k;
var G__37534_37645 = cljs.core.get.cljs$core$IFn$_invoke$arity$2(self__.kvs,k);
var G__37535_37646 = this$;
(f.cljs$core$IFn$_invoke$arity$3 ? f.cljs$core$IFn$_invoke$arity$3(G__37533_37644,G__37534_37645,G__37535_37646) : f.call(null,G__37533_37644,G__37534_37645,G__37535_37646));


var G__37647 = seq__37521;
var G__37648 = chunk__37522;
var G__37649 = count__37523;
var G__37650 = (i__37524 + (1));
seq__37521 = G__37647;
chunk__37522 = G__37648;
count__37523 = G__37649;
i__37524 = G__37650;
continue;
} else {
var temp__5804__auto__ = cljs.core.seq(seq__37521);
if(temp__5804__auto__){
var seq__37521__$1 = temp__5804__auto__;
if(cljs.core.chunked_seq_QMARK_(seq__37521__$1)){
var c__5673__auto__ = cljs.core.chunk_first(seq__37521__$1);
var G__37651 = cljs.core.chunk_rest(seq__37521__$1);
var G__37652 = c__5673__auto__;
var G__37653 = cljs.core.count(c__5673__auto__);
var G__37654 = (0);
seq__37521 = G__37651;
chunk__37522 = G__37652;
count__37523 = G__37653;
i__37524 = G__37654;
continue;
} else {
var k = cljs.core.first(seq__37521__$1);
var G__37536_37655 = k;
var G__37537_37656 = cljs.core.get.cljs$core$IFn$_invoke$arity$2(self__.kvs,k);
var G__37538_37657 = this$;
(f.cljs$core$IFn$_invoke$arity$3 ? f.cljs$core$IFn$_invoke$arity$3(G__37536_37655,G__37537_37656,G__37538_37657) : f.call(null,G__37536_37655,G__37537_37656,G__37538_37657));


var G__37658 = cljs.core.next(seq__37521__$1);
var G__37659 = null;
var G__37660 = (0);
var G__37661 = (0);
seq__37521 = G__37658;
chunk__37522 = G__37659;
count__37523 = G__37660;
i__37524 = G__37661;
continue;
}
} else {
return null;
}
}
break;
}
});
var G__37643__2 = (function (f,use_as_this){
var self__ = this;
var this$ = this;
var seq__37539 = cljs.core.seq(self__.ks);
var chunk__37540 = null;
var count__37541 = (0);
var i__37542 = (0);
while(true){
if((i__37542 < count__37541)){
var k = chunk__37540.cljs$core$IIndexed$_nth$arity$2(null,i__37542);
f.call(use_as_this,k,cljs.core.get.cljs$core$IFn$_invoke$arity$2(self__.kvs,k),this$);


var G__37662 = seq__37539;
var G__37663 = chunk__37540;
var G__37664 = count__37541;
var G__37665 = (i__37542 + (1));
seq__37539 = G__37662;
chunk__37540 = G__37663;
count__37541 = G__37664;
i__37542 = G__37665;
continue;
} else {
var temp__5804__auto__ = cljs.core.seq(seq__37539);
if(temp__5804__auto__){
var seq__37539__$1 = temp__5804__auto__;
if(cljs.core.chunked_seq_QMARK_(seq__37539__$1)){
var c__5673__auto__ = cljs.core.chunk_first(seq__37539__$1);
var G__37666 = cljs.core.chunk_rest(seq__37539__$1);
var G__37667 = c__5673__auto__;
var G__37668 = cljs.core.count(c__5673__auto__);
var G__37669 = (0);
seq__37539 = G__37666;
chunk__37540 = G__37667;
count__37541 = G__37668;
i__37542 = G__37669;
continue;
} else {
var k = cljs.core.first(seq__37539__$1);
f.call(use_as_this,k,cljs.core.get.cljs$core$IFn$_invoke$arity$2(self__.kvs,k),this$);


var G__37670 = cljs.core.next(seq__37539__$1);
var G__37671 = null;
var G__37672 = (0);
var G__37673 = (0);
seq__37539 = G__37670;
chunk__37540 = G__37671;
count__37541 = G__37672;
i__37542 = G__37673;
continue;
}
} else {
return null;
}
}
break;
}
});
G__37643 = function(f,use_as_this){
switch(arguments.length){
case 1:
return G__37643__1.call(this,f);
case 2:
return G__37643__2.call(this,f,use_as_this);
}
throw(new Error('Invalid arity: ' + arguments.length));
};
G__37643.cljs$core$IFn$_invoke$arity$1 = G__37643__1;
G__37643.cljs$core$IFn$_invoke$arity$2 = G__37643__2;
return G__37643;
})()
);

(flatland.ordered.map.OrderedMap.prototype.get = (function (k){
var self__ = this;
var this$ = this;
return self__.kvs.get(k);
}));

(flatland.ordered.map.OrderedMap.prototype.key_set = (function (){
var self__ = this;
var this$ = this;
return cljs.core.to_array(cljs.core.keys(self__.kvs));
}));

(flatland.ordered.map.OrderedMap.prototype.entries = (function (){
var self__ = this;
var this$ = this;
return cljs.core.es6_entries_iterator(cljs.core.seq(self__.kvs));
}));

(flatland.ordered.map.OrderedMap.prototype.value_set = (function (){
var self__ = this;
var this$ = this;
return cljs.core.to_array(cljs.core.map.cljs$core$IFn$_invoke$arity$2(cljs.core.val,self__.kvs));
}));

(flatland.ordered.map.OrderedMap.prototype.toString = (function (){
var self__ = this;
var this$ = this;
return cljs.core.pr_str_STAR_(this$);
}));

(flatland.ordered.map.OrderedMap.prototype.keys = (function (){
var self__ = this;
var this$ = this;
return cljs.core.es6_iterator(self__.ks);
}));

(flatland.ordered.map.OrderedMap.prototype.values = (function (){
var self__ = this;
var this$ = this;
return cljs.core.es6_iterator(cljs.core.vals(self__.kvs));
}));

(flatland.ordered.map.OrderedMap.prototype.equiv = (function (that){
var self__ = this;
var this$ = this;
return (flatland.ordered.map.equiv_impl.cljs$core$IFn$_invoke$arity$2 ? flatland.ordered.map.equiv_impl.cljs$core$IFn$_invoke$arity$2(self__.kvs,that) : flatland.ordered.map.equiv_impl.call(null,self__.kvs,that));
}));

(flatland.ordered.map.OrderedMap.prototype.has = (function (k){
var self__ = this;
var this$ = this;
return (!((self__.kvs.get(k) == null)));
}));

(flatland.ordered.map.OrderedMap.prototype.cljs$core$ILookup$_lookup$arity$2 = (function (this$,attr){
var self__ = this;
var this$__$1 = this;
return cljs.core._lookup(self__.kvs,attr);
}));

(flatland.ordered.map.OrderedMap.prototype.cljs$core$ILookup$_lookup$arity$3 = (function (this$,attr,not_found){
var self__ = this;
var this$__$1 = this;
return cljs.core._lookup(self__.kvs,attr,not_found);
}));

(flatland.ordered.map.OrderedMap.prototype.cljs$core$IKVReduce$_kv_reduce$arity$3 = (function (coll,f,init){
var self__ = this;
var coll__$1 = this;
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (acc,k){
var G__37587 = acc;
var G__37588 = k;
var G__37589 = cljs.core.get.cljs$core$IFn$_invoke$arity$2(self__.kvs,k);
return (f.cljs$core$IFn$_invoke$arity$3 ? f.cljs$core$IFn$_invoke$arity$3(G__37587,G__37588,G__37589) : f.call(null,G__37587,G__37588,G__37589));
}),init,self__.ks);
}));

(flatland.ordered.map.OrderedMap.prototype.cljs$core$IPrintWithWriter$_pr_writer$arity$3 = (function (_,writer,opts){
var self__ = this;
var ___$1 = this;
cljs.core._write(writer,"#ordered/map ");

return flatland.ordered.map.print_ordered_map(writer,self__.kvs,self__.ks,opts);
}));

(flatland.ordered.map.OrderedMap.prototype.cljs$core$IMeta$_meta$arity$1 = (function (this$){
var self__ = this;
var this$__$1 = this;
return cljs.core.meta(self__.kvs);
}));

(flatland.ordered.map.OrderedMap.prototype.cljs$core$ICloneable$_clone$arity$1 = (function (_){
var self__ = this;
var ___$1 = this;
return (new flatland.ordered.map.OrderedMap(self__.kvs,self__.ks));
}));

(flatland.ordered.map.OrderedMap.prototype.cljs$core$ICounted$_count$arity$1 = (function (this$){
var self__ = this;
var this$__$1 = this;
return cljs.core.count(self__.kvs);
}));

(flatland.ordered.map.OrderedMap.prototype.cljs$core$IReversible$_rseq$arity$1 = (function (this$){
var self__ = this;
var this$__$1 = this;
if(cljs.core.seq(self__.ks)){
return cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (p1__37518_SHARP_){
return cljs.core._find(self__.kvs,p1__37518_SHARP_);
}),cljs.core.rseq(self__.ks));
} else {
return null;
}
}));

(flatland.ordered.map.OrderedMap.prototype.cljs$core$IHash$_hash$arity$1 = (function (_){
var self__ = this;
var ___$1 = this;
return cljs.core.hash(self__.kvs);
}));

(flatland.ordered.map.OrderedMap.prototype.cljs$core$IEquiv$_equiv$arity$2 = (function (this$,that){
var self__ = this;
var this$__$1 = this;
return (flatland.ordered.map.equiv_impl.cljs$core$IFn$_invoke$arity$2 ? flatland.ordered.map.equiv_impl.cljs$core$IFn$_invoke$arity$2(self__.kvs,that) : flatland.ordered.map.equiv_impl.call(null,self__.kvs,that));
}));

(flatland.ordered.map.OrderedMap.prototype.cljs$core$IEmptyableCollection$_empty$arity$1 = (function (this$){
var self__ = this;
var this$__$1 = this;
if(cljs.core.seq(self__.ks)){
return (new flatland.ordered.map.OrderedMap(cljs.core._empty(self__.kvs),cljs.core.PersistentVector.EMPTY));
} else {
return this$__$1;
}
}));

(flatland.ordered.map.OrderedMap.prototype.cljs$core$IMap$_dissoc$arity$2 = (function (this$,k){
var self__ = this;
var this$__$1 = this;
if(cljs.core.contains_QMARK_(self__.kvs,k)){
return (new flatland.ordered.map.OrderedMap(cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(self__.kvs,k),cljs.core.into.cljs$core$IFn$_invoke$arity$3(cljs.core.PersistentVector.EMPTY,cljs.core.remove.cljs$core$IFn$_invoke$arity$1(cljs.core.PersistentHashSet.createAsIfByAssoc([k])),self__.ks)));
} else {
return this$__$1;
}
}));

(flatland.ordered.map.OrderedMap.prototype.cljs$core$IAssociative$_assoc$arity$3 = (function (coll,k,v){
var self__ = this;
var coll__$1 = this;
return (new flatland.ordered.map.OrderedMap(cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(self__.kvs,k,v),((cljs.core.contains_QMARK_(self__.kvs,k))?self__.ks:cljs.core.conj.cljs$core$IFn$_invoke$arity$2(self__.ks,k))));
}));

(flatland.ordered.map.OrderedMap.prototype.cljs$core$IAssociative$_contains_key_QMARK_$arity$2 = (function (this$,k){
var self__ = this;
var this$__$1 = this;
return cljs.core.contains_QMARK_(self__.kvs,k);
}));

(flatland.ordered.map.OrderedMap.prototype.cljs$core$ISeqable$_seq$arity$1 = (function (this$){
var self__ = this;
var this$__$1 = this;
if(cljs.core.seq(self__.ks)){
return cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (p1__37517_SHARP_){
return cljs.core._find(self__.kvs,p1__37517_SHARP_);
}),self__.ks);
} else {
return null;
}
}));

(flatland.ordered.map.OrderedMap.prototype.cljs$core$IWithMeta$_with_meta$arity$2 = (function (this$,new_meta){
var self__ = this;
var this$__$1 = this;
if((cljs.core.meta(self__.kvs) === new_meta)){
return this$__$1;
} else {
return (new flatland.ordered.map.OrderedMap(cljs.core.with_meta(self__.kvs,new_meta),self__.ks));
}
}));

(flatland.ordered.map.OrderedMap.prototype.cljs$core$ICollection$_conj$arity$2 = (function (coll,entry){
var self__ = this;
var coll__$1 = this;
if(cljs.core.vector_QMARK_(entry)){
return (new flatland.ordered.map.OrderedMap(cljs.core.conj.cljs$core$IFn$_invoke$arity$2(self__.kvs,entry),((cljs.core.contains_QMARK_(self__.kvs,cljs.core._nth(entry,(0))))?self__.ks:cljs.core.conj.cljs$core$IFn$_invoke$arity$2(self__.ks,cljs.core._nth(entry,(0))))));
} else {
return (new flatland.ordered.map.OrderedMap(cljs.core.conj.cljs$core$IFn$_invoke$arity$2(self__.kvs,entry),cljs.core.into.cljs$core$IFn$_invoke$arity$3(self__.ks,cljs.core.comp.cljs$core$IFn$_invoke$arity$2(cljs.core.map.cljs$core$IFn$_invoke$arity$1((function (p1__37515_SHARP_){
return cljs.core._nth(p1__37515_SHARP_,(0));
})),cljs.core.remove.cljs$core$IFn$_invoke$arity$1((function (p1__37516_SHARP_){
return cljs.core.contains_QMARK_(self__.kvs,p1__37516_SHARP_);
}))),entry)));
}
}));

(flatland.ordered.map.OrderedMap.prototype.call = (function (unused__11865__auto__){
var self__ = this;
var self__ = this;
var G__37633 = (arguments.length - (1));
switch (G__37633) {
case (1):
return self__.cljs$core$IFn$_invoke$arity$1((arguments[(1)]));

break;
case (2):
return self__.cljs$core$IFn$_invoke$arity$2((arguments[(1)]),(arguments[(2)]));

break;
default:
throw (new Error((""+"Invalid arity: "+cljs.core.str.cljs$core$IFn$_invoke$arity$1((arguments.length - (1))))));

}
}));

(flatland.ordered.map.OrderedMap.prototype.apply = (function (self__,args37519){
var self__ = this;
var self____$1 = this;
return self____$1.call.apply(self____$1,[self____$1].concat(cljs.core.aclone(args37519)));
}));

(flatland.ordered.map.OrderedMap.prototype.cljs$core$IFn$_invoke$arity$1 = (function (k){
var self__ = this;
var this$ = this;
return (self__.kvs.cljs$core$IFn$_invoke$arity$1 ? self__.kvs.cljs$core$IFn$_invoke$arity$1(k) : self__.kvs.call(null,k));
}));

(flatland.ordered.map.OrderedMap.prototype.cljs$core$IFn$_invoke$arity$2 = (function (k,not_found){
var self__ = this;
var this$ = this;
return (self__.kvs.cljs$core$IFn$_invoke$arity$2 ? self__.kvs.cljs$core$IFn$_invoke$arity$2(k,not_found) : self__.kvs.call(null,k,not_found));
}));

(flatland.ordered.map.OrderedMap.getBasis = (function (){
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"kvs","kvs",-1695980277,null),new cljs.core.Symbol(null,"ks","ks",-754231827,null)], null);
}));

(flatland.ordered.map.OrderedMap.cljs$lang$type = true);

(flatland.ordered.map.OrderedMap.cljs$lang$ctorStr = "flatland.ordered.map/OrderedMap");

(flatland.ordered.map.OrderedMap.cljs$lang$ctorPrWriter = (function (this__5434__auto__,writer__5435__auto__,opt__5436__auto__){
return cljs.core._write(writer__5435__auto__,"flatland.ordered.map/OrderedMap");
}));

/**
 * Positional factory function for flatland.ordered.map/OrderedMap.
 */
flatland.ordered.map.__GT_OrderedMap = (function flatland$ordered$map$__GT_OrderedMap(kvs,ks){
return (new flatland.ordered.map.OrderedMap(kvs,ks));
});

flatland.ordered.map.equiv_impl = (function flatland$ordered$map$equiv_impl(kvs,that){
return cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(kvs,(((that instanceof flatland.ordered.map.OrderedMap))?that.kvs:that));
});
flatland.ordered.map.empty_ordered_map = (new flatland.ordered.map.OrderedMap(cljs.core.PersistentArrayMap.EMPTY,cljs.core.PersistentVector.EMPTY));
flatland.ordered.map.ordered_map = (function flatland$ordered$map$ordered_map(var_args){
var G__37642 = arguments.length;
switch (G__37642) {
case 0:
return flatland.ordered.map.ordered_map.cljs$core$IFn$_invoke$arity$0();

break;
case 1:
return flatland.ordered.map.ordered_map.cljs$core$IFn$_invoke$arity$1((arguments[(0)]));

break;
default:
var args_arr__5901__auto__ = [];
var len__5876__auto___37676 = arguments.length;
var i__5877__auto___37677 = (0);
while(true){
if((i__5877__auto___37677 < len__5876__auto___37676)){
args_arr__5901__auto__.push((arguments[i__5877__auto___37677]));

var G__37678 = (i__5877__auto___37677 + (1));
i__5877__auto___37677 = G__37678;
continue;
} else {
}
break;
}

var argseq__5902__auto__ = ((((2) < args_arr__5901__auto__.length))?(new cljs.core.IndexedSeq(args_arr__5901__auto__.slice((2)),(0),null)):null);
return flatland.ordered.map.ordered_map.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),(arguments[(1)]),argseq__5902__auto__);

}
});

(flatland.ordered.map.ordered_map.cljs$core$IFn$_invoke$arity$0 = (function (){
return flatland.ordered.map.empty_ordered_map;
}));

(flatland.ordered.map.ordered_map.cljs$core$IFn$_invoke$arity$1 = (function (coll){
return cljs.core.into.cljs$core$IFn$_invoke$arity$2(flatland.ordered.map.empty_ordered_map,coll);
}));

(flatland.ordered.map.ordered_map.cljs$core$IFn$_invoke$arity$variadic = (function (k,v,kvs){
return cljs.core.apply.cljs$core$IFn$_invoke$arity$5(cljs.core.assoc,flatland.ordered.map.empty_ordered_map,k,v,kvs);
}));

/** @this {Function} */
(flatland.ordered.map.ordered_map.cljs$lang$applyTo = (function (seq37639){
var G__37640 = cljs.core.first(seq37639);
var seq37639__$1 = cljs.core.next(seq37639);
var G__37641 = cljs.core.first(seq37639__$1);
var seq37639__$2 = cljs.core.next(seq37639__$1);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__37640,G__37641,seq37639__$2);
}));

(flatland.ordered.map.ordered_map.cljs$lang$maxFixedArity = (2));


//# sourceMappingURL=flatland.ordered.map.js.map
