import "./cljs_env.js";
import "./cljs.core.js";
import "./datascript.db.js";
import "./datascript.util.js";
import "./me.tonsky.persistent_sorted_set.protocol.js";
import "./me.tonsky.persistent_sorted_set.js";
import "./me.tonsky.persistent_sorted_set.arrays.js";
goog.provide('datascript.storage');

/**
 * @interface
 */
datascript.storage.IStorage = function(){};

var datascript$storage$IStorage$_store$dyn_37790 = (function (_,addr_PLUS_data_seq,delete_addrs){
var x__5498__auto__ = (((_ == null))?null:_);
var m__5499__auto__ = (datascript.storage._store[goog.typeOf(x__5498__auto__)]);
if((!((m__5499__auto__ == null)))){
return (m__5499__auto__.cljs$core$IFn$_invoke$arity$3 ? m__5499__auto__.cljs$core$IFn$_invoke$arity$3(_,addr_PLUS_data_seq,delete_addrs) : m__5499__auto__.call(null,_,addr_PLUS_data_seq,delete_addrs));
} else {
var m__5497__auto__ = (datascript.storage._store["_"]);
if((!((m__5497__auto__ == null)))){
return (m__5497__auto__.cljs$core$IFn$_invoke$arity$3 ? m__5497__auto__.cljs$core$IFn$_invoke$arity$3(_,addr_PLUS_data_seq,delete_addrs) : m__5497__auto__.call(null,_,addr_PLUS_data_seq,delete_addrs));
} else {
throw cljs.core.missing_protocol("IStorage.-store",_);
}
}
});
/**
 * Gives you a sequence of `[addr data]` pairs to serialize and store.
 * 
 *   `addr`s are 64 bit integers.
 *   `data`s are clojure-serializable data structure (maps, keywords, lists, integers etc)
 */
datascript.storage._store = (function datascript$storage$_store(_,addr_PLUS_data_seq,delete_addrs){
if((((!((_ == null)))) && ((!((_.datascript$storage$IStorage$_store$arity$3 == null)))))){
return _.datascript$storage$IStorage$_store$arity$3(_,addr_PLUS_data_seq,delete_addrs);
} else {
return datascript$storage$IStorage$_store$dyn_37790(_,addr_PLUS_data_seq,delete_addrs);
}
});

var datascript$storage$IStorage$_restore$dyn_37791 = (function (_,addr){
var x__5498__auto__ = (((_ == null))?null:_);
var m__5499__auto__ = (datascript.storage._restore[goog.typeOf(x__5498__auto__)]);
if((!((m__5499__auto__ == null)))){
return (m__5499__auto__.cljs$core$IFn$_invoke$arity$2 ? m__5499__auto__.cljs$core$IFn$_invoke$arity$2(_,addr) : m__5499__auto__.call(null,_,addr));
} else {
var m__5497__auto__ = (datascript.storage._restore["_"]);
if((!((m__5497__auto__ == null)))){
return (m__5497__auto__.cljs$core$IFn$_invoke$arity$2 ? m__5497__auto__.cljs$core$IFn$_invoke$arity$2(_,addr) : m__5497__auto__.call(null,_,addr));
} else {
throw cljs.core.missing_protocol("IStorage.-restore",_);
}
}
});
/**
 * Read back and deserialize data stored under single `addr`
 */
datascript.storage._restore = (function datascript$storage$_restore(_,addr){
if((((!((_ == null)))) && ((!((_.datascript$storage$IStorage$_restore$arity$2 == null)))))){
return _.datascript$storage$IStorage$_restore$arity$2(_,addr);
} else {
return datascript$storage$IStorage$_restore$dyn_37791(_,addr);
}
});

datascript.storage._STAR_delete_buffer = cljs.core.atom.cljs$core$IFn$_invoke$arity$1(cljs.core.PersistentArrayMap.EMPTY);
datascript.storage.serializable_datom = (function datascript$storage$serializable_datom(d){
return new cljs.core.PersistentVector(null, 4, 5, cljs.core.PersistentVector.EMPTY_NODE, [d.e,d.a,d.v,d.tx], null);
});
datascript.storage.root_addr = (0);
datascript.storage.tail_addr = (1);
if((typeof datascript !== 'undefined') && (typeof datascript.storage !== 'undefined') && (typeof datascript.storage._STAR_max_addr !== 'undefined')){
} else {
datascript.storage._STAR_max_addr = cljs.core.volatile_BANG_((1000000));
}
datascript.storage.gen_addr = (function datascript$storage$gen_addr(){
return datascript.storage._STAR_max_addr.cljs$core$IVolatile$_vreset_BANG_$arity$2(null,(datascript.storage._STAR_max_addr.cljs$core$IDeref$_deref$arity$1(null) + (1)));
});

/**
* @constructor
 * @implements {cljs.core.IRecord}
 * @implements {cljs.core.IKVReduce}
 * @implements {cljs.core.IEquiv}
 * @implements {cljs.core.IHash}
 * @implements {cljs.core.ICollection}
 * @implements {cljs.core.ICounted}
 * @implements {cljs.core.ISeqable}
 * @implements {cljs.core.IMeta}
 * @implements {cljs.core.ICloneable}
 * @implements {cljs.core.IPrintWithWriter}
 * @implements {cljs.core.IIterable}
 * @implements {me.tonsky.persistent_sorted_set.protocol.IStorage}
 * @implements {cljs.core.IWithMeta}
 * @implements {cljs.core.IAssociative}
 * @implements {cljs.core.IMap}
 * @implements {cljs.core.ILookup}
*/
datascript.storage.StorageAdapter = (function (storage,__meta,__extmap,__hash){
this.storage = storage;
this.__meta = __meta;
this.__extmap = __extmap;
this.__hash = __hash;
this.cljs$lang$protocol_mask$partition0$ = 2230716170;
this.cljs$lang$protocol_mask$partition1$ = 139264;
});
(datascript.storage.StorageAdapter.prototype.cljs$core$ILookup$_lookup$arity$2 = (function (this__5448__auto__,k__5449__auto__){
var self__ = this;
var this__5448__auto____$1 = this;
return this__5448__auto____$1.cljs$core$ILookup$_lookup$arity$3(null,k__5449__auto__,null);
}));

(datascript.storage.StorageAdapter.prototype.cljs$core$ILookup$_lookup$arity$3 = (function (this__5450__auto__,k37696,else__5451__auto__){
var self__ = this;
var this__5450__auto____$1 = this;
var G__37704 = k37696;
var G__37704__$1 = (((G__37704 instanceof cljs.core.Keyword))?G__37704.fqn:null);
switch (G__37704__$1) {
case "storage":
return self__.storage;

break;
default:
return cljs.core.get.cljs$core$IFn$_invoke$arity$3(self__.__extmap,k37696,else__5451__auto__);

}
}));

(datascript.storage.StorageAdapter.prototype.cljs$core$IKVReduce$_kv_reduce$arity$3 = (function (this__5468__auto__,f__5469__auto__,init__5470__auto__){
var self__ = this;
var this__5468__auto____$1 = this;
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (ret__5471__auto__,p__37705){
var vec__37706 = p__37705;
var k__5472__auto__ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__37706,(0),null);
var v__5473__auto__ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__37706,(1),null);
return (f__5469__auto__.cljs$core$IFn$_invoke$arity$3 ? f__5469__auto__.cljs$core$IFn$_invoke$arity$3(ret__5471__auto__,k__5472__auto__,v__5473__auto__) : f__5469__auto__.call(null,ret__5471__auto__,k__5472__auto__,v__5473__auto__));
}),init__5470__auto__,this__5468__auto____$1);
}));

(datascript.storage.StorageAdapter.prototype.cljs$core$IPrintWithWriter$_pr_writer$arity$3 = (function (this__5463__auto__,writer__5464__auto__,opts__5465__auto__){
var self__ = this;
var this__5463__auto____$1 = this;
var pr_pair__5466__auto__ = (function (keyval__5467__auto__){
return cljs.core.pr_sequential_writer(writer__5464__auto__,cljs.core.pr_writer,""," ","",opts__5465__auto__,keyval__5467__auto__);
});
return cljs.core.pr_sequential_writer(writer__5464__auto__,pr_pair__5466__auto__,"#datascript.storage.StorageAdapter{",", ","}",opts__5465__auto__,cljs.core.concat.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"storage","storage",1867247511),self__.storage],null))], null),self__.__extmap));
}));

(datascript.storage.StorageAdapter.prototype.cljs$core$IIterable$_iterator$arity$1 = (function (G__37695){
var self__ = this;
var G__37695__$1 = this;
return (new cljs.core.RecordIter((0),G__37695__$1,1,new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"storage","storage",1867247511)], null),(cljs.core.truth_(self__.__extmap)?cljs.core._iterator(self__.__extmap):cljs.core.nil_iter())));
}));

(datascript.storage.StorageAdapter.prototype.cljs$core$IMeta$_meta$arity$1 = (function (this__5446__auto__){
var self__ = this;
var this__5446__auto____$1 = this;
return self__.__meta;
}));

(datascript.storage.StorageAdapter.prototype.cljs$core$ICloneable$_clone$arity$1 = (function (this__5443__auto__){
var self__ = this;
var this__5443__auto____$1 = this;
return (new datascript.storage.StorageAdapter(self__.storage,self__.__meta,self__.__extmap,self__.__hash));
}));

(datascript.storage.StorageAdapter.prototype.cljs$core$ICounted$_count$arity$1 = (function (this__5452__auto__){
var self__ = this;
var this__5452__auto____$1 = this;
return (1 + cljs.core.count(self__.__extmap));
}));

(datascript.storage.StorageAdapter.prototype.cljs$core$IHash$_hash$arity$1 = (function (this__5444__auto__){
var self__ = this;
var this__5444__auto____$1 = this;
var h__5251__auto__ = self__.__hash;
if((!((h__5251__auto__ == null)))){
return h__5251__auto__;
} else {
var h__5251__auto____$1 = (function (coll__5445__auto__){
return (1627330428 ^ cljs.core.hash_unordered_coll(coll__5445__auto__));
})(this__5444__auto____$1);
(self__.__hash = h__5251__auto____$1);

return h__5251__auto____$1;
}
}));

(datascript.storage.StorageAdapter.prototype.cljs$core$IEquiv$_equiv$arity$2 = (function (this37697,other37698){
var self__ = this;
var this37697__$1 = this;
return (((!((other37698 == null)))) && ((((this37697__$1.constructor === other37698.constructor)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this37697__$1.storage,other37698.storage)) && (cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this37697__$1.__extmap,other37698.__extmap)))))));
}));

(datascript.storage.StorageAdapter.prototype.cljs$core$IMap$_dissoc$arity$2 = (function (this__5458__auto__,k__5459__auto__){
var self__ = this;
var this__5458__auto____$1 = this;
if(cljs.core.contains_QMARK_(new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"storage","storage",1867247511),null], null), null),k__5459__auto__)){
return cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(cljs.core._with_meta(cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,this__5458__auto____$1),self__.__meta),k__5459__auto__);
} else {
return (new datascript.storage.StorageAdapter(self__.storage,self__.__meta,cljs.core.not_empty(cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(self__.__extmap,k__5459__auto__)),null));
}
}));

(datascript.storage.StorageAdapter.prototype.cljs$core$IAssociative$_contains_key_QMARK_$arity$2 = (function (this__5455__auto__,k37696){
var self__ = this;
var this__5455__auto____$1 = this;
var G__37732 = k37696;
var G__37732__$1 = (((G__37732 instanceof cljs.core.Keyword))?G__37732.fqn:null);
switch (G__37732__$1) {
case "storage":
return true;

break;
default:
return cljs.core.contains_QMARK_(self__.__extmap,k37696);

}
}));

(datascript.storage.StorageAdapter.prototype.cljs$core$IAssociative$_assoc$arity$3 = (function (this__5456__auto__,k__5457__auto__,G__37695){
var self__ = this;
var this__5456__auto____$1 = this;
var pred__37734 = cljs.core.keyword_identical_QMARK_;
var expr__37735 = k__5457__auto__;
if(cljs.core.truth_((pred__37734.cljs$core$IFn$_invoke$arity$2 ? pred__37734.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"storage","storage",1867247511),expr__37735) : pred__37734.call(null,new cljs.core.Keyword(null,"storage","storage",1867247511),expr__37735)))){
return (new datascript.storage.StorageAdapter(G__37695,self__.__meta,self__.__extmap,null));
} else {
return (new datascript.storage.StorageAdapter(self__.storage,self__.__meta,cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(self__.__extmap,k__5457__auto__,G__37695),null));
}
}));

(datascript.storage.StorageAdapter.prototype.cljs$core$ISeqable$_seq$arity$1 = (function (this__5461__auto__){
var self__ = this;
var this__5461__auto____$1 = this;
return cljs.core.seq(cljs.core.concat.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new cljs.core.MapEntry(new cljs.core.Keyword(null,"storage","storage",1867247511),self__.storage,null))], null),self__.__extmap));
}));

(datascript.storage.StorageAdapter.prototype.cljs$core$IWithMeta$_with_meta$arity$2 = (function (this__5447__auto__,G__37695){
var self__ = this;
var this__5447__auto____$1 = this;
return (new datascript.storage.StorageAdapter(self__.storage,G__37695,self__.__extmap,self__.__hash));
}));

(datascript.storage.StorageAdapter.prototype.cljs$core$ICollection$_conj$arity$2 = (function (this__5453__auto__,entry__5454__auto__){
var self__ = this;
var this__5453__auto____$1 = this;
if(cljs.core.vector_QMARK_(entry__5454__auto__)){
return this__5453__auto____$1.cljs$core$IAssociative$_assoc$arity$3(null,cljs.core._nth(entry__5454__auto__,(0)),cljs.core._nth(entry__5454__auto__,(1)));
} else {
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(cljs.core._conj,this__5453__auto____$1,entry__5454__auto__);
}
}));

(datascript.storage.StorageAdapter.prototype.me$tonsky$persistent_sorted_set$protocol$IStorage$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.storage.StorageAdapter.prototype.me$tonsky$persistent_sorted_set$protocol$IStorage$store$arity$3 = (function (_,node,address){
var self__ = this;
var ___$1 = this;
var addr = (cljs.core.truth_((function (){var and__5140__auto__ = address;
if(cljs.core.truth_(and__5140__auto__)){
return (((node instanceof me.tonsky.persistent_sorted_set.Node)) && (cljs.core.contains_QMARK_(cljs.core.set(node._addresses),address)));
} else {
return and__5140__auto__;
}
})())?datascript.storage.gen_addr():(function (){var or__5142__auto__ = address;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return datascript.storage.gen_addr();
}
})()
);
var keys = cljs.core.mapv.cljs$core$IFn$_invoke$arity$2(datascript.storage.serializable_datom,node.keys);
var data = (function (){var G__37740 = new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"keys","keys",1068423698),keys], null);
if((node instanceof me.tonsky.persistent_sorted_set.Node)){
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__37740,new cljs.core.Keyword(null,"addresses","addresses",-559529694),node._addresses);
} else {
return G__37740;
}
})();
cljs.core._vreset_BANG_(datascript.storage._STAR_store_buffer_STAR_,cljs.core.conj_BANG_.cljs$core$IFn$_invoke$arity$2(cljs.core._deref(datascript.storage._STAR_store_buffer_STAR_),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [addr,data], null)));

return addr;
}));

(datascript.storage.StorageAdapter.prototype.me$tonsky$persistent_sorted_set$protocol$IStorage$restore$arity$2 = (function (_,addr){
var self__ = this;
var ___$1 = this;
if(cljs.core.truth_(addr)){
var map__37742 = datascript.storage._restore(self__.storage,addr);
var map__37742__$1 = cljs.core.__destructure_map(map__37742);
var keys = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__37742__$1,new cljs.core.Keyword(null,"keys","keys",1068423698));
var addresses = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__37742__$1,new cljs.core.Keyword(null,"addresses","addresses",-559529694));
if(cljs.core.truth_(keys)){
var keys_SINGLEQUOTE_ = me.tonsky.persistent_sorted_set.arrays.into_array(cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (p__37744){
var vec__37745 = p__37744;
var e = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__37745,(0),null);
var a = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__37745,(1),null);
var v = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__37745,(2),null);
var tx = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__37745,(3),null);
return datascript.db.datom.cljs$core$IFn$_invoke$arity$4(e,a,v,tx);
}),keys));
if(cljs.core.truth_(addresses)){
var children = me.tonsky.persistent_sorted_set.arrays.make_array(cljs.core.count(addresses));
return me.tonsky.persistent_sorted_set.new_node.cljs$core$IFn$_invoke$arity$5(keys_SINGLEQUOTE_,children,addresses,addr,false);
} else {
return me.tonsky.persistent_sorted_set.new_leaf.cljs$core$IFn$_invoke$arity$3(keys_SINGLEQUOTE_,addr,false);
}
} else {
return null;
}
} else {
return null;
}
}));

(datascript.storage.StorageAdapter.prototype.me$tonsky$persistent_sorted_set$protocol$IStorage$accessed$arity$2 = (function (_,_addr){
var self__ = this;
var ___$1 = this;
return null;
}));

(datascript.storage.StorageAdapter.prototype.me$tonsky$persistent_sorted_set$protocol$IStorage$delete$arity$2 = (function (_,unused_addresses){
var self__ = this;
var ___$1 = this;
return cljs.core.swap_BANG_.cljs$core$IFn$_invoke$arity$4(datascript.storage._STAR_delete_buffer,cljs.core.update,self__.storage,(function (buffer){
return cljs.core.into.cljs$core$IFn$_invoke$arity$2(buffer,cljs.core.remove.cljs$core$IFn$_invoke$arity$2(cljs.core.nil_QMARK_,unused_addresses));
}));
}));

(datascript.storage.StorageAdapter.getBasis = (function (){
return new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"storage","storage",-787188258,null)], null);
}));

(datascript.storage.StorageAdapter.cljs$lang$type = true);

(datascript.storage.StorageAdapter.cljs$lang$ctorPrSeq = (function (this__5494__auto__){
return (new cljs.core.List(null,"datascript.storage/StorageAdapter",null,(1),null));
}));

(datascript.storage.StorageAdapter.cljs$lang$ctorPrWriter = (function (this__5494__auto__,writer__5495__auto__){
return cljs.core._write(writer__5495__auto__,"datascript.storage/StorageAdapter");
}));

/**
 * Positional factory function for datascript.storage/StorageAdapter.
 */
datascript.storage.__GT_StorageAdapter = (function datascript$storage$__GT_StorageAdapter(storage){
return (new datascript.storage.StorageAdapter(storage,null,null,null));
});

/**
 * Factory function for datascript.storage/StorageAdapter, taking a map of keywords to field values.
 */
datascript.storage.map__GT_StorageAdapter = (function datascript$storage$map__GT_StorageAdapter(G__37703){
var extmap__5490__auto__ = (function (){var G__37749 = cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(G__37703,new cljs.core.Keyword(null,"storage","storage",1867247511));
if(cljs.core.record_QMARK_(G__37703)){
return cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,G__37749);
} else {
return G__37749;
}
})();
return (new datascript.storage.StorageAdapter(new cljs.core.Keyword(null,"storage","storage",1867247511).cljs$core$IFn$_invoke$arity$1(G__37703),null,cljs.core.not_empty(extmap__5490__auto__),null));
});

datascript.storage.make_storage_adapter = (function datascript$storage$make_storage_adapter(storage,_opts){
return (new datascript.storage.StorageAdapter(storage,null,null,null));
});
datascript.storage.storage_adapter = (function datascript$storage$storage_adapter(db){
if(cljs.core.truth_(db)){
return new cljs.core.Keyword(null,"eavt","eavt",-666437073).cljs$core$IFn$_invoke$arity$1(db).storage;
} else {
return null;
}
});
datascript.storage.storage = (function datascript$storage$storage(db){
var temp__5808__auto__ = datascript.storage.storage_adapter(db);
if((temp__5808__auto__ == null)){
return null;
} else {
var adapter = temp__5808__auto__;
return new cljs.core.Keyword(null,"storage","storage",1867247511).cljs$core$IFn$_invoke$arity$1(adapter);
}
});
datascript.storage.stored_dbs = cljs.core.atom.cljs$core$IFn$_invoke$arity$1(cljs.core.PersistentVector.EMPTY);
datascript.storage.remember_db = (function datascript$storage$remember_db(db){
return cljs.core.swap_BANG_.cljs$core$IFn$_invoke$arity$3(datascript.storage.stored_dbs,cljs.core.conj,(new WeakRef(db)));
});
datascript.storage.store_impl_BANG_ = (function datascript$storage$store_impl_BANG_(db,adapter,force_QMARK_){
datascript.storage.remember_db(db);

var _STAR_store_buffer_STAR__orig_val__37750 = datascript.storage._STAR_store_buffer_STAR_;
var _STAR_store_buffer_STAR__temp_val__37751 = cljs.core.volatile_BANG_(cljs.core.transient$(cljs.core.PersistentVector.EMPTY));
(datascript.storage._STAR_store_buffer_STAR_ = _STAR_store_buffer_STAR__temp_val__37751);

try{var eavt_set = new cljs.core.Keyword(null,"eavt","eavt",-666437073).cljs$core$IFn$_invoke$arity$1(db);
var aevt_set = new cljs.core.Keyword(null,"aevt","aevt",-585148059).cljs$core$IFn$_invoke$arity$1(db);
var avet_set = new cljs.core.Keyword(null,"avet","avet",1383857032).cljs$core$IFn$_invoke$arity$1(db);
var eavt_addr = me.tonsky.persistent_sorted_set.store.cljs$core$IFn$_invoke$arity$2(eavt_set,adapter);
var aevt_addr = me.tonsky.persistent_sorted_set.store.cljs$core$IFn$_invoke$arity$2(aevt_set,adapter);
var avet_addr = me.tonsky.persistent_sorted_set.store.cljs$core$IFn$_invoke$arity$2(avet_set,adapter);
var meta = cljs.core.merge.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([cljs.core.PersistentHashMap.fromArrays([new cljs.core.Keyword(null,"schema","schema",-1582001791),new cljs.core.Keyword(null,"max-tx","max-tx",1119558339),new cljs.core.Keyword(null,"aevt","aevt",-585148059),new cljs.core.Keyword(null,"max-addr","max-addr",1373100454),new cljs.core.Keyword(null,"avet","avet",1383857032),new cljs.core.Keyword(null,"max-eid","max-eid",2134868075),new cljs.core.Keyword(null,"eavt","eavt",-666437073),new cljs.core.Keyword(null,"aevt-metadata","aevt-metadata",-1844063817),new cljs.core.Keyword(null,"avet-metadata","avet-metadata",1556019837),new cljs.core.Keyword(null,"eavt-metadata","eavt-metadata",-1384819842)],[new cljs.core.Keyword(null,"schema","schema",-1582001791).cljs$core$IFn$_invoke$arity$1(db),new cljs.core.Keyword(null,"max-tx","max-tx",1119558339).cljs$core$IFn$_invoke$arity$1(db),aevt_addr,cljs.core.deref(datascript.storage._STAR_max_addr),avet_addr,new cljs.core.Keyword(null,"max-eid","max-eid",2134868075).cljs$core$IFn$_invoke$arity$1(db),eavt_addr,new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"count","count",2139924085),aevt_set.cnt,new cljs.core.Keyword(null,"shift","shift",997140064),aevt_set.shift], null),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"count","count",2139924085),avet_set.cnt,new cljs.core.Keyword(null,"shift","shift",997140064),avet_set.shift], null),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"count","count",2139924085),eavt_set.cnt,new cljs.core.Keyword(null,"shift","shift",997140064),eavt_set.shift], null)]),me.tonsky.persistent_sorted_set.settings(new cljs.core.Keyword(null,"eavt","eavt",-666437073).cljs$core$IFn$_invoke$arity$1(db))], 0));
if(cljs.core.truth_((function (){var or__5142__auto__ = force_QMARK_;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return (cljs.core.count(cljs.core.deref(datascript.storage._STAR_store_buffer_STAR_)) > (0));
}
})())){
cljs.core._vreset_BANG_(datascript.storage._STAR_store_buffer_STAR_,cljs.core.conj_BANG_.cljs$core$IFn$_invoke$arity$2(cljs.core._deref(datascript.storage._STAR_store_buffer_STAR_),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [datascript.storage.root_addr,meta], null)));

cljs.core._vreset_BANG_(datascript.storage._STAR_store_buffer_STAR_,cljs.core.conj_BANG_.cljs$core$IFn$_invoke$arity$2(cljs.core._deref(datascript.storage._STAR_store_buffer_STAR_),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [datascript.storage.tail_addr,cljs.core.PersistentVector.EMPTY], null)));

var storage_37804 = new cljs.core.Keyword(null,"storage","storage",1867247511).cljs$core$IFn$_invoke$arity$1(adapter);
var delete_addrs_37805 = cljs.core.distinct.cljs$core$IFn$_invoke$arity$1(cljs.core.get.cljs$core$IFn$_invoke$arity$2(cljs.core.deref(datascript.storage._STAR_delete_buffer),storage_37804));
var __37806 = cljs.core.swap_BANG_.cljs$core$IFn$_invoke$arity$4(datascript.storage._STAR_delete_buffer,cljs.core.assoc,storage_37804,null);
datascript.storage._store(storage_37804,cljs.core.persistent_BANG_(cljs.core.deref(datascript.storage._STAR_store_buffer_STAR_)),delete_addrs_37805);
} else {
}

return db;
}finally {(datascript.storage._STAR_store_buffer_STAR_ = _STAR_store_buffer_STAR__orig_val__37750);
}});
datascript.storage.store = (function datascript$storage$store(var_args){
var G__37758 = arguments.length;
switch (G__37758) {
case 1:
return datascript.storage.store.cljs$core$IFn$_invoke$arity$1((arguments[(0)]));

break;
case 2:
return datascript.storage.store.cljs$core$IFn$_invoke$arity$2((arguments[(0)]),(arguments[(1)]));

break;
default:
throw (new Error(["Invalid arity: ",arguments.length].join("")));

}
});

(datascript.storage.store.cljs$core$IFn$_invoke$arity$1 = (function (db){
var temp__5806__auto__ = datascript.storage.storage_adapter(db);
if((temp__5806__auto__ == null)){
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2("Database has no associated storage",cljs.core.PersistentArrayMap.EMPTY);
} else {
var adapter = temp__5806__auto__;
return datascript.storage.store_impl_BANG_(db,adapter,false);
}
}));

(datascript.storage.store.cljs$core$IFn$_invoke$arity$2 = (function (db,storage){
var temp__5806__auto__ = datascript.storage.storage_adapter(db);
if((temp__5806__auto__ == null)){
var settings = new cljs.core.Keyword(null,"eavt","eavt",-666437073).cljs$core$IFn$_invoke$arity$1(db)._settings;
var adapter = (new datascript.storage.StorageAdapter(storage,null,null,null));
return datascript.storage.store_impl_BANG_(db,adapter,false);
} else {
var adapter = temp__5806__auto__;
var current_storage = new cljs.core.Keyword(null,"storage","storage",1867247511).cljs$core$IFn$_invoke$arity$1(adapter);
if((current_storage === storage)){
return datascript.storage.store_impl_BANG_(db,adapter,false);
} else {
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2("Database is already stored with another IStorage",new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"storage","storage",1867247511),current_storage], null));
}
}
}));

(datascript.storage.store.cljs$lang$maxFixedArity = 2);

datascript.storage.store_tail = (function datascript$storage$store_tail(db,tail){
return datascript.storage._store(datascript.storage.storage(db),new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [datascript.storage.tail_addr,cljs.core.mapv.cljs$core$IFn$_invoke$arity$2((function (p1__37764_SHARP_){
return cljs.core.mapv.cljs$core$IFn$_invoke$arity$2(datascript.storage.serializable_datom,p1__37764_SHARP_);
}),tail)], null)], null),null);
});
datascript.storage.restore_impl = (function datascript$storage$restore_impl(storage,opts){
var temp__5808__auto__ = datascript.storage._restore(storage,datascript.storage.root_addr);
if((temp__5808__auto__ == null)){
return null;
} else {
var root = temp__5808__auto__;
var tail = datascript.storage._restore(storage,datascript.storage.tail_addr);
var map__37769 = root;
var map__37769__$1 = cljs.core.__destructure_map(map__37769);
var avet_metadata = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__37769__$1,new cljs.core.Keyword(null,"avet-metadata","avet-metadata",1556019837));
var eavt_metadata = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__37769__$1,new cljs.core.Keyword(null,"eavt-metadata","eavt-metadata",-1384819842));
var schema = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__37769__$1,new cljs.core.Keyword(null,"schema","schema",-1582001791));
var max_tx = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__37769__$1,new cljs.core.Keyword(null,"max-tx","max-tx",1119558339));
var aevt = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__37769__$1,new cljs.core.Keyword(null,"aevt","aevt",-585148059));
var max_addr = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__37769__$1,new cljs.core.Keyword(null,"max-addr","max-addr",1373100454));
var avet = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__37769__$1,new cljs.core.Keyword(null,"avet","avet",1383857032));
var max_eid = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__37769__$1,new cljs.core.Keyword(null,"max-eid","max-eid",2134868075));
var eavt = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__37769__$1,new cljs.core.Keyword(null,"eavt","eavt",-666437073));
var aevt_metadata = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__37769__$1,new cljs.core.Keyword(null,"aevt-metadata","aevt-metadata",-1844063817));
var _ = datascript.storage._STAR_max_addr.cljs$core$IVolatile$_vreset_BANG_$arity$2(null,cljs.core.max.cljs$core$IFn$_invoke$arity$2(datascript.storage._STAR_max_addr.cljs$core$IDeref$_deref$arity$1(null),max_addr));
var opts__$1 = cljs.core.merge.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([root,opts], 0));
var adapter = datascript.storage.make_storage_adapter(storage,opts__$1);
var db = datascript.db.restore_db(new cljs.core.PersistentArrayMap(null, 6, [new cljs.core.Keyword(null,"schema","schema",-1582001791),schema,new cljs.core.Keyword(null,"eavt","eavt",-666437073),me.tonsky.persistent_sorted_set.restore_by.cljs$core$IFn$_invoke$arity$4(datascript.db.cmp_datoms_eavt,eavt,adapter,cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(opts__$1,new cljs.core.Keyword(null,"set-metadata","set-metadata",1293757705),eavt_metadata)),new cljs.core.Keyword(null,"aevt","aevt",-585148059),me.tonsky.persistent_sorted_set.restore_by.cljs$core$IFn$_invoke$arity$4(datascript.db.cmp_datoms_aevt,aevt,adapter,cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(opts__$1,new cljs.core.Keyword(null,"set-metadata","set-metadata",1293757705),aevt_metadata)),new cljs.core.Keyword(null,"avet","avet",1383857032),me.tonsky.persistent_sorted_set.restore_by.cljs$core$IFn$_invoke$arity$4(datascript.db.cmp_datoms_avet,avet,adapter,cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(opts__$1,new cljs.core.Keyword(null,"set-metadata","set-metadata",1293757705),avet_metadata)),new cljs.core.Keyword(null,"max-eid","max-eid",2134868075),max_eid,new cljs.core.Keyword(null,"max-tx","max-tx",1119558339),max_tx], null));
datascript.storage.remember_db(db);

return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [db,cljs.core.mapv.cljs$core$IFn$_invoke$arity$2((function (p1__37766_SHARP_){
return cljs.core.keep.cljs$core$IFn$_invoke$arity$2((function (p__37775){
var vec__37776 = p__37775;
var e = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__37776,(0),null);
var a = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__37776,(1),null);
var v = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__37776,(2),null);
var tx = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__37776,(3),null);
var datoms = datascript.db._datoms(db,new cljs.core.Keyword(null,"eavt","eavt",-666437073),e,a,v,null);
var datom_exists_QMARK_ = cljs.core.some((function (datom){
return cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(tx,new cljs.core.Keyword(null,"tx","tx",466630418).cljs$core$IFn$_invoke$arity$1(datom));
}),datoms);
var added_QMARK_ = (tx > (0));
if(cljs.core.truth_((function (){var and__5140__auto__ = datom_exists_QMARK_;
if(cljs.core.truth_(and__5140__auto__)){
return added_QMARK_;
} else {
return and__5140__auto__;
}
})())){
return null;
} else {
return datascript.db.datom.cljs$core$IFn$_invoke$arity$4(e,a,v,tx);
}
}),p1__37766_SHARP_);
}),tail)], null);
}
});
datascript.storage.db_with_tail = (function datascript$storage$db_with_tail(db,tail){
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (db__$1,datoms){
if(cljs.core.empty_QMARK_(datoms)){
return db__$1;
} else {
try{var _PERCENT_ = db__$1;
var _PERCENT___$1 = cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(datascript.db.with_datom,_PERCENT_,datoms);
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(_PERCENT___$1,new cljs.core.Keyword(null,"max-tx","max-tx",1119558339),new cljs.core.Keyword(null,"tx","tx",466630418).cljs$core$IFn$_invoke$arity$1(cljs.core.first(datoms)));
}catch (e37783){var e = e37783;
console.error(e);

return db__$1;
}}
}),db,tail);
});
datascript.storage.restore = (function datascript$storage$restore(var_args){
var G__37786 = arguments.length;
switch (G__37786) {
case 1:
return datascript.storage.restore.cljs$core$IFn$_invoke$arity$1((arguments[(0)]));

break;
case 2:
return datascript.storage.restore.cljs$core$IFn$_invoke$arity$2((arguments[(0)]),(arguments[(1)]));

break;
default:
throw (new Error(["Invalid arity: ",arguments.length].join("")));

}
});

(datascript.storage.restore.cljs$core$IFn$_invoke$arity$1 = (function (storage){
return datascript.storage.restore.cljs$core$IFn$_invoke$arity$2(storage,cljs.core.PersistentArrayMap.EMPTY);
}));

(datascript.storage.restore.cljs$core$IFn$_invoke$arity$2 = (function (storage,opts){
var vec__37787 = datascript.storage.restore_impl(storage,opts);
var db = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__37787,(0),null);
var tail = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__37787,(1),null);
return datascript.storage.db_with_tail(db,tail);
}));

(datascript.storage.restore.cljs$lang$maxFixedArity = 2);

datascript.storage.maybe_adapt_storage = (function datascript$storage$maybe_adapt_storage(opts){
var temp__5806__auto__ = new cljs.core.Keyword(null,"storage","storage",1867247511).cljs$core$IFn$_invoke$arity$1(opts);
if((temp__5806__auto__ == null)){
return opts;
} else {
var storage = temp__5806__auto__;
return cljs.core.update.cljs$core$IFn$_invoke$arity$4(opts,new cljs.core.Keyword(null,"storage","storage",1867247511),datascript.storage.make_storage_adapter,opts);
}
});

//# sourceMappingURL=datascript.storage.js.map
