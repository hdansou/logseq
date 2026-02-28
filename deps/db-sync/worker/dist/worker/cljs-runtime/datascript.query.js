import "./cljs_env.js";
import "./cljs.core.js";
import "./cljs.reader.js";
import "./clojure.set.js";
import "./clojure.string.js";
import "./clojure.walk.js";
import "./datascript.built_ins.js";
import "./datascript.db.js";
import "./me.tonsky.persistent_sorted_set.arrays.js";
import "./datascript.lru.js";
import "./datascript.impl.entity.js";
import "./datascript.parser.js";
import "./datascript.pull_api.js";
import "./datascript.util.js";
goog.provide('datascript.query');
datascript.query._STAR_query_cache_STAR_ = datascript.lru.cache((100));




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
 * @implements {cljs.core.IWithMeta}
 * @implements {cljs.core.IAssociative}
 * @implements {cljs.core.IMap}
 * @implements {cljs.core.ILookup}
*/
datascript.query.Context = (function (rels,sources,rules,__meta,__extmap,__hash){
this.rels = rels;
this.sources = sources;
this.rules = rules;
this.__meta = __meta;
this.__extmap = __extmap;
this.__hash = __hash;
this.cljs$lang$protocol_mask$partition0$ = 2230716170;
this.cljs$lang$protocol_mask$partition1$ = 139264;
});
(datascript.query.Context.prototype.cljs$core$ILookup$_lookup$arity$2 = (function (this__5448__auto__,k__5449__auto__){
var self__ = this;
var this__5448__auto____$1 = this;
return this__5448__auto____$1.cljs$core$ILookup$_lookup$arity$3(null,k__5449__auto__,null);
}));

(datascript.query.Context.prototype.cljs$core$ILookup$_lookup$arity$3 = (function (this__5450__auto__,k42666,else__5451__auto__){
var self__ = this;
var this__5450__auto____$1 = this;
var G__42689 = k42666;
var G__42689__$1 = (((G__42689 instanceof cljs.core.Keyword))?G__42689.fqn:null);
switch (G__42689__$1) {
case "rels":
return self__.rels;

break;
case "sources":
return self__.sources;

break;
case "rules":
return self__.rules;

break;
default:
return cljs.core.get.cljs$core$IFn$_invoke$arity$3(self__.__extmap,k42666,else__5451__auto__);

}
}));

(datascript.query.Context.prototype.cljs$core$IKVReduce$_kv_reduce$arity$3 = (function (this__5468__auto__,f__5469__auto__,init__5470__auto__){
var self__ = this;
var this__5468__auto____$1 = this;
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (ret__5471__auto__,p__42691){
var vec__42693 = p__42691;
var k__5472__auto__ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42693,(0),null);
var v__5473__auto__ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42693,(1),null);
return (f__5469__auto__.cljs$core$IFn$_invoke$arity$3 ? f__5469__auto__.cljs$core$IFn$_invoke$arity$3(ret__5471__auto__,k__5472__auto__,v__5473__auto__) : f__5469__auto__.call(null,ret__5471__auto__,k__5472__auto__,v__5473__auto__));
}),init__5470__auto__,this__5468__auto____$1);
}));

(datascript.query.Context.prototype.cljs$core$IPrintWithWriter$_pr_writer$arity$3 = (function (this__5463__auto__,writer__5464__auto__,opts__5465__auto__){
var self__ = this;
var this__5463__auto____$1 = this;
var pr_pair__5466__auto__ = (function (keyval__5467__auto__){
return cljs.core.pr_sequential_writer(writer__5464__auto__,cljs.core.pr_writer,""," ","",opts__5465__auto__,keyval__5467__auto__);
});
return cljs.core.pr_sequential_writer(writer__5464__auto__,pr_pair__5466__auto__,"#datascript.query.Context{",", ","}",opts__5465__auto__,cljs.core.concat.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"rels","rels",1770187185),self__.rels],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"sources","sources",-321166424),self__.sources],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"rules","rules",1198912366),self__.rules],null))], null),self__.__extmap));
}));

(datascript.query.Context.prototype.cljs$core$IIterable$_iterator$arity$1 = (function (G__42665){
var self__ = this;
var G__42665__$1 = this;
return (new cljs.core.RecordIter((0),G__42665__$1,3,new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"rels","rels",1770187185),new cljs.core.Keyword(null,"sources","sources",-321166424),new cljs.core.Keyword(null,"rules","rules",1198912366)], null),(cljs.core.truth_(self__.__extmap)?cljs.core._iterator(self__.__extmap):cljs.core.nil_iter())));
}));

(datascript.query.Context.prototype.cljs$core$IMeta$_meta$arity$1 = (function (this__5446__auto__){
var self__ = this;
var this__5446__auto____$1 = this;
return self__.__meta;
}));

(datascript.query.Context.prototype.cljs$core$ICloneable$_clone$arity$1 = (function (this__5443__auto__){
var self__ = this;
var this__5443__auto____$1 = this;
return (new datascript.query.Context(self__.rels,self__.sources,self__.rules,self__.__meta,self__.__extmap,self__.__hash));
}));

(datascript.query.Context.prototype.cljs$core$ICounted$_count$arity$1 = (function (this__5452__auto__){
var self__ = this;
var this__5452__auto____$1 = this;
return (3 + cljs.core.count(self__.__extmap));
}));

(datascript.query.Context.prototype.cljs$core$IHash$_hash$arity$1 = (function (this__5444__auto__){
var self__ = this;
var this__5444__auto____$1 = this;
var h__5251__auto__ = self__.__hash;
if((!((h__5251__auto__ == null)))){
return h__5251__auto__;
} else {
var h__5251__auto____$1 = (function (coll__5445__auto__){
return (1014232958 ^ cljs.core.hash_unordered_coll(coll__5445__auto__));
})(this__5444__auto____$1);
(self__.__hash = h__5251__auto____$1);

return h__5251__auto____$1;
}
}));

(datascript.query.Context.prototype.cljs$core$IEquiv$_equiv$arity$2 = (function (this42667,other42668){
var self__ = this;
var this42667__$1 = this;
return (((!((other42668 == null)))) && ((((this42667__$1.constructor === other42668.constructor)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this42667__$1.rels,other42668.rels)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this42667__$1.sources,other42668.sources)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this42667__$1.rules,other42668.rules)) && (cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this42667__$1.__extmap,other42668.__extmap)))))))))));
}));

(datascript.query.Context.prototype.cljs$core$IMap$_dissoc$arity$2 = (function (this__5458__auto__,k__5459__auto__){
var self__ = this;
var this__5458__auto____$1 = this;
if(cljs.core.contains_QMARK_(new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"sources","sources",-321166424),null,new cljs.core.Keyword(null,"rules","rules",1198912366),null,new cljs.core.Keyword(null,"rels","rels",1770187185),null], null), null),k__5459__auto__)){
return cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(cljs.core._with_meta(cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,this__5458__auto____$1),self__.__meta),k__5459__auto__);
} else {
return (new datascript.query.Context(self__.rels,self__.sources,self__.rules,self__.__meta,cljs.core.not_empty(cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(self__.__extmap,k__5459__auto__)),null));
}
}));

(datascript.query.Context.prototype.cljs$core$IAssociative$_contains_key_QMARK_$arity$2 = (function (this__5455__auto__,k42666){
var self__ = this;
var this__5455__auto____$1 = this;
var G__42722 = k42666;
var G__42722__$1 = (((G__42722 instanceof cljs.core.Keyword))?G__42722.fqn:null);
switch (G__42722__$1) {
case "rels":
case "sources":
case "rules":
return true;

break;
default:
return cljs.core.contains_QMARK_(self__.__extmap,k42666);

}
}));

(datascript.query.Context.prototype.cljs$core$IAssociative$_assoc$arity$3 = (function (this__5456__auto__,k__5457__auto__,G__42665){
var self__ = this;
var this__5456__auto____$1 = this;
var pred__42727 = cljs.core.keyword_identical_QMARK_;
var expr__42728 = k__5457__auto__;
if(cljs.core.truth_((pred__42727.cljs$core$IFn$_invoke$arity$2 ? pred__42727.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"rels","rels",1770187185),expr__42728) : pred__42727.call(null,new cljs.core.Keyword(null,"rels","rels",1770187185),expr__42728)))){
return (new datascript.query.Context(G__42665,self__.sources,self__.rules,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__42727.cljs$core$IFn$_invoke$arity$2 ? pred__42727.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"sources","sources",-321166424),expr__42728) : pred__42727.call(null,new cljs.core.Keyword(null,"sources","sources",-321166424),expr__42728)))){
return (new datascript.query.Context(self__.rels,G__42665,self__.rules,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__42727.cljs$core$IFn$_invoke$arity$2 ? pred__42727.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"rules","rules",1198912366),expr__42728) : pred__42727.call(null,new cljs.core.Keyword(null,"rules","rules",1198912366),expr__42728)))){
return (new datascript.query.Context(self__.rels,self__.sources,G__42665,self__.__meta,self__.__extmap,null));
} else {
return (new datascript.query.Context(self__.rels,self__.sources,self__.rules,self__.__meta,cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(self__.__extmap,k__5457__auto__,G__42665),null));
}
}
}
}));

(datascript.query.Context.prototype.cljs$core$ISeqable$_seq$arity$1 = (function (this__5461__auto__){
var self__ = this;
var this__5461__auto____$1 = this;
return cljs.core.seq(cljs.core.concat.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new cljs.core.MapEntry(new cljs.core.Keyword(null,"rels","rels",1770187185),self__.rels,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"sources","sources",-321166424),self__.sources,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"rules","rules",1198912366),self__.rules,null))], null),self__.__extmap));
}));

(datascript.query.Context.prototype.cljs$core$IWithMeta$_with_meta$arity$2 = (function (this__5447__auto__,G__42665){
var self__ = this;
var this__5447__auto____$1 = this;
return (new datascript.query.Context(self__.rels,self__.sources,self__.rules,G__42665,self__.__extmap,self__.__hash));
}));

(datascript.query.Context.prototype.cljs$core$ICollection$_conj$arity$2 = (function (this__5453__auto__,entry__5454__auto__){
var self__ = this;
var this__5453__auto____$1 = this;
if(cljs.core.vector_QMARK_(entry__5454__auto__)){
return this__5453__auto____$1.cljs$core$IAssociative$_assoc$arity$3(null,cljs.core._nth(entry__5454__auto__,(0)),cljs.core._nth(entry__5454__auto__,(1)));
} else {
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(cljs.core._conj,this__5453__auto____$1,entry__5454__auto__);
}
}));

(datascript.query.Context.getBasis = (function (){
return new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"rels","rels",-884248584,null),new cljs.core.Symbol(null,"sources","sources",1319365103,null),new cljs.core.Symbol(null,"rules","rules",-1455523403,null)], null);
}));

(datascript.query.Context.cljs$lang$type = true);

(datascript.query.Context.cljs$lang$ctorPrSeq = (function (this__5494__auto__){
return (new cljs.core.List(null,"datascript.query/Context",null,(1),null));
}));

(datascript.query.Context.cljs$lang$ctorPrWriter = (function (this__5494__auto__,writer__5495__auto__){
return cljs.core._write(writer__5495__auto__,"datascript.query/Context");
}));

/**
 * Positional factory function for datascript.query/Context.
 */
datascript.query.__GT_Context = (function datascript$query$__GT_Context(rels,sources,rules){
return (new datascript.query.Context(rels,sources,rules,null,null,null));
});

/**
 * Factory function for datascript.query/Context, taking a map of keywords to field values.
 */
datascript.query.map__GT_Context = (function datascript$query$map__GT_Context(G__42669){
var extmap__5490__auto__ = (function (){var G__42734 = cljs.core.dissoc.cljs$core$IFn$_invoke$arity$variadic(G__42669,new cljs.core.Keyword(null,"rels","rels",1770187185),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"sources","sources",-321166424),new cljs.core.Keyword(null,"rules","rules",1198912366)], 0));
if(cljs.core.record_QMARK_(G__42669)){
return cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,G__42734);
} else {
return G__42734;
}
})();
return (new datascript.query.Context(new cljs.core.Keyword(null,"rels","rels",1770187185).cljs$core$IFn$_invoke$arity$1(G__42669),new cljs.core.Keyword(null,"sources","sources",-321166424).cljs$core$IFn$_invoke$arity$1(G__42669),new cljs.core.Keyword(null,"rules","rules",1198912366).cljs$core$IFn$_invoke$arity$1(G__42669),null,cljs.core.not_empty(extmap__5490__auto__),null));
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
 * @implements {cljs.core.IWithMeta}
 * @implements {cljs.core.IAssociative}
 * @implements {cljs.core.IMap}
 * @implements {cljs.core.ILookup}
*/
datascript.query.Relation = (function (attrs,tuples,__meta,__extmap,__hash){
this.attrs = attrs;
this.tuples = tuples;
this.__meta = __meta;
this.__extmap = __extmap;
this.__hash = __hash;
this.cljs$lang$protocol_mask$partition0$ = 2230716170;
this.cljs$lang$protocol_mask$partition1$ = 139264;
});
(datascript.query.Relation.prototype.cljs$core$ILookup$_lookup$arity$2 = (function (this__5448__auto__,k__5449__auto__){
var self__ = this;
var this__5448__auto____$1 = this;
return this__5448__auto____$1.cljs$core$ILookup$_lookup$arity$3(null,k__5449__auto__,null);
}));

(datascript.query.Relation.prototype.cljs$core$ILookup$_lookup$arity$3 = (function (this__5450__auto__,k42737,else__5451__auto__){
var self__ = this;
var this__5450__auto____$1 = this;
var G__42746 = k42737;
var G__42746__$1 = (((G__42746 instanceof cljs.core.Keyword))?G__42746.fqn:null);
switch (G__42746__$1) {
case "attrs":
return self__.attrs;

break;
case "tuples":
return self__.tuples;

break;
default:
return cljs.core.get.cljs$core$IFn$_invoke$arity$3(self__.__extmap,k42737,else__5451__auto__);

}
}));

(datascript.query.Relation.prototype.cljs$core$IKVReduce$_kv_reduce$arity$3 = (function (this__5468__auto__,f__5469__auto__,init__5470__auto__){
var self__ = this;
var this__5468__auto____$1 = this;
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (ret__5471__auto__,p__42748){
var vec__42749 = p__42748;
var k__5472__auto__ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42749,(0),null);
var v__5473__auto__ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42749,(1),null);
return (f__5469__auto__.cljs$core$IFn$_invoke$arity$3 ? f__5469__auto__.cljs$core$IFn$_invoke$arity$3(ret__5471__auto__,k__5472__auto__,v__5473__auto__) : f__5469__auto__.call(null,ret__5471__auto__,k__5472__auto__,v__5473__auto__));
}),init__5470__auto__,this__5468__auto____$1);
}));

(datascript.query.Relation.prototype.cljs$core$IPrintWithWriter$_pr_writer$arity$3 = (function (this__5463__auto__,writer__5464__auto__,opts__5465__auto__){
var self__ = this;
var this__5463__auto____$1 = this;
var pr_pair__5466__auto__ = (function (keyval__5467__auto__){
return cljs.core.pr_sequential_writer(writer__5464__auto__,cljs.core.pr_writer,""," ","",opts__5465__auto__,keyval__5467__auto__);
});
return cljs.core.pr_sequential_writer(writer__5464__auto__,pr_pair__5466__auto__,"#datascript.query.Relation{",", ","}",opts__5465__auto__,cljs.core.concat.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"attrs","attrs",-2090668713),self__.attrs],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"tuples","tuples",-676032639),self__.tuples],null))], null),self__.__extmap));
}));

(datascript.query.Relation.prototype.cljs$core$IIterable$_iterator$arity$1 = (function (G__42736){
var self__ = this;
var G__42736__$1 = this;
return (new cljs.core.RecordIter((0),G__42736__$1,2,new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"attrs","attrs",-2090668713),new cljs.core.Keyword(null,"tuples","tuples",-676032639)], null),(cljs.core.truth_(self__.__extmap)?cljs.core._iterator(self__.__extmap):cljs.core.nil_iter())));
}));

(datascript.query.Relation.prototype.cljs$core$IMeta$_meta$arity$1 = (function (this__5446__auto__){
var self__ = this;
var this__5446__auto____$1 = this;
return self__.__meta;
}));

(datascript.query.Relation.prototype.cljs$core$ICloneable$_clone$arity$1 = (function (this__5443__auto__){
var self__ = this;
var this__5443__auto____$1 = this;
return (new datascript.query.Relation(self__.attrs,self__.tuples,self__.__meta,self__.__extmap,self__.__hash));
}));

(datascript.query.Relation.prototype.cljs$core$ICounted$_count$arity$1 = (function (this__5452__auto__){
var self__ = this;
var this__5452__auto____$1 = this;
return (2 + cljs.core.count(self__.__extmap));
}));

(datascript.query.Relation.prototype.cljs$core$IHash$_hash$arity$1 = (function (this__5444__auto__){
var self__ = this;
var this__5444__auto____$1 = this;
var h__5251__auto__ = self__.__hash;
if((!((h__5251__auto__ == null)))){
return h__5251__auto__;
} else {
var h__5251__auto____$1 = (function (coll__5445__auto__){
return (1107093117 ^ cljs.core.hash_unordered_coll(coll__5445__auto__));
})(this__5444__auto____$1);
(self__.__hash = h__5251__auto____$1);

return h__5251__auto____$1;
}
}));

(datascript.query.Relation.prototype.cljs$core$IEquiv$_equiv$arity$2 = (function (this42738,other42739){
var self__ = this;
var this42738__$1 = this;
return (((!((other42739 == null)))) && ((((this42738__$1.constructor === other42739.constructor)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this42738__$1.attrs,other42739.attrs)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this42738__$1.tuples,other42739.tuples)) && (cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this42738__$1.__extmap,other42739.__extmap)))))))));
}));

(datascript.query.Relation.prototype.cljs$core$IMap$_dissoc$arity$2 = (function (this__5458__auto__,k__5459__auto__){
var self__ = this;
var this__5458__auto____$1 = this;
if(cljs.core.contains_QMARK_(new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"tuples","tuples",-676032639),null,new cljs.core.Keyword(null,"attrs","attrs",-2090668713),null], null), null),k__5459__auto__)){
return cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(cljs.core._with_meta(cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,this__5458__auto____$1),self__.__meta),k__5459__auto__);
} else {
return (new datascript.query.Relation(self__.attrs,self__.tuples,self__.__meta,cljs.core.not_empty(cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(self__.__extmap,k__5459__auto__)),null));
}
}));

(datascript.query.Relation.prototype.cljs$core$IAssociative$_contains_key_QMARK_$arity$2 = (function (this__5455__auto__,k42737){
var self__ = this;
var this__5455__auto____$1 = this;
var G__42761 = k42737;
var G__42761__$1 = (((G__42761 instanceof cljs.core.Keyword))?G__42761.fqn:null);
switch (G__42761__$1) {
case "attrs":
case "tuples":
return true;

break;
default:
return cljs.core.contains_QMARK_(self__.__extmap,k42737);

}
}));

(datascript.query.Relation.prototype.cljs$core$IAssociative$_assoc$arity$3 = (function (this__5456__auto__,k__5457__auto__,G__42736){
var self__ = this;
var this__5456__auto____$1 = this;
var pred__42762 = cljs.core.keyword_identical_QMARK_;
var expr__42763 = k__5457__auto__;
if(cljs.core.truth_((pred__42762.cljs$core$IFn$_invoke$arity$2 ? pred__42762.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"attrs","attrs",-2090668713),expr__42763) : pred__42762.call(null,new cljs.core.Keyword(null,"attrs","attrs",-2090668713),expr__42763)))){
return (new datascript.query.Relation(G__42736,self__.tuples,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__42762.cljs$core$IFn$_invoke$arity$2 ? pred__42762.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"tuples","tuples",-676032639),expr__42763) : pred__42762.call(null,new cljs.core.Keyword(null,"tuples","tuples",-676032639),expr__42763)))){
return (new datascript.query.Relation(self__.attrs,G__42736,self__.__meta,self__.__extmap,null));
} else {
return (new datascript.query.Relation(self__.attrs,self__.tuples,self__.__meta,cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(self__.__extmap,k__5457__auto__,G__42736),null));
}
}
}));

(datascript.query.Relation.prototype.cljs$core$ISeqable$_seq$arity$1 = (function (this__5461__auto__){
var self__ = this;
var this__5461__auto____$1 = this;
return cljs.core.seq(cljs.core.concat.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new cljs.core.MapEntry(new cljs.core.Keyword(null,"attrs","attrs",-2090668713),self__.attrs,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"tuples","tuples",-676032639),self__.tuples,null))], null),self__.__extmap));
}));

(datascript.query.Relation.prototype.cljs$core$IWithMeta$_with_meta$arity$2 = (function (this__5447__auto__,G__42736){
var self__ = this;
var this__5447__auto____$1 = this;
return (new datascript.query.Relation(self__.attrs,self__.tuples,G__42736,self__.__extmap,self__.__hash));
}));

(datascript.query.Relation.prototype.cljs$core$ICollection$_conj$arity$2 = (function (this__5453__auto__,entry__5454__auto__){
var self__ = this;
var this__5453__auto____$1 = this;
if(cljs.core.vector_QMARK_(entry__5454__auto__)){
return this__5453__auto____$1.cljs$core$IAssociative$_assoc$arity$3(null,cljs.core._nth(entry__5454__auto__,(0)),cljs.core._nth(entry__5454__auto__,(1)));
} else {
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(cljs.core._conj,this__5453__auto____$1,entry__5454__auto__);
}
}));

(datascript.query.Relation.getBasis = (function (){
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"attrs","attrs",-450137186,null),new cljs.core.Symbol(null,"tuples","tuples",964498888,null)], null);
}));

(datascript.query.Relation.cljs$lang$type = true);

(datascript.query.Relation.cljs$lang$ctorPrSeq = (function (this__5494__auto__){
return (new cljs.core.List(null,"datascript.query/Relation",null,(1),null));
}));

(datascript.query.Relation.cljs$lang$ctorPrWriter = (function (this__5494__auto__,writer__5495__auto__){
return cljs.core._write(writer__5495__auto__,"datascript.query/Relation");
}));

/**
 * Positional factory function for datascript.query/Relation.
 */
datascript.query.__GT_Relation = (function datascript$query$__GT_Relation(attrs,tuples){
return (new datascript.query.Relation(attrs,tuples,null,null,null));
});

/**
 * Factory function for datascript.query/Relation, taking a map of keywords to field values.
 */
datascript.query.map__GT_Relation = (function datascript$query$map__GT_Relation(G__42742){
var extmap__5490__auto__ = (function (){var G__42776 = cljs.core.dissoc.cljs$core$IFn$_invoke$arity$variadic(G__42742,new cljs.core.Keyword(null,"attrs","attrs",-2090668713),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"tuples","tuples",-676032639)], 0));
if(cljs.core.record_QMARK_(G__42742)){
return cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,G__42776);
} else {
return G__42776;
}
})();
return (new datascript.query.Relation(new cljs.core.Keyword(null,"attrs","attrs",-2090668713).cljs$core$IFn$_invoke$arity$1(G__42742),new cljs.core.Keyword(null,"tuples","tuples",-676032639).cljs$core$IFn$_invoke$arity$1(G__42742),null,cljs.core.not_empty(extmap__5490__auto__),null));
});

datascript.query.intersect_keys = (function datascript$query$intersect_keys(attrs1,attrs2){
return clojure.set.intersection.cljs$core$IFn$_invoke$arity$2(cljs.core.set(cljs.core.keys(attrs1)),cljs.core.set(cljs.core.keys(attrs2)));
});
datascript.query.same_keys_QMARK_ = (function datascript$query$same_keys_QMARK_(a,b){
return ((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(cljs.core.count(a),cljs.core.count(b))) && (((cljs.core.every_QMARK_((function (p1__42779_SHARP_){
return cljs.core.contains_QMARK_(b,p1__42779_SHARP_);
}),cljs.core.keys(a))) && (cljs.core.every_QMARK_((function (p1__42780_SHARP_){
return cljs.core.contains_QMARK_(a,p1__42780_SHARP_);
}),cljs.core.keys(b))))));
});
datascript.query.looks_like_QMARK_ = (function datascript$query$looks_like_QMARK_(pattern,form){
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Symbol(null,"_","_",-1201019570,null),pattern)){
return true;
} else {
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"*","*",345799209,null)], null),pattern)){
return cljs.core.sequential_QMARK_(form);
} else {
if((pattern instanceof cljs.core.Symbol)){
return cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(form,pattern);
} else {
if(cljs.core.sequential_QMARK_(pattern)){
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(cljs.core.last(pattern),new cljs.core.Symbol(null,"*","*",345799209,null))){
return ((cljs.core.sequential_QMARK_(form)) && (cljs.core.every_QMARK_((function (p__42783){
var vec__42784 = p__42783;
var pattern_el = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42784,(0),null);
var form_el = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42784,(1),null);
return (datascript.query.looks_like_QMARK_.cljs$core$IFn$_invoke$arity$2 ? datascript.query.looks_like_QMARK_.cljs$core$IFn$_invoke$arity$2(pattern_el,form_el) : datascript.query.looks_like_QMARK_.call(null,pattern_el,form_el));
}),cljs.core.map.cljs$core$IFn$_invoke$arity$3(cljs.core.vector,cljs.core.butlast(pattern),form))));
} else {
return ((cljs.core.sequential_QMARK_(form)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(cljs.core.count(form),cljs.core.count(pattern))) && (cljs.core.every_QMARK_((function (p__42792){
var vec__42793 = p__42792;
var pattern_el = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42793,(0),null);
var form_el = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42793,(1),null);
return (datascript.query.looks_like_QMARK_.cljs$core$IFn$_invoke$arity$2 ? datascript.query.looks_like_QMARK_.cljs$core$IFn$_invoke$arity$2(pattern_el,form_el) : datascript.query.looks_like_QMARK_.call(null,pattern_el,form_el));
}),cljs.core.map.cljs$core$IFn$_invoke$arity$3(cljs.core.vector,pattern,form))))));
}
} else {
return (pattern.cljs$core$IFn$_invoke$arity$1 ? pattern.cljs$core$IFn$_invoke$arity$1(form) : pattern.call(null,form));

}
}
}
}
});
datascript.query.source_QMARK_ = (function datascript$query$source_QMARK_(sym){
return (((sym instanceof cljs.core.Symbol)) && (cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2("$",cljs.core.first(cljs.core.name(sym)))));
});
datascript.query.free_var_QMARK_ = (function datascript$query$free_var_QMARK_(sym){
return (((sym instanceof cljs.core.Symbol)) && (cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2("?",cljs.core.first(cljs.core.name(sym)))));
});
datascript.query.attr_QMARK_ = (function datascript$query$attr_QMARK_(form){
return (((form instanceof cljs.core.Keyword)) || (typeof form === 'string'));
});
datascript.query.lookup_ref_QMARK_ = (function datascript$query$lookup_ref_QMARK_(form){
return ((((cljs.core.sequential_QMARK_(form)) || (me.tonsky.persistent_sorted_set.arrays.array_QMARK_(form)))) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2((2),cljs.core.count(form))) && (datascript.query.attr_QMARK_(cljs.core.first(form))))));
});
datascript.query.join_tuples = (function datascript$query$join_tuples(t1,idxs1,t2,idxs2){
var l1 = idxs1.length;
var l2 = idxs2.length;
var res = me.tonsky.persistent_sorted_set.arrays.make_array((l1 + l2));
var n__5741__auto___43627 = l1;
var i_43628 = (0);
while(true){
if((i_43628 < n__5741__auto___43627)){
(res[i_43628] = (t1[(idxs1[i_43628])]));

var G__43630 = (i_43628 + (1));
i_43628 = G__43630;
continue;
} else {
}
break;
}

var n__5741__auto___43631 = l2;
var i_43632 = (0);
while(true){
if((i_43632 < n__5741__auto___43631)){
(res[(l1 + i_43632)] = (t2[(idxs2[i_43632])]));

var G__43633 = (i_43632 + (1));
i_43632 = G__43633;
continue;
} else {
}
break;
}

return res;
});
datascript.query.sum_rel_STAR_ = (function datascript$query$sum_rel_STAR_(attrs_a,tuples_a,attrs_b,tuples_b){
var idxb__GT_idxa = cljs.core.vec((function (){var iter__5628__auto__ = (function datascript$query$sum_rel_STAR__$_iter__42803(s__42804){
return (new cljs.core.LazySeq(null,(function (){
var s__42804__$1 = s__42804;
while(true){
var temp__5804__auto__ = cljs.core.seq(s__42804__$1);
if(temp__5804__auto__){
var s__42804__$2 = temp__5804__auto__;
if(cljs.core.chunked_seq_QMARK_(s__42804__$2)){
var c__5626__auto__ = cljs.core.chunk_first(s__42804__$2);
var size__5627__auto__ = cljs.core.count(c__5626__auto__);
var b__42806 = cljs.core.chunk_buffer(size__5627__auto__);
if((function (){var i__42805 = (0);
while(true){
if((i__42805 < size__5627__auto__)){
var vec__42808 = cljs.core._nth(c__5626__auto__,i__42805);
var sym = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42808,(0),null);
var idx_b = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42808,(1),null);
cljs.core.chunk_append(b__42806,new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [idx_b,(attrs_a.cljs$core$IFn$_invoke$arity$1 ? attrs_a.cljs$core$IFn$_invoke$arity$1(sym) : attrs_a.call(null,sym))], null));

var G__43634 = (i__42805 + (1));
i__42805 = G__43634;
continue;
} else {
return true;
}
break;
}
})()){
return cljs.core.chunk_cons(cljs.core.chunk(b__42806),datascript$query$sum_rel_STAR__$_iter__42803(cljs.core.chunk_rest(s__42804__$2)));
} else {
return cljs.core.chunk_cons(cljs.core.chunk(b__42806),null);
}
} else {
var vec__42812 = cljs.core.first(s__42804__$2);
var sym = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42812,(0),null);
var idx_b = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42812,(1),null);
return cljs.core.cons(new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [idx_b,(attrs_a.cljs$core$IFn$_invoke$arity$1 ? attrs_a.cljs$core$IFn$_invoke$arity$1(sym) : attrs_a.call(null,sym))], null),datascript$query$sum_rel_STAR__$_iter__42803(cljs.core.rest(s__42804__$2)));
}
} else {
return null;
}
break;
}
}),null,null));
});
return iter__5628__auto__(attrs_b);
})());
var tlen = (cljs.core.reduce.cljs$core$IFn$_invoke$arity$2(cljs.core.max,cljs.core.vals(attrs_a)) + (1));
var tuples_SINGLEQUOTE_ = cljs.core.persistent_BANG_(cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (acc,tuple_b){
var tuple_SINGLEQUOTE_ = me.tonsky.persistent_sorted_set.arrays.make_array(tlen);
var seq__42815_43635 = cljs.core.seq(idxb__GT_idxa);
var chunk__42816_43636 = null;
var count__42817_43637 = (0);
var i__42818_43638 = (0);
while(true){
if((i__42818_43638 < count__42817_43637)){
var vec__42827_43639 = chunk__42816_43636.cljs$core$IIndexed$_nth$arity$2(null,i__42818_43638);
var idx_b_43640 = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42827_43639,(0),null);
var idx_a_43641 = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42827_43639,(1),null);
(tuple_SINGLEQUOTE_[idx_a_43641] = (tuple_b[idx_b_43640]));


var G__43642 = seq__42815_43635;
var G__43643 = chunk__42816_43636;
var G__43644 = count__42817_43637;
var G__43645 = (i__42818_43638 + (1));
seq__42815_43635 = G__43642;
chunk__42816_43636 = G__43643;
count__42817_43637 = G__43644;
i__42818_43638 = G__43645;
continue;
} else {
var temp__5804__auto___43646 = cljs.core.seq(seq__42815_43635);
if(temp__5804__auto___43646){
var seq__42815_43647__$1 = temp__5804__auto___43646;
if(cljs.core.chunked_seq_QMARK_(seq__42815_43647__$1)){
var c__5673__auto___43648 = cljs.core.chunk_first(seq__42815_43647__$1);
var G__43649 = cljs.core.chunk_rest(seq__42815_43647__$1);
var G__43650 = c__5673__auto___43648;
var G__43651 = cljs.core.count(c__5673__auto___43648);
var G__43652 = (0);
seq__42815_43635 = G__43649;
chunk__42816_43636 = G__43650;
count__42817_43637 = G__43651;
i__42818_43638 = G__43652;
continue;
} else {
var vec__42830_43653 = cljs.core.first(seq__42815_43647__$1);
var idx_b_43654 = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42830_43653,(0),null);
var idx_a_43655 = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42830_43653,(1),null);
(tuple_SINGLEQUOTE_[idx_a_43655] = (tuple_b[idx_b_43654]));


var G__43656 = cljs.core.next(seq__42815_43647__$1);
var G__43657 = null;
var G__43658 = (0);
var G__43659 = (0);
seq__42815_43635 = G__43656;
chunk__42816_43636 = G__43657;
count__42817_43637 = G__43658;
i__42818_43638 = G__43659;
continue;
}
} else {
}
}
break;
}

return cljs.core.conj_BANG_.cljs$core$IFn$_invoke$arity$2(acc,tuple_SINGLEQUOTE_);
}),cljs.core.transient$(cljs.core.vec(tuples_a)),tuples_b));
return (new datascript.query.Relation(attrs_a,tuples_SINGLEQUOTE_,null,null,null));
});
datascript.query.sum_rel = (function datascript$query$sum_rel(a,b){
var map__42835 = a;
var map__42835__$1 = cljs.core.__destructure_map(map__42835);
var attrs_a = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__42835__$1,new cljs.core.Keyword(null,"attrs","attrs",-2090668713));
var tuples_a = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__42835__$1,new cljs.core.Keyword(null,"tuples","tuples",-676032639));
var map__42836 = b;
var map__42836__$1 = cljs.core.__destructure_map(map__42836);
var attrs_b = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__42836__$1,new cljs.core.Keyword(null,"attrs","attrs",-2090668713));
var tuples_b = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__42836__$1,new cljs.core.Keyword(null,"tuples","tuples",-676032639));
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(attrs_a,attrs_b)){
return (new datascript.query.Relation(attrs_a,cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.vec(tuples_a),tuples_b),null,null,null));
} else {
if(cljs.core.empty_QMARK_(tuples_a)){
return b;
} else {
if(cljs.core.empty_QMARK_(tuples_b)){
return a;
} else {
if((!(datascript.query.same_keys_QMARK_(attrs_a,attrs_b)))){
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2((""+"Can\u2019t sum relations with different attrs: "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([attrs_a], 0)))+" and "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([attrs_b], 0)))),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"error","error",-978969032),new cljs.core.Keyword("query","where","query/where",-1935159429)], null));
} else {
if(cljs.core.every_QMARK_(cljs.core.number_QMARK_,cljs.core.vals(attrs_a))){
return datascript.query.sum_rel_STAR_(attrs_a,tuples_a,attrs_b,tuples_b);
} else {
var number_attrs = cljs.core.zipmap(cljs.core.keys(attrs_a),cljs.core.range.cljs$core$IFn$_invoke$arity$0());
var G__42839 = datascript.query.sum_rel_STAR_(number_attrs,cljs.core.PersistentVector.EMPTY,attrs_a,tuples_a);
var G__42840 = b;
return (datascript.query.sum_rel.cljs$core$IFn$_invoke$arity$2 ? datascript.query.sum_rel.cljs$core$IFn$_invoke$arity$2(G__42839,G__42840) : datascript.query.sum_rel.call(null,G__42839,G__42840));

}
}
}
}
}
});
datascript.query.prod_rel = (function datascript$query$prod_rel(var_args){
var G__42844 = arguments.length;
switch (G__42844) {
case 0:
return datascript.query.prod_rel.cljs$core$IFn$_invoke$arity$0();

break;
case 2:
return datascript.query.prod_rel.cljs$core$IFn$_invoke$arity$2((arguments[(0)]),(arguments[(1)]));

break;
default:
throw (new Error(["Invalid arity: ",arguments.length].join("")));

}
});

(datascript.query.prod_rel.cljs$core$IFn$_invoke$arity$0 = (function (){
return (new datascript.query.Relation(cljs.core.PersistentArrayMap.EMPTY,new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [me.tonsky.persistent_sorted_set.arrays.make_array((0))], null),null,null,null));
}));

(datascript.query.prod_rel.cljs$core$IFn$_invoke$arity$2 = (function (rel1,rel2){
var attrs1 = cljs.core.keys(new cljs.core.Keyword(null,"attrs","attrs",-2090668713).cljs$core$IFn$_invoke$arity$1(rel1));
var attrs2 = cljs.core.keys(new cljs.core.Keyword(null,"attrs","attrs",-2090668713).cljs$core$IFn$_invoke$arity$1(rel2));
var idxs1 = cljs.core.to_array(cljs.core.map.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"attrs","attrs",-2090668713).cljs$core$IFn$_invoke$arity$1(rel1),attrs1));
var idxs2 = cljs.core.to_array(cljs.core.map.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"attrs","attrs",-2090668713).cljs$core$IFn$_invoke$arity$1(rel2),attrs2));
return (new datascript.query.Relation(cljs.core.zipmap(cljs.core.concat.cljs$core$IFn$_invoke$arity$2(attrs1,attrs2),cljs.core.range.cljs$core$IFn$_invoke$arity$0()),cljs.core.persistent_BANG_(cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (acc,t1){
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (acc__$1,t2){
return cljs.core.conj_BANG_.cljs$core$IFn$_invoke$arity$2(acc__$1,datascript.query.join_tuples(t1,idxs1,t2,idxs2));
}),acc,new cljs.core.Keyword(null,"tuples","tuples",-676032639).cljs$core$IFn$_invoke$arity$1(rel2));
}),cljs.core.transient$(cljs.core.PersistentVector.EMPTY),new cljs.core.Keyword(null,"tuples","tuples",-676032639).cljs$core$IFn$_invoke$arity$1(rel1))),null,null,null));
}));

(datascript.query.prod_rel.cljs$lang$maxFixedArity = 2);

datascript.query.parse_rules = (function datascript$query$parse_rules(rules){
var rules__$1 = ((typeof rules === 'string')?cljs.reader.read_string.cljs$core$IFn$_invoke$arity$1(rules):rules);
datascript.parser.parse_rules(rules__$1);

return cljs.core.group_by(cljs.core.ffirst,rules__$1);
});
datascript.query.empty_rel = (function datascript$query$empty_rel(binding){
var vars = cljs.core.map.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"symbol","symbol",-1038572696),datascript.parser.collect_vars_distinct(binding));
return (new datascript.query.Relation(cljs.core.zipmap(vars,cljs.core.range.cljs$core$IFn$_invoke$arity$0()),cljs.core.PersistentVector.EMPTY,null,null,null));
});

/**
 * @interface
 */
datascript.query.IBinding = function(){};

var datascript$query$IBinding$in__GT_rel$dyn_43661 = (function (binding,value){
var x__5498__auto__ = (((binding == null))?null:binding);
var m__5499__auto__ = (datascript.query.in__GT_rel[goog.typeOf(x__5498__auto__)]);
if((!((m__5499__auto__ == null)))){
return (m__5499__auto__.cljs$core$IFn$_invoke$arity$2 ? m__5499__auto__.cljs$core$IFn$_invoke$arity$2(binding,value) : m__5499__auto__.call(null,binding,value));
} else {
var m__5497__auto__ = (datascript.query.in__GT_rel["_"]);
if((!((m__5497__auto__ == null)))){
return (m__5497__auto__.cljs$core$IFn$_invoke$arity$2 ? m__5497__auto__.cljs$core$IFn$_invoke$arity$2(binding,value) : m__5497__auto__.call(null,binding,value));
} else {
throw cljs.core.missing_protocol("IBinding.in->rel",binding);
}
}
});
datascript.query.in__GT_rel = (function datascript$query$in__GT_rel(binding,value){
if((((!((binding == null)))) && ((!((binding.datascript$query$IBinding$in__GT_rel$arity$2 == null)))))){
return binding.datascript$query$IBinding$in__GT_rel$arity$2(binding,value);
} else {
return datascript$query$IBinding$in__GT_rel$dyn_43661(binding,value);
}
});

(datascript.parser.BindIgnore.prototype.datascript$query$IBinding$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.parser.BindIgnore.prototype.datascript$query$IBinding$in__GT_rel$arity$2 = (function (_,___$1){
var ___$2 = this;
return datascript.query.prod_rel.cljs$core$IFn$_invoke$arity$0();
}));

(datascript.parser.BindScalar.prototype.datascript$query$IBinding$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.parser.BindScalar.prototype.datascript$query$IBinding$in__GT_rel$arity$2 = (function (binding,value){
var binding__$1 = this;
return (new datascript.query.Relation(cljs.core.PersistentArrayMap.createAsIfByAssoc([cljs.core.get_in.cljs$core$IFn$_invoke$arity$2(binding__$1,new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"variable","variable",-281346492),new cljs.core.Keyword(null,"symbol","symbol",-1038572696)], null)),(0)]),new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [cljs.core.into_array.cljs$core$IFn$_invoke$arity$1(new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [value], null))], null),null,null,null));
}));

(datascript.parser.BindColl.prototype.datascript$query$IBinding$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.parser.BindColl.prototype.datascript$query$IBinding$in__GT_rel$arity$2 = (function (binding,coll){
var binding__$1 = this;
if((!(datascript.db.seqable_QMARK_(coll)))){
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2((""+"Cannot bind value "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([coll], 0)))+" to collection "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([datascript.parser.source(binding__$1)], 0)))),new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"error","error",-978969032),new cljs.core.Keyword("query","binding","query/binding",698240489),new cljs.core.Keyword(null,"value","value",305978217),coll,new cljs.core.Keyword(null,"binding","binding",539932593),datascript.parser.source(binding__$1)], null));
} else {
if(cljs.core.empty_QMARK_(coll)){
return datascript.query.empty_rel(binding__$1);
} else {
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$2(datascript.query.sum_rel,cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (p1__42862_SHARP_){
return datascript.query.in__GT_rel(binding__$1.binding,p1__42862_SHARP_);
}),coll));

}
}
}));

(datascript.parser.BindTuple.prototype.datascript$query$IBinding$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.parser.BindTuple.prototype.datascript$query$IBinding$in__GT_rel$arity$2 = (function (binding,coll){
var binding__$1 = this;
if((!(datascript.db.seqable_QMARK_(coll)))){
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2((""+"Cannot bind value "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([coll], 0)))+" to tuple "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([datascript.parser.source(binding__$1)], 0)))),new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"error","error",-978969032),new cljs.core.Keyword("query","binding","query/binding",698240489),new cljs.core.Keyword(null,"value","value",305978217),coll,new cljs.core.Keyword(null,"binding","binding",539932593),datascript.parser.source(binding__$1)], null));
} else {
if((cljs.core.count(coll) < cljs.core.count(binding__$1.bindings))){
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2((""+"Not enough elements in a collection "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([coll], 0)))+" to bind tuple "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([datascript.parser.source(binding__$1)], 0)))),new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"error","error",-978969032),new cljs.core.Keyword("query","binding","query/binding",698240489),new cljs.core.Keyword(null,"value","value",305978217),coll,new cljs.core.Keyword(null,"binding","binding",539932593),datascript.parser.source(binding__$1)], null));
} else {
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$2(datascript.query.prod_rel,cljs.core.map.cljs$core$IFn$_invoke$arity$3((function (p1__42863_SHARP_,p2__42864_SHARP_){
return datascript.query.in__GT_rel(p1__42863_SHARP_,p2__42864_SHARP_);
}),binding__$1.bindings,coll));

}
}
}));
datascript.query.resolve_in = (function datascript$query$resolve_in(context,p__42889){
var vec__42890 = p__42889;
var binding = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42890,(0),null);
var value = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42890,(1),null);
if((((binding instanceof datascript.parser.BindScalar)) && ((new cljs.core.Keyword(null,"variable","variable",-281346492).cljs$core$IFn$_invoke$arity$1(binding) instanceof datascript.parser.SrcVar)))){
return cljs.core.update.cljs$core$IFn$_invoke$arity$5(context,new cljs.core.Keyword(null,"sources","sources",-321166424),cljs.core.assoc,cljs.core.get_in.cljs$core$IFn$_invoke$arity$2(binding,new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"variable","variable",-281346492),new cljs.core.Keyword(null,"symbol","symbol",-1038572696)], null)),value);
} else {
if((((binding instanceof datascript.parser.BindScalar)) && ((new cljs.core.Keyword(null,"variable","variable",-281346492).cljs$core$IFn$_invoke$arity$1(binding) instanceof datascript.parser.RulesVar)))){
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(context,new cljs.core.Keyword(null,"rules","rules",1198912366),datascript.query.parse_rules(value));
} else {
return cljs.core.update.cljs$core$IFn$_invoke$arity$4(context,new cljs.core.Keyword(null,"rels","rels",1770187185),cljs.core.conj,datascript.query.in__GT_rel(binding,value));

}
}
});
datascript.query.resolve_ins = (function datascript$query$resolve_ins(context,bindings,values){
var cb = cljs.core.count(bindings);
var cv = cljs.core.count(values);
if((cb < cv)){
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2((""+"Extra inputs passed, expected: "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([cljs.core.mapv.cljs$core$IFn$_invoke$arity$2((function (p1__42897_SHARP_){
return new cljs.core.Keyword(null,"source","source",-433931539).cljs$core$IFn$_invoke$arity$1(cljs.core.meta(p1__42897_SHARP_));
}),bindings)], 0)))+", got: "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([cv], 0)))),new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"error","error",-978969032),new cljs.core.Keyword("query","inputs","query/inputs",1042810394),new cljs.core.Keyword(null,"expected","expected",1583670997),bindings,new cljs.core.Keyword(null,"got","got",-1674745710),values], null));
} else {
if((cb > cv)){
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2((""+"Too few inputs passed, expected: "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([cljs.core.mapv.cljs$core$IFn$_invoke$arity$2((function (p1__42898_SHARP_){
return new cljs.core.Keyword(null,"source","source",-433931539).cljs$core$IFn$_invoke$arity$1(cljs.core.meta(p1__42898_SHARP_));
}),bindings)], 0)))+", got: "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([cv], 0)))),new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"error","error",-978969032),new cljs.core.Keyword("query","inputs","query/inputs",1042810394),new cljs.core.Keyword(null,"expected","expected",1583670997),bindings,new cljs.core.Keyword(null,"got","got",-1674745710),values], null));
} else {
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(datascript.query.resolve_in,context,cljs.core.zipmap(bindings,values));

}
}
});
/**
 * List of symbols in current pattern that might potentiall be resolved to refs
 */
datascript.query._STAR_lookup_attrs_STAR_ = null;
/**
 * Default pattern source. Lookup refs, patterns, rules will be resolved with it
 */
datascript.query._STAR_implicit_source_STAR_ = null;
datascript.query.getter_fn = (function datascript$query$getter_fn(attrs,attr){
var idx = (attrs.cljs$core$IFn$_invoke$arity$1 ? attrs.cljs$core$IFn$_invoke$arity$1(attr) : attrs.call(null,attr));
if(cljs.core.contains_QMARK_(datascript.query._STAR_lookup_attrs_STAR_,attr)){
if(cljs.core.int_QMARK_(idx)){
var idx__$1 = (idx | 0);
return (function datascript$query$getter_fn_$_contained_int_getter_fn(tuple){
var eid = (tuple[idx__$1]);
if(typeof eid === 'number'){
return eid;
} else {
if(cljs.core.sequential_QMARK_(eid)){
return datascript.db.entid(datascript.query._STAR_implicit_source_STAR_,eid);
} else {
if(me.tonsky.persistent_sorted_set.arrays.array_QMARK_(eid)){
return datascript.db.entid(datascript.query._STAR_implicit_source_STAR_,eid);
} else {
return eid;

}
}
}
});
} else {
return (function datascript$query$getter_fn_$_contained_getter_fn(tuple){
var eid = (tuple[idx]);
if(typeof eid === 'number'){
return eid;
} else {
if(cljs.core.sequential_QMARK_(eid)){
return datascript.db.entid(datascript.query._STAR_implicit_source_STAR_,eid);
} else {
if(me.tonsky.persistent_sorted_set.arrays.array_QMARK_(eid)){
return datascript.db.entid(datascript.query._STAR_implicit_source_STAR_,eid);
} else {
return eid;

}
}
}
});
}
} else {
if(cljs.core.int_QMARK_(idx)){
var idx__$1 = (idx | 0);
return (function datascript$query$getter_fn_$_int_getter(tuple){
return (tuple[idx__$1]);
});
} else {
return (function datascript$query$getter_fn_$_getter(tuple){
return (tuple[idx]);
});
}
}
});
datascript.query.tuple_key_fn = (function datascript$query$tuple_key_fn(attrs,common_attrs){
var n = cljs.core.count(common_attrs);
if((n === (1))){
return datascript.query.getter_fn(attrs,cljs.core.first(common_attrs));
} else {
var getters_arr = cljs.core.into_array.cljs$core$IFn$_invoke$arity$1(common_attrs);
var i = (0);
while(true){
if((i < n)){
(getters_arr[i] = datascript.query.getter_fn(attrs,(getters_arr[i])));

var G__43670 = (i + (1));
i = G__43670;
continue;
} else {
return ((function (i,getters_arr,n){
return (function (tuple){
return cljs.core.list_STAR_.cljs$core$IFn$_invoke$arity$1(getters_arr.map(((function (i,getters_arr,n){
return (function (p1__42906_SHARP_){
return (p1__42906_SHARP_.cljs$core$IFn$_invoke$arity$1 ? p1__42906_SHARP_.cljs$core$IFn$_invoke$arity$1(tuple) : p1__42906_SHARP_.call(null,tuple));
});})(i,getters_arr,n))
));
});
;})(i,getters_arr,n))
}
break;
}
}
});
datascript.query._group_by = (function datascript$query$_group_by(f,init,coll){
return cljs.core.persistent_BANG_(cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (ret,x){
var k = (f.cljs$core$IFn$_invoke$arity$1 ? f.cljs$core$IFn$_invoke$arity$1(x) : f.call(null,x));
return cljs.core.assoc_BANG_.cljs$core$IFn$_invoke$arity$3(ret,k,cljs.core.conj.cljs$core$IFn$_invoke$arity$2(cljs.core.get.cljs$core$IFn$_invoke$arity$3(ret,k,init),x));
}),cljs.core.transient$(cljs.core.PersistentArrayMap.EMPTY),coll));
});
datascript.query.hash_attrs = (function datascript$query$hash_attrs(key_fn,tuples){
return datascript.query._group_by(key_fn,cljs.core.List.EMPTY,tuples);
});
datascript.query.hash_join = (function datascript$query$hash_join(rel1,rel2){
var tuples1 = new cljs.core.Keyword(null,"tuples","tuples",-676032639).cljs$core$IFn$_invoke$arity$1(rel1);
var tuples2 = new cljs.core.Keyword(null,"tuples","tuples",-676032639).cljs$core$IFn$_invoke$arity$1(rel2);
var attrs1 = new cljs.core.Keyword(null,"attrs","attrs",-2090668713).cljs$core$IFn$_invoke$arity$1(rel1);
var attrs2 = new cljs.core.Keyword(null,"attrs","attrs",-2090668713).cljs$core$IFn$_invoke$arity$1(rel2);
var common_attrs = cljs.core.vec(datascript.query.intersect_keys(new cljs.core.Keyword(null,"attrs","attrs",-2090668713).cljs$core$IFn$_invoke$arity$1(rel1),new cljs.core.Keyword(null,"attrs","attrs",-2090668713).cljs$core$IFn$_invoke$arity$1(rel2)));
var keep_attrs1 = cljs.core.keys(attrs1);
var keep_attrs2 = cljs.core.persistent_BANG_(cljs.core.reduce_kv((function datascript$query$hash_join_$_keeper(vec,k,_){
if(cljs.core.truth_((attrs1.cljs$core$IFn$_invoke$arity$1 ? attrs1.cljs$core$IFn$_invoke$arity$1(k) : attrs1.call(null,k)))){
return vec;
} else {
return cljs.core.conj_BANG_.cljs$core$IFn$_invoke$arity$2(vec,k);
}
}),cljs.core.transient$(cljs.core.PersistentVector.EMPTY),attrs2));
var keep_idxs1 = cljs.core.to_array(cljs.core.vals(attrs1));
var keep_idxs2 = cljs.core.to_array(cljs.core.__GT_Eduction(cljs.core.map.cljs$core$IFn$_invoke$arity$1(attrs2),keep_attrs2));
var key_fn1 = datascript.query.tuple_key_fn(attrs1,common_attrs);
var key_fn2 = datascript.query.tuple_key_fn(attrs2,common_attrs);
var hash = datascript.query.hash_attrs(key_fn1,tuples1);
var new_tuples = cljs.core.persistent_BANG_(cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function datascript$query$hash_join_$_outer(acc,tuple2){
var key = key_fn2(tuple2);
var temp__5806__auto__ = cljs.core.get.cljs$core$IFn$_invoke$arity$2(hash,key);
if((temp__5806__auto__ == null)){
return acc;
} else {
var tuples1__$1 = temp__5806__auto__;
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function datascript$query$hash_join_$_outer_$_inner(acc__$1,tuple1){
return cljs.core.conj_BANG_.cljs$core$IFn$_invoke$arity$2(acc__$1,datascript.query.join_tuples(tuple1,keep_idxs1,tuple2,keep_idxs2));
}),acc,tuples1__$1);
}
}),cljs.core.transient$(cljs.core.PersistentVector.EMPTY),tuples2));
return (new datascript.query.Relation(cljs.core.zipmap(cljs.core.concat.cljs$core$IFn$_invoke$arity$2(keep_attrs1,keep_attrs2),cljs.core.range.cljs$core$IFn$_invoke$arity$0()),new_tuples,null,null,null));
});
datascript.query.subtract_rel = (function datascript$query$subtract_rel(a,b){
var map__42911 = a;
var map__42911__$1 = cljs.core.__destructure_map(map__42911);
var attrs_a = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__42911__$1,new cljs.core.Keyword(null,"attrs","attrs",-2090668713));
var tuples_a = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__42911__$1,new cljs.core.Keyword(null,"tuples","tuples",-676032639));
var map__42912 = b;
var map__42912__$1 = cljs.core.__destructure_map(map__42912);
var attrs_b = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__42912__$1,new cljs.core.Keyword(null,"attrs","attrs",-2090668713));
var tuples_b = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__42912__$1,new cljs.core.Keyword(null,"tuples","tuples",-676032639));
var attrs = cljs.core.vec(datascript.query.intersect_keys(attrs_a,attrs_b));
var key_fn_b = datascript.query.tuple_key_fn(attrs_b,attrs);
var hash = datascript.query.hash_attrs(key_fn_b,tuples_b);
var key_fn_a = datascript.query.tuple_key_fn(attrs_a,attrs);
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(a,new cljs.core.Keyword(null,"tuples","tuples",-676032639),cljs.core.filterv((function (p1__42909_SHARP_){
return ((function (){var G__42913 = key_fn_a(p1__42909_SHARP_);
return (hash.cljs$core$IFn$_invoke$arity$1 ? hash.cljs$core$IFn$_invoke$arity$1(G__42913) : hash.call(null,G__42913));
})() == null);
}),tuples_a));
});
datascript.query.rel_with_attr = (function datascript$query$rel_with_attr(context,sym){
return cljs.core.some((function (p1__42914_SHARP_){
if(cljs.core.contains_QMARK_(new cljs.core.Keyword(null,"attrs","attrs",-2090668713).cljs$core$IFn$_invoke$arity$1(p1__42914_SHARP_),sym)){
return p1__42914_SHARP_;
} else {
return null;
}
}),new cljs.core.Keyword(null,"rels","rels",1770187185).cljs$core$IFn$_invoke$arity$1(context));
});
datascript.query.substitute_constant = (function datascript$query$substitute_constant(context,pattern_el){
if(datascript.query.free_var_QMARK_(pattern_el)){
var temp__5808__auto__ = datascript.query.rel_with_attr(context,pattern_el);
if((temp__5808__auto__ == null)){
return null;
} else {
var rel = temp__5808__auto__;
var temp__5808__auto____$1 = cljs.core.first(new cljs.core.Keyword(null,"tuples","tuples",-676032639).cljs$core$IFn$_invoke$arity$1(rel));
if((temp__5808__auto____$1 == null)){
return null;
} else {
var tuple = temp__5808__auto____$1;
if((cljs.core.fnext(new cljs.core.Keyword(null,"tuples","tuples",-676032639).cljs$core$IFn$_invoke$arity$1(rel)) == null)){
var idx = cljs.core.get.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"attrs","attrs",-2090668713).cljs$core$IFn$_invoke$arity$1(rel),pattern_el);
return (tuple[idx]);
} else {
return null;
}
}
}
} else {
return null;
}
});
datascript.query.substitute_constants = (function datascript$query$substitute_constants(context,pattern){
return cljs.core.mapv.cljs$core$IFn$_invoke$arity$2((function (p1__42917_SHARP_){
var or__5142__auto__ = datascript.query.substitute_constant(context,p1__42917_SHARP_);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return p1__42917_SHARP_;
}
}),pattern);
});
datascript.query.resolve_pattern_lookup_refs = (function datascript$query$resolve_pattern_lookup_refs(source,pattern){
if((((!((source == null))))?((((false) || ((cljs.core.PROTOCOL_SENTINEL === source.datascript$db$IDB$))))?true:(((!source.cljs$lang$protocol_mask$partition$))?cljs.core.native_satisfies_QMARK_(datascript.db.IDB,source):false)):cljs.core.native_satisfies_QMARK_(datascript.db.IDB,source))){
var vec__42921 = pattern;
var e = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42921,(0),null);
var a = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42921,(1),null);
var v = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42921,(2),null);
var tx = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42921,(3),null);
var e_SINGLEQUOTE_ = ((((datascript.query.lookup_ref_QMARK_(e)) || (datascript.query.attr_QMARK_(e))))?datascript.db.entid_strict(source,e):e);
var v_SINGLEQUOTE_ = (cljs.core.truth_((function (){var and__5140__auto__ = v;
if(cljs.core.truth_(and__5140__auto__)){
return ((datascript.query.attr_QMARK_(a)) && (((datascript.db.ref_QMARK_(source,a)) && (((datascript.query.lookup_ref_QMARK_(v)) || (datascript.query.attr_QMARK_(v)))))));
} else {
return and__5140__auto__;
}
})())?datascript.db.entid_strict(source,v):v);
var tx_SINGLEQUOTE_ = ((datascript.query.lookup_ref_QMARK_(tx))?datascript.db.entid_strict(source,tx):tx);
return cljs.core.subvec.cljs$core$IFn$_invoke$arity$3(new cljs.core.PersistentVector(null, 4, 5, cljs.core.PersistentVector.EMPTY_NODE, [e_SINGLEQUOTE_,a,v_SINGLEQUOTE_,tx_SINGLEQUOTE_], null),(0),cljs.core.count(pattern));
} else {
return pattern;
}
});
datascript.query.lookup_pattern_db = (function datascript$query$lookup_pattern_db(context,db,pattern){
var search_pattern = cljs.core.mapv.cljs$core$IFn$_invoke$arity$2((function (p1__42925_SHARP_){
if(((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(p1__42925_SHARP_,new cljs.core.Symbol(null,"_","_",-1201019570,null))) || (datascript.query.free_var_QMARK_(p1__42925_SHARP_)))){
return null;
} else {
return p1__42925_SHARP_;
}
}),datascript.query.resolve_pattern_lookup_refs(db,datascript.query.substitute_constants(context,pattern)));
var datoms = datascript.db._search(db,search_pattern);
var attr__GT_prop = cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,cljs.core.filter.cljs$core$IFn$_invoke$arity$2((function (p__42928){
var vec__42929 = p__42928;
var s = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42929,(0),null);
var _ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42929,(1),null);
return datascript.query.free_var_QMARK_(s);
}),cljs.core.map.cljs$core$IFn$_invoke$arity$3(cljs.core.vector,pattern,new cljs.core.PersistentVector(null, 4, 5, cljs.core.PersistentVector.EMPTY_NODE, ["e","a","v","tx"], null))));
return (new datascript.query.Relation(attr__GT_prop,datoms,null,null,null));
});
datascript.query.matches_pattern_QMARK_ = (function datascript$query$matches_pattern_QMARK_(pattern,tuple){
var tuple__$1 = tuple;
var pattern__$1 = pattern;
while(true){
if(cljs.core.truth_((function (){var and__5140__auto__ = tuple__$1;
if(cljs.core.truth_(and__5140__auto__)){
return pattern__$1;
} else {
return and__5140__auto__;
}
})())){
var t = cljs.core.first(tuple__$1);
var p = cljs.core.first(pattern__$1);
if(((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(p,new cljs.core.Symbol(null,"_","_",-1201019570,null))) || (((datascript.query.free_var_QMARK_(p)) || (cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(t,p)))))){
var G__43679 = cljs.core.next(tuple__$1);
var G__43680 = cljs.core.next(pattern__$1);
tuple__$1 = G__43679;
pattern__$1 = G__43680;
continue;
} else {
return false;
}
} else {
return true;
}
break;
}
});
datascript.query.lookup_pattern_coll = (function datascript$query$lookup_pattern_coll(context,coll,pattern){
var data = cljs.core.filter.cljs$core$IFn$_invoke$arity$2((function (p1__42938_SHARP_){
return datascript.query.matches_pattern_QMARK_(pattern,p1__42938_SHARP_);
}),coll);
var attr__GT_idx = cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,cljs.core.filter.cljs$core$IFn$_invoke$arity$2((function (p__42941){
var vec__42942 = p__42941;
var s = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42942,(0),null);
var _ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42942,(1),null);
return datascript.query.free_var_QMARK_(s);
}),cljs.core.map.cljs$core$IFn$_invoke$arity$3(cljs.core.vector,pattern,cljs.core.range.cljs$core$IFn$_invoke$arity$0())));
return (new datascript.query.Relation(attr__GT_idx,cljs.core.mapv.cljs$core$IFn$_invoke$arity$2(cljs.core.to_array,data),null,null,null));
});
datascript.query.normalize_pattern_clause = (function datascript$query$normalize_pattern_clause(clause){
if(datascript.query.source_QMARK_(cljs.core.first(clause))){
return clause;
} else {
return cljs.core.concat.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"$","$",-1580747756,null)], null),clause);
}
});
datascript.query.lookup_pattern = (function datascript$query$lookup_pattern(context,source,pattern){
if((((!((source == null))))?((((false) || ((cljs.core.PROTOCOL_SENTINEL === source.datascript$db$ISearch$))))?true:(((!source.cljs$lang$protocol_mask$partition$))?cljs.core.native_satisfies_QMARK_(datascript.db.ISearch,source):false)):cljs.core.native_satisfies_QMARK_(datascript.db.ISearch,source))){
return datascript.query.lookup_pattern_db(context,source,pattern);
} else {
return datascript.query.lookup_pattern_coll(context,source,pattern);
}
});
datascript.query.collapse_rels = (function datascript$query$collapse_rels(rels,new_rel){
var rels__$1 = rels;
var new_rel__$1 = new_rel;
var acc = cljs.core.PersistentVector.EMPTY;
while(true){
var temp__5806__auto__ = cljs.core.first(rels__$1);
if((temp__5806__auto__ == null)){
return cljs.core.conj.cljs$core$IFn$_invoke$arity$2(acc,new_rel__$1);
} else {
var rel = temp__5806__auto__;
if(cljs.core.truth_(cljs.core.not_empty(datascript.query.intersect_keys(new cljs.core.Keyword(null,"attrs","attrs",-2090668713).cljs$core$IFn$_invoke$arity$1(new_rel__$1),new cljs.core.Keyword(null,"attrs","attrs",-2090668713).cljs$core$IFn$_invoke$arity$1(rel))))){
var G__43683 = cljs.core.next(rels__$1);
var G__43684 = datascript.query.hash_join(rel,new_rel__$1);
var G__43685 = acc;
rels__$1 = G__43683;
new_rel__$1 = G__43684;
acc = G__43685;
continue;
} else {
var G__43686 = cljs.core.next(rels__$1);
var G__43687 = new_rel__$1;
var G__43688 = cljs.core.conj.cljs$core$IFn$_invoke$arity$2(acc,rel);
rels__$1 = G__43686;
new_rel__$1 = G__43687;
acc = G__43688;
continue;
}
}
break;
}
});
datascript.query.context_resolve_val = (function datascript$query$context_resolve_val(context,sym){
var temp__5808__auto__ = datascript.query.rel_with_attr(context,sym);
if((temp__5808__auto__ == null)){
return null;
} else {
var rel = temp__5808__auto__;
var temp__5808__auto____$1 = cljs.core.first(new cljs.core.Keyword(null,"tuples","tuples",-676032639).cljs$core$IFn$_invoke$arity$1(rel));
if((temp__5808__auto____$1 == null)){
return null;
} else {
var tuple = temp__5808__auto____$1;
return (tuple[(function (){var fexpr__42963 = new cljs.core.Keyword(null,"attrs","attrs",-2090668713).cljs$core$IFn$_invoke$arity$1(rel);
return (fexpr__42963.cljs$core$IFn$_invoke$arity$1 ? fexpr__42963.cljs$core$IFn$_invoke$arity$1(sym) : fexpr__42963.call(null,sym));
})()]);
}
}
});
datascript.query.rel_contains_attrs_QMARK_ = (function datascript$query$rel_contains_attrs_QMARK_(rel,attrs){
return cljs.core.some((function (p1__42965_SHARP_){
return cljs.core.contains_QMARK_(new cljs.core.Keyword(null,"attrs","attrs",-2090668713).cljs$core$IFn$_invoke$arity$1(rel),p1__42965_SHARP_);
}),attrs);
});
datascript.query.rel_prod_by_attrs = (function datascript$query$rel_prod_by_attrs(context,attrs){
var rels = cljs.core.filter.cljs$core$IFn$_invoke$arity$2((function (p1__42966_SHARP_){
return datascript.query.rel_contains_attrs_QMARK_(p1__42966_SHARP_,attrs);
}),new cljs.core.Keyword(null,"rels","rels",1770187185).cljs$core$IFn$_invoke$arity$1(context));
var production = cljs.core.reduce.cljs$core$IFn$_invoke$arity$2(datascript.query.prod_rel,rels);
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [cljs.core.update.cljs$core$IFn$_invoke$arity$3(context,new cljs.core.Keyword(null,"rels","rels",1770187185),(function (p1__42967_SHARP_){
return cljs.core.remove.cljs$core$IFn$_invoke$arity$2(cljs.core.set(rels),p1__42967_SHARP_);
})),production], null);
});
datascript.query._call_fn = (function datascript$query$_call_fn(context,rel,f,args){
var sources = new cljs.core.Keyword(null,"sources","sources",-321166424).cljs$core$IFn$_invoke$arity$1(context);
var attrs = new cljs.core.Keyword(null,"attrs","attrs",-2090668713).cljs$core$IFn$_invoke$arity$1(rel);
var len = cljs.core.count(args);
var static_args = me.tonsky.persistent_sorted_set.arrays.make_array(len);
var tuples_args = me.tonsky.persistent_sorted_set.arrays.make_array(len);
var n__5741__auto___43691 = len;
var i_43692 = (0);
while(true){
if((i_43692 < n__5741__auto___43691)){
var arg_43694 = cljs.core.nth.cljs$core$IFn$_invoke$arity$2(args,i_43692);
if((arg_43694 instanceof cljs.core.Symbol)){
var temp__5806__auto___43695 = cljs.core.get.cljs$core$IFn$_invoke$arity$2(sources,arg_43694);
if((temp__5806__auto___43695 == null)){
(tuples_args[i_43692] = cljs.core.get.cljs$core$IFn$_invoke$arity$2(attrs,arg_43694));
} else {
var source_43696 = temp__5806__auto___43695;
(static_args[i_43692] = source_43696);
}
} else {
(static_args[i_43692] = arg_43694);
}

var G__43697 = (i_43692 + (1));
i_43692 = G__43697;
continue;
} else {
}
break;
}

if((f === cljs.core.vector)){
return (function (tuple){
var args__$1 = me.tonsky.persistent_sorted_set.arrays.aclone(static_args);
var n__5741__auto___43698 = len;
var i_43699 = (0);
while(true){
if((i_43699 < n__5741__auto___43698)){
var temp__5808__auto___43700 = (tuples_args[i_43699]);
if((temp__5808__auto___43700 == null)){
} else {
var tuple_idx_43701 = temp__5808__auto___43700;
var v_43702 = (tuple[tuple_idx_43701]);
(args__$1[i_43699] = v_43702);
}

var G__43703 = (i_43699 + (1));
i_43699 = G__43703;
continue;
} else {
}
break;
}

return cljs.core.apply.cljs$core$IFn$_invoke$arity$2(f,args__$1);
});
} else {
return (function (tuple){
var n__5741__auto___43705 = len;
var i_43706 = (0);
while(true){
if((i_43706 < n__5741__auto___43705)){
var temp__5808__auto___43707 = (tuples_args[i_43706]);
if((temp__5808__auto___43707 == null)){
} else {
var tuple_idx_43708 = temp__5808__auto___43707;
var v_43709 = (tuple[tuple_idx_43708]);
(static_args[i_43706] = v_43709);
}

var G__43710 = (i_43706 + (1));
i_43706 = G__43710;
continue;
} else {
}
break;
}

return cljs.core.apply.cljs$core$IFn$_invoke$arity$2(f,static_args);
});
}
});
datascript.query.resolve_sym = (function datascript$query$resolve_sym(sym){
return null;
});
datascript.query.filter_by_pred = (function datascript$query$filter_by_pred(context,clause){
var vec__42973 = clause;
var vec__42976 = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42973,(0),null);
var seq__42977 = cljs.core.seq(vec__42976);
var first__42978 = cljs.core.first(seq__42977);
var seq__42977__$1 = cljs.core.next(seq__42977);
var f = first__42978;
var args = seq__42977__$1;
var pred = (function (){var or__5142__auto__ = cljs.core.get.cljs$core$IFn$_invoke$arity$2(datascript.built_ins.query_fns,f);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
var or__5142__auto____$1 = datascript.query.context_resolve_val(context,f);
if(cljs.core.truth_(or__5142__auto____$1)){
return or__5142__auto____$1;
} else {
var or__5142__auto____$2 = datascript.query.resolve_sym(f);
if(cljs.core.truth_(or__5142__auto____$2)){
return or__5142__auto____$2;
} else {
if((datascript.query.rel_with_attr(context,f) == null)){
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2((""+"Unknown predicate '"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([f], 0)))+" in "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([clause], 0)))),new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"error","error",-978969032),new cljs.core.Keyword("query","where","query/where",-1935159429),new cljs.core.Keyword(null,"form","form",-1624062471),clause,new cljs.core.Keyword(null,"var","var",-769682797),f], null));
} else {
return null;
}
}
}
}
})();
var vec__42979 = datascript.query.rel_prod_by_attrs(context,cljs.core.filter.cljs$core$IFn$_invoke$arity$2(cljs.core.symbol_QMARK_,args));
var context__$1 = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42979,(0),null);
var production = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42979,(1),null);
var new_rel = (cljs.core.truth_(pred)?(function (){var tuple_pred = datascript.query._call_fn(context__$1,production,pred,args);
return cljs.core.update.cljs$core$IFn$_invoke$arity$3(production,new cljs.core.Keyword(null,"tuples","tuples",-676032639),(function (p1__42972_SHARP_){
return cljs.core.filter.cljs$core$IFn$_invoke$arity$2(tuple_pred,p1__42972_SHARP_);
}));
})():cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(production,new cljs.core.Keyword(null,"tuples","tuples",-676032639),cljs.core.PersistentVector.EMPTY));
return cljs.core.update.cljs$core$IFn$_invoke$arity$4(context__$1,new cljs.core.Keyword(null,"rels","rels",1770187185),cljs.core.conj,new_rel);
});
datascript.query.bind_by_fn = (function datascript$query$bind_by_fn(context,clause){
var vec__42983 = clause;
var vec__42986 = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42983,(0),null);
var seq__42987 = cljs.core.seq(vec__42986);
var first__42988 = cljs.core.first(seq__42987);
var seq__42987__$1 = cljs.core.next(seq__42987);
var f = first__42988;
var args = seq__42987__$1;
var out = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42983,(1),null);
var binding = datascript.parser.parse_binding(out);
var fun = (function (){var or__5142__auto__ = cljs.core.get.cljs$core$IFn$_invoke$arity$2(datascript.built_ins.query_fns,f);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
var or__5142__auto____$1 = datascript.query.context_resolve_val(context,f);
if(cljs.core.truth_(or__5142__auto____$1)){
return or__5142__auto____$1;
} else {
var or__5142__auto____$2 = datascript.query.resolve_sym(f);
if(cljs.core.truth_(or__5142__auto____$2)){
return or__5142__auto____$2;
} else {
if((datascript.query.rel_with_attr(context,f) == null)){
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2((""+"Unknown function '"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([f], 0)))+" in "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([clause], 0)))),new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"error","error",-978969032),new cljs.core.Keyword("query","where","query/where",-1935159429),new cljs.core.Keyword(null,"form","form",-1624062471),clause,new cljs.core.Keyword(null,"var","var",-769682797),f], null));
} else {
return null;
}
}
}
}
})();
var vec__42989 = datascript.query.rel_prod_by_attrs(context,cljs.core.filter.cljs$core$IFn$_invoke$arity$2(cljs.core.symbol_QMARK_,args));
var context__$1 = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42989,(0),null);
var production = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42989,(1),null);
var new_rel = (cljs.core.truth_(fun)?(function (){var tuple_fn = datascript.query._call_fn(context__$1,production,fun,args);
var rels = (function (){var iter__5628__auto__ = (function datascript$query$bind_by_fn_$_iter__42998(s__42999){
return (new cljs.core.LazySeq(null,(function (){
var s__42999__$1 = s__42999;
while(true){
var temp__5804__auto__ = cljs.core.seq(s__42999__$1);
if(temp__5804__auto__){
var s__42999__$2 = temp__5804__auto__;
if(cljs.core.chunked_seq_QMARK_(s__42999__$2)){
var c__5626__auto__ = cljs.core.chunk_first(s__42999__$2);
var size__5627__auto__ = cljs.core.count(c__5626__auto__);
var b__43001 = cljs.core.chunk_buffer(size__5627__auto__);
if((function (){var i__43000 = (0);
while(true){
if((i__43000 < size__5627__auto__)){
var tuple = cljs.core._nth(c__5626__auto__,i__43000);
var val = tuple_fn(tuple);
if((!((val == null)))){
cljs.core.chunk_append(b__43001,datascript.query.prod_rel.cljs$core$IFn$_invoke$arity$2((new datascript.query.Relation(new cljs.core.Keyword(null,"attrs","attrs",-2090668713).cljs$core$IFn$_invoke$arity$1(production),new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [tuple], null),null,null,null)),datascript.query.in__GT_rel(binding,val)));

var G__43713 = (i__43000 + (1));
i__43000 = G__43713;
continue;
} else {
var G__43714 = (i__43000 + (1));
i__43000 = G__43714;
continue;
}
} else {
return true;
}
break;
}
})()){
return cljs.core.chunk_cons(cljs.core.chunk(b__43001),datascript$query$bind_by_fn_$_iter__42998(cljs.core.chunk_rest(s__42999__$2)));
} else {
return cljs.core.chunk_cons(cljs.core.chunk(b__43001),null);
}
} else {
var tuple = cljs.core.first(s__42999__$2);
var val = tuple_fn(tuple);
if((!((val == null)))){
return cljs.core.cons(datascript.query.prod_rel.cljs$core$IFn$_invoke$arity$2((new datascript.query.Relation(new cljs.core.Keyword(null,"attrs","attrs",-2090668713).cljs$core$IFn$_invoke$arity$1(production),new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [tuple], null),null,null,null)),datascript.query.in__GT_rel(binding,val)),datascript$query$bind_by_fn_$_iter__42998(cljs.core.rest(s__42999__$2)));
} else {
var G__43716 = cljs.core.rest(s__42999__$2);
s__42999__$1 = G__43716;
continue;
}
}
} else {
return null;
}
break;
}
}),null,null));
});
return iter__5628__auto__(new cljs.core.Keyword(null,"tuples","tuples",-676032639).cljs$core$IFn$_invoke$arity$1(production));
})();
if(cljs.core.empty_QMARK_(rels)){
return datascript.query.prod_rel.cljs$core$IFn$_invoke$arity$2(production,datascript.query.empty_rel(binding));
} else {
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$2(datascript.query.sum_rel,rels);
}
})():datascript.query.prod_rel.cljs$core$IFn$_invoke$arity$2(cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(production,new cljs.core.Keyword(null,"tuples","tuples",-676032639),cljs.core.PersistentVector.EMPTY),datascript.query.empty_rel(binding)));
return cljs.core.update.cljs$core$IFn$_invoke$arity$4(context__$1,new cljs.core.Keyword(null,"rels","rels",1770187185),datascript.query.collapse_rels,new_rel);
});
datascript.query.rule_QMARK_ = (function datascript$query$rule_QMARK_(context,clause){
if((!(cljs.core.sequential_QMARK_(clause)))){
return false;
} else {
var head = ((datascript.query.source_QMARK_(cljs.core.first(clause)))?cljs.core.second(clause):cljs.core.first(clause));
if((!((head instanceof cljs.core.Symbol)))){
return false;
} else {
if(datascript.query.free_var_QMARK_(head)){
return false;
} else {
if(cljs.core.contains_QMARK_(new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 6, [new cljs.core.Symbol(null,"and","and",668631710,null),null,new cljs.core.Symbol(null,"not","not",1044554643,null),null,new cljs.core.Symbol(null,"not-join","not-join",-645515756,null),null,new cljs.core.Symbol(null,"or-join","or-join",591375469,null),null,new cljs.core.Symbol(null,"or","or",1876275696,null),null,new cljs.core.Symbol(null,"_","_",-1201019570,null),null], null), null),head)){
return false;
} else {
if((!(cljs.core.contains_QMARK_(new cljs.core.Keyword(null,"rules","rules",1198912366).cljs$core$IFn$_invoke$arity$1(context),head)))){
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2((""+"Unknown rule '"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([head], 0)))+" in "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([clause], 0)))),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"error","error",-978969032),new cljs.core.Keyword("query","where","query/where",-1935159429),new cljs.core.Keyword(null,"form","form",-1624062471),clause], null));
} else {
return true;

}
}
}
}
}
});
datascript.query.rule_seqid = cljs.core.atom.cljs$core$IFn$_invoke$arity$1((0));
datascript.query.expand_rule = (function datascript$query$expand_rule(clause,context,used_args){
var vec__43007 = clause;
var seq__43008 = cljs.core.seq(vec__43007);
var first__43009 = cljs.core.first(seq__43008);
var seq__43008__$1 = cljs.core.next(seq__43008);
var rule = first__43009;
var call_args = seq__43008__$1;
var seqid = cljs.core.swap_BANG_.cljs$core$IFn$_invoke$arity$2(datascript.query.rule_seqid,cljs.core.inc);
var branches = cljs.core.get.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"rules","rules",1198912366).cljs$core$IFn$_invoke$arity$1(context),rule);
var iter__5628__auto__ = (function datascript$query$expand_rule_$_iter__43011(s__43012){
return (new cljs.core.LazySeq(null,(function (){
var s__43012__$1 = s__43012;
while(true){
var temp__5804__auto__ = cljs.core.seq(s__43012__$1);
if(temp__5804__auto__){
var s__43012__$2 = temp__5804__auto__;
if(cljs.core.chunked_seq_QMARK_(s__43012__$2)){
var c__5626__auto__ = cljs.core.chunk_first(s__43012__$2);
var size__5627__auto__ = cljs.core.count(c__5626__auto__);
var b__43014 = cljs.core.chunk_buffer(size__5627__auto__);
if((function (){var i__43013 = (0);
while(true){
if((i__43013 < size__5627__auto__)){
var branch = cljs.core._nth(c__5626__auto__,i__43013);
var vec__43022 = branch;
var seq__43023 = cljs.core.seq(vec__43022);
var first__43024 = cljs.core.first(seq__43023);
var seq__43023__$1 = cljs.core.next(seq__43023);
var vec__43025 = first__43024;
var seq__43026 = cljs.core.seq(vec__43025);
var first__43027 = cljs.core.first(seq__43026);
var seq__43026__$1 = cljs.core.next(seq__43026);
var _ = first__43027;
var rule_args = seq__43026__$1;
var clauses = seq__43023__$1;
var replacements = cljs.core.zipmap(rule_args,call_args);
cljs.core.chunk_append(b__43014,clojure.walk.postwalk(((function (i__43013,vec__43022,seq__43023,first__43024,seq__43023__$1,vec__43025,seq__43026,first__43027,seq__43026__$1,_,rule_args,clauses,replacements,branch,c__5626__auto__,size__5627__auto__,b__43014,s__43012__$2,temp__5804__auto__,vec__43007,seq__43008,first__43009,seq__43008__$1,rule,call_args,seqid,branches){
return (function (p1__43005_SHARP_){
if(datascript.query.free_var_QMARK_(p1__43005_SHARP_)){
var x__34211__auto__ = (replacements.cljs$core$IFn$_invoke$arity$1 ? replacements.cljs$core$IFn$_invoke$arity$1(p1__43005_SHARP_) : replacements.call(null,p1__43005_SHARP_));
if((x__34211__auto__ == null)){
return cljs.core.symbol.cljs$core$IFn$_invoke$arity$1((""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.name(p1__43005_SHARP_))+"__auto__"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(seqid)));
} else {
return x__34211__auto__;
}
} else {
return p1__43005_SHARP_;
}
});})(i__43013,vec__43022,seq__43023,first__43024,seq__43023__$1,vec__43025,seq__43026,first__43027,seq__43026__$1,_,rule_args,clauses,replacements,branch,c__5626__auto__,size__5627__auto__,b__43014,s__43012__$2,temp__5804__auto__,vec__43007,seq__43008,first__43009,seq__43008__$1,rule,call_args,seqid,branches))
,clauses));

var G__43723 = (i__43013 + (1));
i__43013 = G__43723;
continue;
} else {
return true;
}
break;
}
})()){
return cljs.core.chunk_cons(cljs.core.chunk(b__43014),datascript$query$expand_rule_$_iter__43011(cljs.core.chunk_rest(s__43012__$2)));
} else {
return cljs.core.chunk_cons(cljs.core.chunk(b__43014),null);
}
} else {
var branch = cljs.core.first(s__43012__$2);
var vec__43031 = branch;
var seq__43032 = cljs.core.seq(vec__43031);
var first__43033 = cljs.core.first(seq__43032);
var seq__43032__$1 = cljs.core.next(seq__43032);
var vec__43034 = first__43033;
var seq__43035 = cljs.core.seq(vec__43034);
var first__43036 = cljs.core.first(seq__43035);
var seq__43035__$1 = cljs.core.next(seq__43035);
var _ = first__43036;
var rule_args = seq__43035__$1;
var clauses = seq__43032__$1;
var replacements = cljs.core.zipmap(rule_args,call_args);
return cljs.core.cons(clojure.walk.postwalk(((function (vec__43031,seq__43032,first__43033,seq__43032__$1,vec__43034,seq__43035,first__43036,seq__43035__$1,_,rule_args,clauses,replacements,branch,s__43012__$2,temp__5804__auto__,vec__43007,seq__43008,first__43009,seq__43008__$1,rule,call_args,seqid,branches){
return (function (p1__43005_SHARP_){
if(datascript.query.free_var_QMARK_(p1__43005_SHARP_)){
var x__34211__auto__ = (replacements.cljs$core$IFn$_invoke$arity$1 ? replacements.cljs$core$IFn$_invoke$arity$1(p1__43005_SHARP_) : replacements.call(null,p1__43005_SHARP_));
if((x__34211__auto__ == null)){
return cljs.core.symbol.cljs$core$IFn$_invoke$arity$1((""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.name(p1__43005_SHARP_))+"__auto__"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(seqid)));
} else {
return x__34211__auto__;
}
} else {
return p1__43005_SHARP_;
}
});})(vec__43031,seq__43032,first__43033,seq__43032__$1,vec__43034,seq__43035,first__43036,seq__43035__$1,_,rule_args,clauses,replacements,branch,s__43012__$2,temp__5804__auto__,vec__43007,seq__43008,first__43009,seq__43008__$1,rule,call_args,seqid,branches))
,clauses),datascript$query$expand_rule_$_iter__43011(cljs.core.rest(s__43012__$2)));
}
} else {
return null;
}
break;
}
}),null,null));
});
return iter__5628__auto__(branches);
});
datascript.query.remove_pairs = (function datascript$query$remove_pairs(xs,ys){
var pairs = cljs.core.remove.cljs$core$IFn$_invoke$arity$2((function (p__43044){
var vec__43047 = p__43044;
var x = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__43047,(0),null);
var y = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__43047,(1),null);
return cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(x,y);
}),cljs.core.map.cljs$core$IFn$_invoke$arity$3(cljs.core.vector,xs,ys));
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [cljs.core.map.cljs$core$IFn$_invoke$arity$2(cljs.core.first,pairs),cljs.core.map.cljs$core$IFn$_invoke$arity$2(cljs.core.second,pairs)], null);
});
datascript.query.rule_gen_guards = (function datascript$query$rule_gen_guards(rule_clause,used_args){
var vec__43059 = rule_clause;
var seq__43060 = cljs.core.seq(vec__43059);
var first__43061 = cljs.core.first(seq__43060);
var seq__43060__$1 = cljs.core.next(seq__43060);
var rule = first__43061;
var call_args = seq__43060__$1;
var prev_call_args = cljs.core.get.cljs$core$IFn$_invoke$arity$2(used_args,rule);
var iter__5628__auto__ = (function datascript$query$rule_gen_guards_$_iter__43062(s__43063){
return (new cljs.core.LazySeq(null,(function (){
var s__43063__$1 = s__43063;
while(true){
var temp__5804__auto__ = cljs.core.seq(s__43063__$1);
if(temp__5804__auto__){
var s__43063__$2 = temp__5804__auto__;
if(cljs.core.chunked_seq_QMARK_(s__43063__$2)){
var c__5626__auto__ = cljs.core.chunk_first(s__43063__$2);
var size__5627__auto__ = cljs.core.count(c__5626__auto__);
var b__43065 = cljs.core.chunk_buffer(size__5627__auto__);
if((function (){var i__43064 = (0);
while(true){
if((i__43064 < size__5627__auto__)){
var prev_args = cljs.core._nth(c__5626__auto__,i__43064);
var vec__43070 = datascript.query.remove_pairs(call_args,prev_args);
var call_args__$1 = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__43070,(0),null);
var prev_args__$1 = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__43070,(1),null);
cljs.core.chunk_append(b__43065,new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [cljs.core.concat.cljs$core$IFn$_invoke$arity$variadic(new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"-differ?","-differ?",1465687357,null)], null),call_args__$1,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([prev_args__$1], 0))], null));

var G__43731 = (i__43064 + (1));
i__43064 = G__43731;
continue;
} else {
return true;
}
break;
}
})()){
return cljs.core.chunk_cons(cljs.core.chunk(b__43065),datascript$query$rule_gen_guards_$_iter__43062(cljs.core.chunk_rest(s__43063__$2)));
} else {
return cljs.core.chunk_cons(cljs.core.chunk(b__43065),null);
}
} else {
var prev_args = cljs.core.first(s__43063__$2);
var vec__43078 = datascript.query.remove_pairs(call_args,prev_args);
var call_args__$1 = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__43078,(0),null);
var prev_args__$1 = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__43078,(1),null);
return cljs.core.cons(new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [cljs.core.concat.cljs$core$IFn$_invoke$arity$variadic(new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"-differ?","-differ?",1465687357,null)], null),call_args__$1,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([prev_args__$1], 0))], null),datascript$query$rule_gen_guards_$_iter__43062(cljs.core.rest(s__43063__$2)));
}
} else {
return null;
}
break;
}
}),null,null));
});
return iter__5628__auto__(prev_call_args);
});
datascript.query.walk_collect = (function datascript$query$walk_collect(form,pred){
var res = cljs.core.atom.cljs$core$IFn$_invoke$arity$1(cljs.core.PersistentVector.EMPTY);
clojure.walk.postwalk((function (p1__43082_SHARP_){
if(cljs.core.truth_((pred.cljs$core$IFn$_invoke$arity$1 ? pred.cljs$core$IFn$_invoke$arity$1(p1__43082_SHARP_) : pred.call(null,p1__43082_SHARP_)))){
cljs.core.swap_BANG_.cljs$core$IFn$_invoke$arity$3(res,cljs.core.conj,p1__43082_SHARP_);
} else {
}

return p1__43082_SHARP_;
}),form);

return cljs.core.deref(res);
});
datascript.query.collect_vars = (function datascript$query$collect_vars(clause){
return cljs.core.set(datascript.query.walk_collect(clause,datascript.query.free_var_QMARK_));
});
datascript.query.split_guards = (function datascript$query$split_guards(clauses,guards){
var bound_vars = datascript.query.collect_vars(clauses);
var pred = (function (p__43096){
var vec__43097 = p__43096;
var vec__43100 = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__43097,(0),null);
var seq__43101 = cljs.core.seq(vec__43100);
var first__43102 = cljs.core.first(seq__43101);
var seq__43101__$1 = cljs.core.next(seq__43101);
var _ = first__43102;
var vars = seq__43101__$1;
return cljs.core.every_QMARK_(bound_vars,vars);
});
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [cljs.core.filter.cljs$core$IFn$_invoke$arity$2(pred,guards),cljs.core.remove.cljs$core$IFn$_invoke$arity$2(pred,guards)], null);
});
datascript.query.solve_rule = (function datascript$query$solve_rule(context,clause){
var final_attrs = cljs.core.filter.cljs$core$IFn$_invoke$arity$2(datascript.query.free_var_QMARK_,clause);
var final_attrs_map = cljs.core.zipmap(final_attrs,cljs.core.range.cljs$core$IFn$_invoke$arity$0());
var solve = (function (prefix_context,clauses){
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(datascript.query._resolve_clause,prefix_context,clauses);
});
var empty_rels_QMARK_ = (function (context__$1){
return cljs.core.some((function (p1__43103_SHARP_){
return cljs.core.empty_QMARK_(new cljs.core.Keyword(null,"tuples","tuples",-676032639).cljs$core$IFn$_invoke$arity$1(p1__43103_SHARP_));
}),new cljs.core.Keyword(null,"rels","rels",1770187185).cljs$core$IFn$_invoke$arity$1(context__$1));
});
var stack = (new cljs.core.List(null,new cljs.core.PersistentArrayMap(null, 5, [new cljs.core.Keyword(null,"prefix-clauses","prefix-clauses",1294180028),cljs.core.PersistentVector.EMPTY,new cljs.core.Keyword(null,"prefix-context","prefix-context",-1269613591),context,new cljs.core.Keyword(null,"clauses","clauses",1454841241),new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [clause], null),new cljs.core.Keyword(null,"used-args","used-args",23596256),cljs.core.PersistentArrayMap.EMPTY,new cljs.core.Keyword(null,"pending-guards","pending-guards",-1255527308),cljs.core.PersistentArrayMap.EMPTY], null),null,(1),null));
var rel = (new datascript.query.Relation(final_attrs_map,cljs.core.PersistentVector.EMPTY,null,null,null));
while(true){
var temp__5806__auto__ = cljs.core.first(stack);
if((temp__5806__auto__ == null)){
return rel;
} else {
var frame = temp__5806__auto__;
var vec__43181 = cljs.core.split_with(((function (stack,rel,frame,temp__5806__auto__,final_attrs,final_attrs_map,solve,empty_rels_QMARK_){
return (function (p1__43104_SHARP_){
return (!(datascript.query.rule_QMARK_(context,p1__43104_SHARP_)));
});})(stack,rel,frame,temp__5806__auto__,final_attrs,final_attrs_map,solve,empty_rels_QMARK_))
,new cljs.core.Keyword(null,"clauses","clauses",1454841241).cljs$core$IFn$_invoke$arity$1(frame));
var clauses = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__43181,(0),null);
var vec__43184 = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__43181,(1),null);
var seq__43185 = cljs.core.seq(vec__43184);
var first__43186 = cljs.core.first(seq__43185);
var seq__43185__$1 = cljs.core.next(seq__43185);
var rule_clause = first__43186;
var next_clauses = seq__43185__$1;
if((rule_clause == null)){
var context__$1 = solve(new cljs.core.Keyword(null,"prefix-context","prefix-context",-1269613591).cljs$core$IFn$_invoke$arity$1(frame),clauses);
var tuples = datascript.util.distinct_by(cljs.core.vec,(datascript.query._collect.cljs$core$IFn$_invoke$arity$2 ? datascript.query._collect.cljs$core$IFn$_invoke$arity$2(context__$1,final_attrs) : datascript.query._collect.call(null,context__$1,final_attrs)));
var new_rel = (new datascript.query.Relation(final_attrs_map,tuples,null,null,null));
var G__43737 = cljs.core.next(stack);
var G__43738 = datascript.query.sum_rel(rel,new_rel);
stack = G__43737;
rel = G__43738;
continue;
} else {
var vec__43188 = rule_clause;
var seq__43189 = cljs.core.seq(vec__43188);
var first__43190 = cljs.core.first(seq__43189);
var seq__43189__$1 = cljs.core.next(seq__43189);
var rule = first__43190;
var call_args = seq__43189__$1;
var guards = datascript.query.rule_gen_guards(rule_clause,new cljs.core.Keyword(null,"used-args","used-args",23596256).cljs$core$IFn$_invoke$arity$1(frame));
var vec__43191 = datascript.query.split_guards(cljs.core.concat.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"prefix-clauses","prefix-clauses",1294180028).cljs$core$IFn$_invoke$arity$1(frame),clauses),cljs.core.concat.cljs$core$IFn$_invoke$arity$2(guards,new cljs.core.Keyword(null,"pending-guards","pending-guards",-1255527308).cljs$core$IFn$_invoke$arity$1(frame)));
var active_gs = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__43191,(0),null);
var pending_gs = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__43191,(1),null);
if(cljs.core.truth_(cljs.core.some(((function (stack,rel,vec__43188,seq__43189,first__43190,seq__43189__$1,rule,call_args,guards,vec__43191,active_gs,pending_gs,vec__43181,clauses,vec__43184,seq__43185,first__43186,seq__43185__$1,rule_clause,next_clauses,frame,temp__5806__auto__,final_attrs,final_attrs_map,solve,empty_rels_QMARK_){
return (function (p1__43105_SHARP_){
return cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(p1__43105_SHARP_,new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [cljs.core.list(new cljs.core.Symbol(null,"-differ?","-differ?",1465687357,null))], null));
});})(stack,rel,vec__43188,seq__43189,first__43190,seq__43189__$1,rule,call_args,guards,vec__43191,active_gs,pending_gs,vec__43181,clauses,vec__43184,seq__43185,first__43186,seq__43185__$1,rule_clause,next_clauses,frame,temp__5806__auto__,final_attrs,final_attrs_map,solve,empty_rels_QMARK_))
,active_gs))){
var G__43742 = cljs.core.next(stack);
var G__43743 = rel;
stack = G__43742;
rel = G__43743;
continue;
} else {
var prefix_clauses = cljs.core.concat.cljs$core$IFn$_invoke$arity$2(clauses,active_gs);
var prefix_context = solve(new cljs.core.Keyword(null,"prefix-context","prefix-context",-1269613591).cljs$core$IFn$_invoke$arity$1(frame),prefix_clauses);
if(cljs.core.truth_(empty_rels_QMARK_(prefix_context))){
var G__43746 = cljs.core.next(stack);
var G__43747 = rel;
stack = G__43746;
rel = G__43747;
continue;
} else {
var used_args = cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(new cljs.core.Keyword(null,"used-args","used-args",23596256).cljs$core$IFn$_invoke$arity$1(frame),rule,cljs.core.conj.cljs$core$IFn$_invoke$arity$2(cljs.core.get.cljs$core$IFn$_invoke$arity$3(new cljs.core.Keyword(null,"used-args","used-args",23596256).cljs$core$IFn$_invoke$arity$1(frame),rule,cljs.core.PersistentVector.EMPTY),call_args));
var branches = datascript.query.expand_rule(rule_clause,context,used_args);
var G__43748 = cljs.core.concat.cljs$core$IFn$_invoke$arity$2((function (){var iter__5628__auto__ = ((function (stack,rel,used_args,branches,prefix_clauses,prefix_context,vec__43188,seq__43189,first__43190,seq__43189__$1,rule,call_args,guards,vec__43191,active_gs,pending_gs,vec__43181,clauses,vec__43184,seq__43185,first__43186,seq__43185__$1,rule_clause,next_clauses,frame,temp__5806__auto__,final_attrs,final_attrs_map,solve,empty_rels_QMARK_){
return (function datascript$query$solve_rule_$_iter__43197(s__43198){
return (new cljs.core.LazySeq(null,((function (stack,rel,used_args,branches,prefix_clauses,prefix_context,vec__43188,seq__43189,first__43190,seq__43189__$1,rule,call_args,guards,vec__43191,active_gs,pending_gs,vec__43181,clauses,vec__43184,seq__43185,first__43186,seq__43185__$1,rule_clause,next_clauses,frame,temp__5806__auto__,final_attrs,final_attrs_map,solve,empty_rels_QMARK_){
return (function (){
var s__43198__$1 = s__43198;
while(true){
var temp__5804__auto__ = cljs.core.seq(s__43198__$1);
if(temp__5804__auto__){
var s__43198__$2 = temp__5804__auto__;
if(cljs.core.chunked_seq_QMARK_(s__43198__$2)){
var c__5626__auto__ = cljs.core.chunk_first(s__43198__$2);
var size__5627__auto__ = cljs.core.count(c__5626__auto__);
var b__43200 = cljs.core.chunk_buffer(size__5627__auto__);
if((function (){var i__43199 = (0);
while(true){
if((i__43199 < size__5627__auto__)){
var branch = cljs.core._nth(c__5626__auto__,i__43199);
cljs.core.chunk_append(b__43200,new cljs.core.PersistentArrayMap(null, 5, [new cljs.core.Keyword(null,"prefix-clauses","prefix-clauses",1294180028),prefix_clauses,new cljs.core.Keyword(null,"prefix-context","prefix-context",-1269613591),prefix_context,new cljs.core.Keyword(null,"clauses","clauses",1454841241),datascript.util.concatv.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([branch,next_clauses], 0)),new cljs.core.Keyword(null,"used-args","used-args",23596256),used_args,new cljs.core.Keyword(null,"pending-guards","pending-guards",-1255527308),pending_gs], null));

var G__43761 = (i__43199 + (1));
i__43199 = G__43761;
continue;
} else {
return true;
}
break;
}
})()){
return cljs.core.chunk_cons(cljs.core.chunk(b__43200),datascript$query$solve_rule_$_iter__43197(cljs.core.chunk_rest(s__43198__$2)));
} else {
return cljs.core.chunk_cons(cljs.core.chunk(b__43200),null);
}
} else {
var branch = cljs.core.first(s__43198__$2);
return cljs.core.cons(new cljs.core.PersistentArrayMap(null, 5, [new cljs.core.Keyword(null,"prefix-clauses","prefix-clauses",1294180028),prefix_clauses,new cljs.core.Keyword(null,"prefix-context","prefix-context",-1269613591),prefix_context,new cljs.core.Keyword(null,"clauses","clauses",1454841241),datascript.util.concatv.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([branch,next_clauses], 0)),new cljs.core.Keyword(null,"used-args","used-args",23596256),used_args,new cljs.core.Keyword(null,"pending-guards","pending-guards",-1255527308),pending_gs], null),datascript$query$solve_rule_$_iter__43197(cljs.core.rest(s__43198__$2)));
}
} else {
return null;
}
break;
}
});})(stack,rel,used_args,branches,prefix_clauses,prefix_context,vec__43188,seq__43189,first__43190,seq__43189__$1,rule,call_args,guards,vec__43191,active_gs,pending_gs,vec__43181,clauses,vec__43184,seq__43185,first__43186,seq__43185__$1,rule_clause,next_clauses,frame,temp__5806__auto__,final_attrs,final_attrs_map,solve,empty_rels_QMARK_))
,null,null));
});})(stack,rel,used_args,branches,prefix_clauses,prefix_context,vec__43188,seq__43189,first__43190,seq__43189__$1,rule,call_args,guards,vec__43191,active_gs,pending_gs,vec__43181,clauses,vec__43184,seq__43185,first__43186,seq__43185__$1,rule_clause,next_clauses,frame,temp__5806__auto__,final_attrs,final_attrs_map,solve,empty_rels_QMARK_))
;
return iter__5628__auto__(branches);
})(),cljs.core.next(stack));
var G__43749 = rel;
stack = G__43748;
rel = G__43749;
continue;
}
}
}
}
break;
}
});
datascript.query.dynamic_lookup_attrs = (function datascript$query$dynamic_lookup_attrs(source,pattern){
var vec__43209 = pattern;
var e = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__43209,(0),null);
var a = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__43209,(1),null);
var v = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__43209,(2),null);
var tx = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__43209,(3),null);
var G__43214 = cljs.core.PersistentHashSet.EMPTY;
var G__43214__$1 = ((datascript.query.free_var_QMARK_(e))?cljs.core.conj.cljs$core$IFn$_invoke$arity$2(G__43214,e):G__43214);
var G__43214__$2 = ((datascript.query.free_var_QMARK_(tx))?cljs.core.conj.cljs$core$IFn$_invoke$arity$2(G__43214__$1,tx):G__43214__$1);
if(((datascript.query.free_var_QMARK_(v)) && ((((!(datascript.query.free_var_QMARK_(a)))) && (datascript.db.ref_QMARK_(source,a)))))){
return cljs.core.conj.cljs$core$IFn$_invoke$arity$2(G__43214__$2,v);
} else {
return G__43214__$2;
}
});
datascript.query.limit_rel = (function datascript$query$limit_rel(rel,vars){
var temp__5808__auto__ = cljs.core.not_empty(cljs.core.select_keys(new cljs.core.Keyword(null,"attrs","attrs",-2090668713).cljs$core$IFn$_invoke$arity$1(rel),vars));
if((temp__5808__auto__ == null)){
return null;
} else {
var attrs_SINGLEQUOTE_ = temp__5808__auto__;
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(rel,new cljs.core.Keyword(null,"attrs","attrs",-2090668713),attrs_SINGLEQUOTE_);
}
});
datascript.query.limit_context = (function datascript$query$limit_context(context,vars){
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(context,new cljs.core.Keyword(null,"rels","rels",1770187185),cljs.core.keep.cljs$core$IFn$_invoke$arity$2((function (p1__43231_SHARP_){
return datascript.query.limit_rel(p1__43231_SHARP_,vars);
}),new cljs.core.Keyword(null,"rels","rels",1770187185).cljs$core$IFn$_invoke$arity$1(context)));
});
datascript.query.bound_vars = (function datascript$query$bound_vars(context){
return cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentHashSet.EMPTY,cljs.core.mapcat.cljs$core$IFn$_invoke$arity$variadic((function (p1__43235_SHARP_){
return cljs.core.keys(new cljs.core.Keyword(null,"attrs","attrs",-2090668713).cljs$core$IFn$_invoke$arity$1(p1__43235_SHARP_));
}),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"rels","rels",1770187185).cljs$core$IFn$_invoke$arity$1(context)], 0)));
});
datascript.query.check_bound = (function datascript$query$check_bound(bound,vars,form){
if(clojure.set.subset_QMARK_(vars,bound)){
return null;
} else {
var missing = clojure.set.difference.cljs$core$IFn$_invoke$arity$2(cljs.core.set(vars),bound);
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2((""+"Insufficient bindings: "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([missing], 0)))+" not bound in "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([form], 0)))),new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"error","error",-978969032),new cljs.core.Keyword("query","where","query/where",-1935159429),new cljs.core.Keyword(null,"form","form",-1624062471),form,new cljs.core.Keyword(null,"vars","vars",-2046957217),missing], null));
}
});
datascript.query.check_free_same = (function datascript$query$check_free_same(bound,branches,form){
var free = cljs.core.mapv.cljs$core$IFn$_invoke$arity$2((function (p1__43245_SHARP_){
return clojure.set.difference.cljs$core$IFn$_invoke$arity$2(datascript.query.collect_vars(p1__43245_SHARP_),bound);
}),branches);
if(cljs.core.truth_(cljs.core.apply.cljs$core$IFn$_invoke$arity$2(cljs.core._EQ_,free))){
return null;
} else {
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2((""+"All clauses in 'or' must use same set of free vars, had "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([free], 0)))+" in "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([form], 0)))),new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"error","error",-978969032),new cljs.core.Keyword("query","where","query/where",-1935159429),new cljs.core.Keyword(null,"form","form",-1624062471),form,new cljs.core.Keyword(null,"vars","vars",-2046957217),free], null));
}
});
datascript.query.check_free_subset = (function datascript$query$check_free_subset(bound,vars,branches){
var free = cljs.core.set(cljs.core.remove.cljs$core$IFn$_invoke$arity$2(bound,vars));
var seq__43254 = cljs.core.seq(branches);
var chunk__43255 = null;
var count__43256 = (0);
var i__43257 = (0);
while(true){
if((i__43257 < count__43256)){
var branch = chunk__43255.cljs$core$IIndexed$_nth$arity$2(null,i__43257);
var temp__5808__auto___43772 = cljs.core.not_empty(clojure.set.difference.cljs$core$IFn$_invoke$arity$2(free,datascript.query.collect_vars(branch)));
if((temp__5808__auto___43772 == null)){
} else {
var missing_43774 = temp__5808__auto___43772;
cljs.core.prn.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([branch,bound,vars,free], 0));

throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2((""+"All clauses in 'or' must use same set of free vars, had "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([missing_43774], 0)))+" not bound in "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([branch], 0)))),new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"error","error",-978969032),new cljs.core.Keyword("query","where","query/where",-1935159429),new cljs.core.Keyword(null,"form","form",-1624062471),branch,new cljs.core.Keyword(null,"vars","vars",-2046957217),missing_43774], null));
}


var G__43775 = seq__43254;
var G__43776 = chunk__43255;
var G__43777 = count__43256;
var G__43778 = (i__43257 + (1));
seq__43254 = G__43775;
chunk__43255 = G__43776;
count__43256 = G__43777;
i__43257 = G__43778;
continue;
} else {
var temp__5804__auto__ = cljs.core.seq(seq__43254);
if(temp__5804__auto__){
var seq__43254__$1 = temp__5804__auto__;
if(cljs.core.chunked_seq_QMARK_(seq__43254__$1)){
var c__5673__auto__ = cljs.core.chunk_first(seq__43254__$1);
var G__43779 = cljs.core.chunk_rest(seq__43254__$1);
var G__43780 = c__5673__auto__;
var G__43781 = cljs.core.count(c__5673__auto__);
var G__43782 = (0);
seq__43254 = G__43779;
chunk__43255 = G__43780;
count__43256 = G__43781;
i__43257 = G__43782;
continue;
} else {
var branch = cljs.core.first(seq__43254__$1);
var temp__5808__auto___43783 = cljs.core.not_empty(clojure.set.difference.cljs$core$IFn$_invoke$arity$2(free,datascript.query.collect_vars(branch)));
if((temp__5808__auto___43783 == null)){
} else {
var missing_43785 = temp__5808__auto___43783;
cljs.core.prn.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([branch,bound,vars,free], 0));

throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2((""+"All clauses in 'or' must use same set of free vars, had "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([missing_43785], 0)))+" not bound in "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([branch], 0)))),new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"error","error",-978969032),new cljs.core.Keyword("query","where","query/where",-1935159429),new cljs.core.Keyword(null,"form","form",-1624062471),branch,new cljs.core.Keyword(null,"vars","vars",-2046957217),missing_43785], null));
}


var G__43787 = cljs.core.next(seq__43254__$1);
var G__43788 = null;
var G__43789 = (0);
var G__43790 = (0);
seq__43254 = G__43787;
chunk__43255 = G__43788;
count__43256 = G__43789;
i__43257 = G__43790;
continue;
}
} else {
return null;
}
}
break;
}
});
datascript.query._resolve_clause = (function datascript$query$_resolve_clause(var_args){
var G__43342 = arguments.length;
switch (G__43342) {
case 2:
return datascript.query._resolve_clause.cljs$core$IFn$_invoke$arity$2((arguments[(0)]),(arguments[(1)]));

break;
case 3:
return datascript.query._resolve_clause.cljs$core$IFn$_invoke$arity$3((arguments[(0)]),(arguments[(1)]),(arguments[(2)]));

break;
default:
throw (new Error(["Invalid arity: ",arguments.length].join("")));

}
});

(datascript.query._resolve_clause.cljs$core$IFn$_invoke$arity$2 = (function (context,clause){
return datascript.query._resolve_clause.cljs$core$IFn$_invoke$arity$3(context,clause,clause);
}));

(datascript.query._resolve_clause.cljs$core$IFn$_invoke$arity$3 = (function (context,clause,orig_clause){
while(true){
var pred__43349 = datascript.query.looks_like_QMARK_;
var expr__43350 = clause;
if(cljs.core.truth_((function (){var G__43353 = new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [cljs.core.symbol_QMARK_,new cljs.core.Symbol(null,"*","*",345799209,null)], null)], null);
var G__43354 = expr__43350;
return (pred__43349.cljs$core$IFn$_invoke$arity$2 ? pred__43349.cljs$core$IFn$_invoke$arity$2(G__43353,G__43354) : pred__43349.call(null,G__43353,G__43354));
})())){
datascript.query.check_bound(datascript.query.bound_vars(context),cljs.core.filter.cljs$core$IFn$_invoke$arity$2(datascript.query.free_var_QMARK_,cljs.core.nfirst(clause)),clause);

return datascript.query.filter_by_pred(context,clause);
} else {
if(cljs.core.truth_((function (){var G__43356 = new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [cljs.core.symbol_QMARK_,new cljs.core.Symbol(null,"*","*",345799209,null)], null),new cljs.core.Symbol(null,"_","_",-1201019570,null)], null);
var G__43357 = expr__43350;
return (pred__43349.cljs$core$IFn$_invoke$arity$2 ? pred__43349.cljs$core$IFn$_invoke$arity$2(G__43356,G__43357) : pred__43349.call(null,G__43356,G__43357));
})())){
datascript.query.check_bound(datascript.query.bound_vars(context),cljs.core.filter.cljs$core$IFn$_invoke$arity$2(datascript.query.free_var_QMARK_,cljs.core.nfirst(clause)),clause);

return datascript.query.bind_by_fn(context,clause);
} else {
if(cljs.core.truth_((function (){var G__43364 = new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [datascript.query.source_QMARK_,new cljs.core.Symbol(null,"*","*",345799209,null)], null);
var G__43365 = expr__43350;
return (pred__43349.cljs$core$IFn$_invoke$arity$2 ? pred__43349.cljs$core$IFn$_invoke$arity$2(G__43364,G__43365) : pred__43349.call(null,G__43364,G__43365));
})())){
var vec__43366 = clause;
var seq__43367 = cljs.core.seq(vec__43366);
var first__43368 = cljs.core.first(seq__43367);
var seq__43367__$1 = cljs.core.next(seq__43367);
var source_sym = first__43368;
var rest = seq__43367__$1;
var _STAR_implicit_source_STAR__orig_val__43371 = datascript.query._STAR_implicit_source_STAR_;
var _STAR_implicit_source_STAR__temp_val__43372 = cljs.core.get.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"sources","sources",-321166424).cljs$core$IFn$_invoke$arity$1(context),source_sym);
(datascript.query._STAR_implicit_source_STAR_ = _STAR_implicit_source_STAR__temp_val__43372);

try{return datascript.query._resolve_clause.cljs$core$IFn$_invoke$arity$3(context,rest,clause);
}finally {(datascript.query._STAR_implicit_source_STAR_ = _STAR_implicit_source_STAR__orig_val__43371);
}} else {
if(cljs.core.truth_((function (){var G__43374 = new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"or","or",1876275696,null),new cljs.core.Symbol(null,"*","*",345799209,null)], null);
var G__43375 = expr__43350;
return (pred__43349.cljs$core$IFn$_invoke$arity$2 ? pred__43349.cljs$core$IFn$_invoke$arity$2(G__43374,G__43375) : pred__43349.call(null,G__43374,G__43375));
})())){
var vec__43376 = clause;
var seq__43377 = cljs.core.seq(vec__43376);
var first__43378 = cljs.core.first(seq__43377);
var seq__43377__$1 = cljs.core.next(seq__43377);
var _ = first__43378;
var branches = seq__43377__$1;
var ___$1 = datascript.query.check_free_same(datascript.query.bound_vars(context),branches,clause);
var contexts = cljs.core.map.cljs$core$IFn$_invoke$arity$2(((function (context,clause,orig_clause,vec__43376,seq__43377,first__43378,seq__43377__$1,_,branches,___$1,pred__43349,expr__43350){
return (function (p1__43335_SHARP_){
return (datascript.query.resolve_clause.cljs$core$IFn$_invoke$arity$2 ? datascript.query.resolve_clause.cljs$core$IFn$_invoke$arity$2(context,p1__43335_SHARP_) : datascript.query.resolve_clause.call(null,context,p1__43335_SHARP_));
});})(context,clause,orig_clause,vec__43376,seq__43377,first__43378,seq__43377__$1,_,branches,___$1,pred__43349,expr__43350))
,branches);
var rels = cljs.core.map.cljs$core$IFn$_invoke$arity$2(((function (context,clause,orig_clause,vec__43376,seq__43377,first__43378,seq__43377__$1,_,branches,___$1,contexts,pred__43349,expr__43350){
return (function (p1__43336_SHARP_){
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$2(datascript.query.hash_join,new cljs.core.Keyword(null,"rels","rels",1770187185).cljs$core$IFn$_invoke$arity$1(p1__43336_SHARP_));
});})(context,clause,orig_clause,vec__43376,seq__43377,first__43378,seq__43377__$1,_,branches,___$1,contexts,pred__43349,expr__43350))
,contexts);
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(cljs.core.first(contexts),new cljs.core.Keyword(null,"rels","rels",1770187185),new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [cljs.core.reduce.cljs$core$IFn$_invoke$arity$2(datascript.query.sum_rel,rels)], null));
} else {
if(cljs.core.truth_((function (){var G__43383 = new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"or-join","or-join",591375469,null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"*","*",345799209,null)], null),new cljs.core.Symbol(null,"*","*",345799209,null)], null),new cljs.core.Symbol(null,"*","*",345799209,null)], null);
var G__43384 = expr__43350;
return (pred__43349.cljs$core$IFn$_invoke$arity$2 ? pred__43349.cljs$core$IFn$_invoke$arity$2(G__43383,G__43384) : pred__43349.call(null,G__43383,G__43384));
})())){
var vec__43387 = clause;
var seq__43388 = cljs.core.seq(vec__43387);
var first__43389 = cljs.core.first(seq__43388);
var seq__43388__$1 = cljs.core.next(seq__43388);
var _ = first__43389;
var first__43389__$1 = cljs.core.first(seq__43388__$1);
var seq__43388__$2 = cljs.core.next(seq__43388__$1);
var vec__43390 = first__43389__$1;
var seq__43391 = cljs.core.seq(vec__43390);
var first__43392 = cljs.core.first(seq__43391);
var seq__43391__$1 = cljs.core.next(seq__43391);
var req_vars = first__43392;
var vars = seq__43391__$1;
var branches = seq__43388__$2;
var bound = datascript.query.bound_vars(context);
datascript.query.check_bound(bound,req_vars,orig_clause);

datascript.query.check_free_subset(bound,vars,branches);

var G__43797 = context;
var G__43798 = cljs.core.list_STAR_.cljs$core$IFn$_invoke$arity$3(new cljs.core.Symbol(null,"or-join","or-join",591375469,null),cljs.core.concat.cljs$core$IFn$_invoke$arity$2(req_vars,vars),branches);
var G__43799 = clause;
context = G__43797;
clause = G__43798;
orig_clause = G__43799;
continue;
} else {
if(cljs.core.truth_((function (){var G__43396 = new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"or-join","or-join",591375469,null),new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"*","*",345799209,null)], null),new cljs.core.Symbol(null,"*","*",345799209,null)], null);
var G__43397 = expr__43350;
return (pred__43349.cljs$core$IFn$_invoke$arity$2 ? pred__43349.cljs$core$IFn$_invoke$arity$2(G__43396,G__43397) : pred__43349.call(null,G__43396,G__43397));
})())){
var vec__43399 = clause;
var seq__43400 = cljs.core.seq(vec__43399);
var first__43401 = cljs.core.first(seq__43400);
var seq__43400__$1 = cljs.core.next(seq__43400);
var _ = first__43401;
var first__43401__$1 = cljs.core.first(seq__43400__$1);
var seq__43400__$2 = cljs.core.next(seq__43400__$1);
var vars = first__43401__$1;
var branches = seq__43400__$2;
var vars__$1 = cljs.core.set(vars);
var ___$1 = datascript.query.check_free_subset(datascript.query.bound_vars(context),vars__$1,branches);
var join_context = datascript.query.limit_context(context,vars__$1);
var contexts = cljs.core.map.cljs$core$IFn$_invoke$arity$2(((function (context,clause,orig_clause,vec__43399,seq__43400,first__43401,seq__43400__$1,_,first__43401__$1,seq__43400__$2,vars,branches,vars__$1,___$1,join_context,pred__43349,expr__43350){
return (function (p1__43337_SHARP_){
return datascript.query.limit_context((datascript.query.resolve_clause.cljs$core$IFn$_invoke$arity$2 ? datascript.query.resolve_clause.cljs$core$IFn$_invoke$arity$2(join_context,p1__43337_SHARP_) : datascript.query.resolve_clause.call(null,join_context,p1__43337_SHARP_)),vars__$1);
});})(context,clause,orig_clause,vec__43399,seq__43400,first__43401,seq__43400__$1,_,first__43401__$1,seq__43400__$2,vars,branches,vars__$1,___$1,join_context,pred__43349,expr__43350))
,branches);
var rels = cljs.core.map.cljs$core$IFn$_invoke$arity$2(((function (context,clause,orig_clause,vec__43399,seq__43400,first__43401,seq__43400__$1,_,first__43401__$1,seq__43400__$2,vars,branches,vars__$1,___$1,join_context,contexts,pred__43349,expr__43350){
return (function (p1__43338_SHARP_){
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$2(datascript.query.hash_join,new cljs.core.Keyword(null,"rels","rels",1770187185).cljs$core$IFn$_invoke$arity$1(p1__43338_SHARP_));
});})(context,clause,orig_clause,vec__43399,seq__43400,first__43401,seq__43400__$1,_,first__43401__$1,seq__43400__$2,vars,branches,vars__$1,___$1,join_context,contexts,pred__43349,expr__43350))
,contexts);
var sum_rel = cljs.core.reduce.cljs$core$IFn$_invoke$arity$2(datascript.query.sum_rel,rels);
return cljs.core.update.cljs$core$IFn$_invoke$arity$4(context,new cljs.core.Keyword(null,"rels","rels",1770187185),datascript.query.collapse_rels,sum_rel);
} else {
if(cljs.core.truth_((function (){var G__43410 = new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"and","and",668631710,null),new cljs.core.Symbol(null,"*","*",345799209,null)], null);
var G__43411 = expr__43350;
return (pred__43349.cljs$core$IFn$_invoke$arity$2 ? pred__43349.cljs$core$IFn$_invoke$arity$2(G__43410,G__43411) : pred__43349.call(null,G__43410,G__43411));
})())){
var vec__43413 = clause;
var seq__43414 = cljs.core.seq(vec__43413);
var first__43415 = cljs.core.first(seq__43414);
var seq__43414__$1 = cljs.core.next(seq__43414);
var _ = first__43415;
var clauses = seq__43414__$1;
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(datascript.query.resolve_clause,context,clauses);
} else {
if(cljs.core.truth_((function (){var G__43417 = new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"not","not",1044554643,null),new cljs.core.Symbol(null,"*","*",345799209,null)], null);
var G__43418 = expr__43350;
return (pred__43349.cljs$core$IFn$_invoke$arity$2 ? pred__43349.cljs$core$IFn$_invoke$arity$2(G__43417,G__43418) : pred__43349.call(null,G__43417,G__43418));
})())){
var vec__43419 = clause;
var seq__43420 = cljs.core.seq(vec__43419);
var first__43421 = cljs.core.first(seq__43420);
var seq__43420__$1 = cljs.core.next(seq__43420);
var _ = first__43421;
var clauses = seq__43420__$1;
var bound = datascript.query.bound_vars(context);
var negation_vars = datascript.query.collect_vars(clauses);
var ___$1 = ((cljs.core.empty_QMARK_(clojure.set.intersection.cljs$core$IFn$_invoke$arity$2(bound,negation_vars)))?(function(){throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2((""+"Insufficient bindings: none of "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([negation_vars], 0)))+" is bound in "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([orig_clause], 0)))),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"error","error",-978969032),new cljs.core.Keyword("query","where","query/where",-1935159429),new cljs.core.Keyword(null,"form","form",-1624062471),orig_clause], null))})():null);
var context_SINGLEQUOTE_ = cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(context,new cljs.core.Keyword(null,"rels","rels",1770187185),new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [cljs.core.reduce.cljs$core$IFn$_invoke$arity$2(datascript.query.hash_join,new cljs.core.Keyword(null,"rels","rels",1770187185).cljs$core$IFn$_invoke$arity$1(context))], null));
var negation_context = cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(datascript.query.resolve_clause,context_SINGLEQUOTE_,clauses);
var negation = datascript.query.subtract_rel(datascript.util.single(new cljs.core.Keyword(null,"rels","rels",1770187185).cljs$core$IFn$_invoke$arity$1(context_SINGLEQUOTE_)),cljs.core.reduce.cljs$core$IFn$_invoke$arity$2(datascript.query.hash_join,new cljs.core.Keyword(null,"rels","rels",1770187185).cljs$core$IFn$_invoke$arity$1(negation_context)));
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(context_SINGLEQUOTE_,new cljs.core.Keyword(null,"rels","rels",1770187185),new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [negation], null));
} else {
if(cljs.core.truth_((function (){var G__43423 = new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"not-join","not-join",-645515756,null),new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"*","*",345799209,null)], null),new cljs.core.Symbol(null,"*","*",345799209,null)], null);
var G__43424 = expr__43350;
return (pred__43349.cljs$core$IFn$_invoke$arity$2 ? pred__43349.cljs$core$IFn$_invoke$arity$2(G__43423,G__43424) : pred__43349.call(null,G__43423,G__43424));
})())){
var vec__43427 = clause;
var seq__43428 = cljs.core.seq(vec__43427);
var first__43429 = cljs.core.first(seq__43428);
var seq__43428__$1 = cljs.core.next(seq__43428);
var _ = first__43429;
var first__43429__$1 = cljs.core.first(seq__43428__$1);
var seq__43428__$2 = cljs.core.next(seq__43428__$1);
var vars = first__43429__$1;
var clauses = seq__43428__$2;
var bound = datascript.query.bound_vars(context);
var ___$1 = datascript.query.check_bound(bound,vars,orig_clause);
var context_SINGLEQUOTE_ = cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(context,new cljs.core.Keyword(null,"rels","rels",1770187185),new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [cljs.core.reduce.cljs$core$IFn$_invoke$arity$2(datascript.query.hash_join,new cljs.core.Keyword(null,"rels","rels",1770187185).cljs$core$IFn$_invoke$arity$1(context))], null));
var join_context = datascript.query.limit_context(context_SINGLEQUOTE_,vars);
var negation_context = datascript.query.limit_context(cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(datascript.query.resolve_clause,join_context,clauses),vars);
var negation = datascript.query.subtract_rel(datascript.util.single(new cljs.core.Keyword(null,"rels","rels",1770187185).cljs$core$IFn$_invoke$arity$1(context_SINGLEQUOTE_)),cljs.core.reduce.cljs$core$IFn$_invoke$arity$2(datascript.query.hash_join,new cljs.core.Keyword(null,"rels","rels",1770187185).cljs$core$IFn$_invoke$arity$1(negation_context)));
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(context_SINGLEQUOTE_,new cljs.core.Keyword(null,"rels","rels",1770187185),new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [negation], null));
} else {
if(cljs.core.truth_((function (){var G__43433 = new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"*","*",345799209,null)], null);
var G__43434 = expr__43350;
return (pred__43349.cljs$core$IFn$_invoke$arity$2 ? pred__43349.cljs$core$IFn$_invoke$arity$2(G__43433,G__43434) : pred__43349.call(null,G__43433,G__43434));
})())){
var source = datascript.query._STAR_implicit_source_STAR_;
var pattern_SINGLEQUOTE_ = datascript.query.resolve_pattern_lookup_refs(source,clause);
var relation = datascript.query.lookup_pattern(context,source,pattern_SINGLEQUOTE_);
var _STAR_lookup_attrs_STAR__orig_val__43435 = datascript.query._STAR_lookup_attrs_STAR_;
var _STAR_lookup_attrs_STAR__temp_val__43436 = (((((!((source == null))))?((((false) || ((cljs.core.PROTOCOL_SENTINEL === source.datascript$db$IDB$))))?true:(((!source.cljs$lang$protocol_mask$partition$))?cljs.core.native_satisfies_QMARK_(datascript.db.IDB,source):false)):cljs.core.native_satisfies_QMARK_(datascript.db.IDB,source)))?datascript.query.dynamic_lookup_attrs(source,pattern_SINGLEQUOTE_):datascript.query._STAR_lookup_attrs_STAR_);
(datascript.query._STAR_lookup_attrs_STAR_ = _STAR_lookup_attrs_STAR__temp_val__43436);

try{return cljs.core.update.cljs$core$IFn$_invoke$arity$4(context,new cljs.core.Keyword(null,"rels","rels",1770187185),datascript.query.collapse_rels,relation);
}finally {(datascript.query._STAR_lookup_attrs_STAR_ = _STAR_lookup_attrs_STAR__orig_val__43435);
}} else {
throw (new Error((""+"No matching clause: "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(expr__43350))));
}
}
}
}
}
}
}
}
}
}
break;
}
}));

(datascript.query._resolve_clause.cljs$lang$maxFixedArity = 3);

datascript.query.resolve_clause = (function datascript$query$resolve_clause(context,clause){
if(cljs.core.truth_(cljs.core.some(cljs.core.comp.cljs$core$IFn$_invoke$arity$2(cljs.core.empty_QMARK_,new cljs.core.Keyword(null,"tuples","tuples",-676032639)),new cljs.core.Keyword(null,"rels","rels",1770187185).cljs$core$IFn$_invoke$arity$1(context)))){
return context;
} else {
if(datascript.query.rule_QMARK_(context,clause)){
if(datascript.query.source_QMARK_(cljs.core.first(clause))){
var _STAR_implicit_source_STAR__orig_val__43442 = datascript.query._STAR_implicit_source_STAR_;
var _STAR_implicit_source_STAR__temp_val__43443 = cljs.core.get.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"sources","sources",-321166424).cljs$core$IFn$_invoke$arity$1(context),cljs.core.first(clause));
(datascript.query._STAR_implicit_source_STAR_ = _STAR_implicit_source_STAR__temp_val__43443);

try{var G__43444 = context;
var G__43445 = cljs.core.next(clause);
return (datascript.query.resolve_clause.cljs$core$IFn$_invoke$arity$2 ? datascript.query.resolve_clause.cljs$core$IFn$_invoke$arity$2(G__43444,G__43445) : datascript.query.resolve_clause.call(null,G__43444,G__43445));
}finally {(datascript.query._STAR_implicit_source_STAR_ = _STAR_implicit_source_STAR__orig_val__43442);
}} else {
return cljs.core.update.cljs$core$IFn$_invoke$arity$4(context,new cljs.core.Keyword(null,"rels","rels",1770187185),datascript.query.collapse_rels,datascript.query.solve_rule(context,clause));
}
} else {
return datascript.query._resolve_clause.cljs$core$IFn$_invoke$arity$2(context,clause);
}
}
});
datascript.query._q = (function datascript$query$_q(context,clauses){
var _STAR_implicit_source_STAR__orig_val__43449 = datascript.query._STAR_implicit_source_STAR_;
var _STAR_implicit_source_STAR__temp_val__43450 = cljs.core.get.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"sources","sources",-321166424).cljs$core$IFn$_invoke$arity$1(context),new cljs.core.Symbol(null,"$","$",-1580747756,null));
(datascript.query._STAR_implicit_source_STAR_ = _STAR_implicit_source_STAR__temp_val__43450);

try{return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(datascript.query.resolve_clause,context,clauses);
}finally {(datascript.query._STAR_implicit_source_STAR_ = _STAR_implicit_source_STAR__orig_val__43449);
}});
datascript.query._collect_tuples = (function datascript$query$_collect_tuples(acc,rel,len,copy_map){
return cljs.core.__GT_Eduction(cljs.core.comp.cljs$core$IFn$_invoke$arity$2(cljs.core.map.cljs$core$IFn$_invoke$arity$1((function (t1){
return cljs.core.__GT_Eduction(cljs.core.map.cljs$core$IFn$_invoke$arity$1((function (t2){
var res = cljs.core.aclone(t1);
var n__5741__auto___43813 = len;
var i_43814 = (0);
while(true){
if((i_43814 < n__5741__auto___43813)){
var temp__5808__auto___43815 = (copy_map[i_43814]);
if((temp__5808__auto___43815 == null)){
} else {
var idx_43816 = temp__5808__auto___43815;
(res[i_43814] = (t2[idx_43816]));
}

var G__43817 = (i_43814 + (1));
i_43814 = G__43817;
continue;
} else {
}
break;
}

return res;
})),new cljs.core.Keyword(null,"tuples","tuples",-676032639).cljs$core$IFn$_invoke$arity$1(rel));
})),cljs.core.cat),acc);
});
datascript.query._collect = (function datascript$query$_collect(var_args){
var G__43464 = arguments.length;
switch (G__43464) {
case 2:
return datascript.query._collect.cljs$core$IFn$_invoke$arity$2((arguments[(0)]),(arguments[(1)]));

break;
case 3:
return datascript.query._collect.cljs$core$IFn$_invoke$arity$3((arguments[(0)]),(arguments[(1)]),(arguments[(2)]));

break;
default:
throw (new Error(["Invalid arity: ",arguments.length].join("")));

}
});

(datascript.query._collect.cljs$core$IFn$_invoke$arity$2 = (function (context,symbols){
var rels = new cljs.core.Keyword(null,"rels","rels",1770187185).cljs$core$IFn$_invoke$arity$1(context);
return datascript.query._collect.cljs$core$IFn$_invoke$arity$3(new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [me.tonsky.persistent_sorted_set.arrays.make_array(cljs.core.count(symbols))], null),rels,symbols);
}));

(datascript.query._collect.cljs$core$IFn$_invoke$arity$3 = (function (acc,rels,symbols){
while(true){
var rel = cljs.core.first(rels);
if((rel == null)){
return acc;
} else {
if(cljs.core.empty_QMARK_(new cljs.core.Keyword(null,"tuples","tuples",-676032639).cljs$core$IFn$_invoke$arity$1(rel))){
return cljs.core.PersistentVector.EMPTY;
} else {
var keep_attrs = cljs.core.select_keys(new cljs.core.Keyword(null,"attrs","attrs",-2090668713).cljs$core$IFn$_invoke$arity$1(rel),symbols);
if(cljs.core.empty_QMARK_(keep_attrs)){
var G__43819 = acc;
var G__43820 = cljs.core.next(rels);
var G__43821 = symbols;
acc = G__43819;
rels = G__43820;
symbols = G__43821;
continue;
} else {
var copy_map = cljs.core.to_array(cljs.core.map.cljs$core$IFn$_invoke$arity$2(((function (acc,rels,symbols,keep_attrs,rel){
return (function (p1__43462_SHARP_){
return cljs.core.get.cljs$core$IFn$_invoke$arity$2(keep_attrs,p1__43462_SHARP_);
});})(acc,rels,symbols,keep_attrs,rel))
,symbols));
var len = cljs.core.count(symbols);
var G__43822 = datascript.query._collect_tuples(acc,rel,len,copy_map);
var G__43823 = cljs.core.next(rels);
var G__43824 = symbols;
acc = G__43822;
rels = G__43823;
symbols = G__43824;
continue;

}
}
}
break;
}
}));

(datascript.query._collect.cljs$lang$maxFixedArity = 3);

datascript.query.collect = (function datascript$query$collect(context,symbols){
return cljs.core.into.cljs$core$IFn$_invoke$arity$3(cljs.core.PersistentHashSet.EMPTY,cljs.core.map.cljs$core$IFn$_invoke$arity$1(cljs.core.vec),datascript.query._collect.cljs$core$IFn$_invoke$arity$2(context,symbols));
});

/**
 * @interface
 */
datascript.query.IContextResolve = function(){};

var datascript$query$IContextResolve$_context_resolve$dyn_43825 = (function (var$,context){
var x__5498__auto__ = (((var$ == null))?null:var$);
var m__5499__auto__ = (datascript.query._context_resolve[goog.typeOf(x__5498__auto__)]);
if((!((m__5499__auto__ == null)))){
return (m__5499__auto__.cljs$core$IFn$_invoke$arity$2 ? m__5499__auto__.cljs$core$IFn$_invoke$arity$2(var$,context) : m__5499__auto__.call(null,var$,context));
} else {
var m__5497__auto__ = (datascript.query._context_resolve["_"]);
if((!((m__5497__auto__ == null)))){
return (m__5497__auto__.cljs$core$IFn$_invoke$arity$2 ? m__5497__auto__.cljs$core$IFn$_invoke$arity$2(var$,context) : m__5497__auto__.call(null,var$,context));
} else {
throw cljs.core.missing_protocol("IContextResolve.-context-resolve",var$);
}
}
});
datascript.query._context_resolve = (function datascript$query$_context_resolve(var$,context){
if((((!((var$ == null)))) && ((!((var$.datascript$query$IContextResolve$_context_resolve$arity$2 == null)))))){
return var$.datascript$query$IContextResolve$_context_resolve$arity$2(var$,context);
} else {
return datascript$query$IContextResolve$_context_resolve$dyn_43825(var$,context);
}
});

(datascript.parser.Variable.prototype.datascript$query$IContextResolve$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.parser.Variable.prototype.datascript$query$IContextResolve$_context_resolve$arity$2 = (function (var$,context){
var var$__$1 = this;
return datascript.query.context_resolve_val(context,var$__$1.symbol);
}));

(datascript.parser.SrcVar.prototype.datascript$query$IContextResolve$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.parser.SrcVar.prototype.datascript$query$IContextResolve$_context_resolve$arity$2 = (function (var$,context){
var var$__$1 = this;
return cljs.core.get_in.cljs$core$IFn$_invoke$arity$2(context,new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"sources","sources",-321166424),var$__$1.symbol], null));
}));

(datascript.parser.PlainSymbol.prototype.datascript$query$IContextResolve$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.parser.PlainSymbol.prototype.datascript$query$IContextResolve$_context_resolve$arity$2 = (function (var$,_){
var var$__$1 = this;
var or__5142__auto__ = cljs.core.get.cljs$core$IFn$_invoke$arity$2(datascript.built_ins.aggregates,var$__$1.symbol);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return datascript.query.resolve_sym(var$__$1.symbol);
}
}));

(datascript.parser.Constant.prototype.datascript$query$IContextResolve$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.parser.Constant.prototype.datascript$query$IContextResolve$_context_resolve$arity$2 = (function (var$,_){
var var$__$1 = this;
return var$__$1.value;
}));
datascript.query._aggregate = (function datascript$query$_aggregate(find_elements,context,tuples){
return cljs.core.mapv.cljs$core$IFn$_invoke$arity$4((function (element,fixed_value,i){
if(datascript.parser.aggregate_QMARK_(element)){
var f = datascript.query._context_resolve(new cljs.core.Keyword(null,"fn","fn",-1175266204).cljs$core$IFn$_invoke$arity$1(element),context);
var args = cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (p1__43493_SHARP_){
return datascript.query._context_resolve(p1__43493_SHARP_,context);
}),cljs.core.butlast(new cljs.core.Keyword(null,"args","args",1315556576).cljs$core$IFn$_invoke$arity$1(element)));
var vals = cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (p1__43494_SHARP_){
return cljs.core.nth.cljs$core$IFn$_invoke$arity$2(p1__43494_SHARP_,i);
}),tuples);
return cljs.core.apply.cljs$core$IFn$_invoke$arity$2(f,cljs.core.concat.cljs$core$IFn$_invoke$arity$2(args,new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [vals], null)));
} else {
return fixed_value;
}
}),find_elements,cljs.core.first(tuples),cljs.core.range.cljs$core$IFn$_invoke$arity$0());
});
datascript.query.idxs_of = (function datascript$query$idxs_of(pred,coll){
return cljs.core.remove.cljs$core$IFn$_invoke$arity$2(cljs.core.nil_QMARK_,cljs.core.map.cljs$core$IFn$_invoke$arity$3((function (p1__43522_SHARP_,p2__43529_SHARP_){
if(cljs.core.truth_((pred.cljs$core$IFn$_invoke$arity$1 ? pred.cljs$core$IFn$_invoke$arity$1(p1__43522_SHARP_) : pred.call(null,p1__43522_SHARP_)))){
return p2__43529_SHARP_;
} else {
return null;
}
}),coll,cljs.core.range.cljs$core$IFn$_invoke$arity$0()));
});
datascript.query.aggregate = (function datascript$query$aggregate(find_elements,context,resultset){
var group_idxs = datascript.query.idxs_of(cljs.core.complement(datascript.parser.aggregate_QMARK_),find_elements);
var group_fn = (function (tuple){
return cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (p1__43540_SHARP_){
return cljs.core.nth.cljs$core$IFn$_invoke$arity$2(tuple,p1__43540_SHARP_);
}),group_idxs);
});
var grouped = cljs.core.group_by(group_fn,resultset);
var iter__5628__auto__ = (function datascript$query$aggregate_$_iter__43546(s__43547){
return (new cljs.core.LazySeq(null,(function (){
var s__43547__$1 = s__43547;
while(true){
var temp__5804__auto__ = cljs.core.seq(s__43547__$1);
if(temp__5804__auto__){
var s__43547__$2 = temp__5804__auto__;
if(cljs.core.chunked_seq_QMARK_(s__43547__$2)){
var c__5626__auto__ = cljs.core.chunk_first(s__43547__$2);
var size__5627__auto__ = cljs.core.count(c__5626__auto__);
var b__43549 = cljs.core.chunk_buffer(size__5627__auto__);
if((function (){var i__43548 = (0);
while(true){
if((i__43548 < size__5627__auto__)){
var vec__43550 = cljs.core._nth(c__5626__auto__,i__43548);
var _ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__43550,(0),null);
var tuples = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__43550,(1),null);
cljs.core.chunk_append(b__43549,datascript.query._aggregate(find_elements,context,tuples));

var G__43826 = (i__43548 + (1));
i__43548 = G__43826;
continue;
} else {
return true;
}
break;
}
})()){
return cljs.core.chunk_cons(cljs.core.chunk(b__43549),datascript$query$aggregate_$_iter__43546(cljs.core.chunk_rest(s__43547__$2)));
} else {
return cljs.core.chunk_cons(cljs.core.chunk(b__43549),null);
}
} else {
var vec__43558 = cljs.core.first(s__43547__$2);
var _ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__43558,(0),null);
var tuples = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__43558,(1),null);
return cljs.core.cons(datascript.query._aggregate(find_elements,context,tuples),datascript$query$aggregate_$_iter__43546(cljs.core.rest(s__43547__$2)));
}
} else {
return null;
}
break;
}
}),null,null));
});
return iter__5628__auto__(grouped);
});
datascript.query.map_STAR_ = (function datascript$query$map_STAR_(f,xs){
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (p1__43566_SHARP_,p2__43567_SHARP_){
return cljs.core.conj.cljs$core$IFn$_invoke$arity$2(p1__43566_SHARP_,(f.cljs$core$IFn$_invoke$arity$1 ? f.cljs$core$IFn$_invoke$arity$1(p2__43567_SHARP_) : f.call(null,p2__43567_SHARP_)));
}),cljs.core.empty(xs),xs);
});
datascript.query.tuples__GT_return_map = (function datascript$query$tuples__GT_return_map(return_map,tuples){
var symbols = new cljs.core.Keyword(null,"symbols","symbols",1211743).cljs$core$IFn$_invoke$arity$1(return_map);
var idxs = cljs.core.range.cljs$core$IFn$_invoke$arity$2((0),cljs.core.count(symbols));
return datascript.query.map_STAR_((function (tuple){
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (m,i){
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(m,cljs.core.nth.cljs$core$IFn$_invoke$arity$2(symbols,i),cljs.core.nth.cljs$core$IFn$_invoke$arity$2(tuple,i));
}),cljs.core.PersistentArrayMap.EMPTY,idxs);
}),tuples);
});

/**
 * @interface
 */
datascript.query.IPostProcess = function(){};

var datascript$query$IPostProcess$_post_process$dyn_43827 = (function (find,return_map,tuples){
var x__5498__auto__ = (((find == null))?null:find);
var m__5499__auto__ = (datascript.query._post_process[goog.typeOf(x__5498__auto__)]);
if((!((m__5499__auto__ == null)))){
return (m__5499__auto__.cljs$core$IFn$_invoke$arity$3 ? m__5499__auto__.cljs$core$IFn$_invoke$arity$3(find,return_map,tuples) : m__5499__auto__.call(null,find,return_map,tuples));
} else {
var m__5497__auto__ = (datascript.query._post_process["_"]);
if((!((m__5497__auto__ == null)))){
return (m__5497__auto__.cljs$core$IFn$_invoke$arity$3 ? m__5497__auto__.cljs$core$IFn$_invoke$arity$3(find,return_map,tuples) : m__5497__auto__.call(null,find,return_map,tuples));
} else {
throw cljs.core.missing_protocol("IPostProcess.-post-process",find);
}
}
});
datascript.query._post_process = (function datascript$query$_post_process(find,return_map,tuples){
if((((!((find == null)))) && ((!((find.datascript$query$IPostProcess$_post_process$arity$3 == null)))))){
return find.datascript$query$IPostProcess$_post_process$arity$3(find,return_map,tuples);
} else {
return datascript$query$IPostProcess$_post_process$dyn_43827(find,return_map,tuples);
}
});

(datascript.parser.FindRel.prototype.datascript$query$IPostProcess$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.parser.FindRel.prototype.datascript$query$IPostProcess$_post_process$arity$3 = (function (_,return_map,tuples){
var ___$1 = this;
if((return_map == null)){
return tuples;
} else {
return datascript.query.tuples__GT_return_map(return_map,tuples);
}
}));

(datascript.parser.FindColl.prototype.datascript$query$IPostProcess$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.parser.FindColl.prototype.datascript$query$IPostProcess$_post_process$arity$3 = (function (_,return_map,tuples){
var ___$1 = this;
return cljs.core.into.cljs$core$IFn$_invoke$arity$3(cljs.core.PersistentVector.EMPTY,cljs.core.map.cljs$core$IFn$_invoke$arity$1(cljs.core.first),tuples);
}));

(datascript.parser.FindScalar.prototype.datascript$query$IPostProcess$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.parser.FindScalar.prototype.datascript$query$IPostProcess$_post_process$arity$3 = (function (_,return_map,tuples){
var ___$1 = this;
return cljs.core.ffirst(tuples);
}));

(datascript.parser.FindTuple.prototype.datascript$query$IPostProcess$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.parser.FindTuple.prototype.datascript$query$IPostProcess$_post_process$arity$3 = (function (_,return_map,tuples){
var ___$1 = this;
if((!((return_map == null)))){
return cljs.core.first(datascript.query.tuples__GT_return_map(return_map,new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [cljs.core.first(tuples)], null)));
} else {
return cljs.core.first(tuples);
}
}));
datascript.query.pull = (function datascript$query$pull(find_elements,context,resultset){
var resolved = (function (){var iter__5628__auto__ = (function datascript$query$pull_$_iter__43585(s__43586){
return (new cljs.core.LazySeq(null,(function (){
var s__43586__$1 = s__43586;
while(true){
var temp__5804__auto__ = cljs.core.seq(s__43586__$1);
if(temp__5804__auto__){
var s__43586__$2 = temp__5804__auto__;
if(cljs.core.chunked_seq_QMARK_(s__43586__$2)){
var c__5626__auto__ = cljs.core.chunk_first(s__43586__$2);
var size__5627__auto__ = cljs.core.count(c__5626__auto__);
var b__43588 = cljs.core.chunk_buffer(size__5627__auto__);
if((function (){var i__43587 = (0);
while(true){
if((i__43587 < size__5627__auto__)){
var find = cljs.core._nth(c__5626__auto__,i__43587);
cljs.core.chunk_append(b__43588,((datascript.parser.pull_QMARK_(find))?(function (){var db = datascript.query._context_resolve(new cljs.core.Keyword(null,"source","source",-433931539).cljs$core$IFn$_invoke$arity$1(find),context);
var pattern = datascript.query._context_resolve(new cljs.core.Keyword(null,"pattern","pattern",242135423).cljs$core$IFn$_invoke$arity$1(find),context);
return datascript.pull_api.parse_opts.cljs$core$IFn$_invoke$arity$2(db,pattern);
})():null));

var G__43828 = (i__43587 + (1));
i__43587 = G__43828;
continue;
} else {
return true;
}
break;
}
})()){
return cljs.core.chunk_cons(cljs.core.chunk(b__43588),datascript$query$pull_$_iter__43585(cljs.core.chunk_rest(s__43586__$2)));
} else {
return cljs.core.chunk_cons(cljs.core.chunk(b__43588),null);
}
} else {
var find = cljs.core.first(s__43586__$2);
return cljs.core.cons(((datascript.parser.pull_QMARK_(find))?(function (){var db = datascript.query._context_resolve(new cljs.core.Keyword(null,"source","source",-433931539).cljs$core$IFn$_invoke$arity$1(find),context);
var pattern = datascript.query._context_resolve(new cljs.core.Keyword(null,"pattern","pattern",242135423).cljs$core$IFn$_invoke$arity$1(find),context);
return datascript.pull_api.parse_opts.cljs$core$IFn$_invoke$arity$2(db,pattern);
})():null),datascript$query$pull_$_iter__43585(cljs.core.rest(s__43586__$2)));
}
} else {
return null;
}
break;
}
}),null,null));
});
return iter__5628__auto__(find_elements);
})();
var iter__5628__auto__ = (function datascript$query$pull_$_iter__43601(s__43602){
return (new cljs.core.LazySeq(null,(function (){
var s__43602__$1 = s__43602;
while(true){
var temp__5804__auto__ = cljs.core.seq(s__43602__$1);
if(temp__5804__auto__){
var s__43602__$2 = temp__5804__auto__;
if(cljs.core.chunked_seq_QMARK_(s__43602__$2)){
var c__5626__auto__ = cljs.core.chunk_first(s__43602__$2);
var size__5627__auto__ = cljs.core.count(c__5626__auto__);
var b__43604 = cljs.core.chunk_buffer(size__5627__auto__);
if((function (){var i__43603 = (0);
while(true){
if((i__43603 < size__5627__auto__)){
var tuple = cljs.core._nth(c__5626__auto__,i__43603);
cljs.core.chunk_append(b__43604,cljs.core.mapv.cljs$core$IFn$_invoke$arity$3(((function (i__43603,tuple,c__5626__auto__,size__5627__auto__,b__43604,s__43602__$2,temp__5804__auto__,resolved){
return (function (parsed_opts,el){
if(cljs.core.truth_(parsed_opts)){
return datascript.pull_api.pull_impl(parsed_opts,el);
} else {
return el;
}
});})(i__43603,tuple,c__5626__auto__,size__5627__auto__,b__43604,s__43602__$2,temp__5804__auto__,resolved))
,resolved,tuple));

var G__43836 = (i__43603 + (1));
i__43603 = G__43836;
continue;
} else {
return true;
}
break;
}
})()){
return cljs.core.chunk_cons(cljs.core.chunk(b__43604),datascript$query$pull_$_iter__43601(cljs.core.chunk_rest(s__43602__$2)));
} else {
return cljs.core.chunk_cons(cljs.core.chunk(b__43604),null);
}
} else {
var tuple = cljs.core.first(s__43602__$2);
return cljs.core.cons(cljs.core.mapv.cljs$core$IFn$_invoke$arity$3(((function (tuple,s__43602__$2,temp__5804__auto__,resolved){
return (function (parsed_opts,el){
if(cljs.core.truth_(parsed_opts)){
return datascript.pull_api.pull_impl(parsed_opts,el);
} else {
return el;
}
});})(tuple,s__43602__$2,temp__5804__auto__,resolved))
,resolved,tuple),datascript$query$pull_$_iter__43601(cljs.core.rest(s__43602__$2)));
}
} else {
return null;
}
break;
}
}),null,null));
});
return iter__5628__auto__(resultset);
});
datascript.query.q = (function datascript$query$q(var_args){
var args__5882__auto__ = [];
var len__5876__auto___43840 = arguments.length;
var i__5877__auto___43841 = (0);
while(true){
if((i__5877__auto___43841 < len__5876__auto___43840)){
args__5882__auto__.push((arguments[i__5877__auto___43841]));

var G__43842 = (i__5877__auto___43841 + (1));
i__5877__auto___43841 = G__43842;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((1) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((1)),(0),null)):null);
return datascript.query.q.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),argseq__5883__auto__);
});

(datascript.query.q.cljs$core$IFn$_invoke$arity$variadic = (function (q,inputs){
var parsed_q = datascript.lru._get(datascript.query._STAR_query_cache_STAR_,q,(function (){
return datascript.parser.parse_query(q);
}));
var find = new cljs.core.Keyword(null,"qfind","qfind",1529332972).cljs$core$IFn$_invoke$arity$1(parsed_q);
var find_elements = datascript.parser.find_elements(find);
var find_vars = datascript.parser.find_vars(find);
var result_arity = cljs.core.count(find_elements);
var with$ = new cljs.core.Keyword(null,"qwith","qwith",-45809392).cljs$core$IFn$_invoke$arity$1(parsed_q);
var all_vars = cljs.core.concat.cljs$core$IFn$_invoke$arity$2(find_vars,cljs.core.map.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"symbol","symbol",-1038572696),with$));
var q__$1 = (function (){var G__43610 = q;
if(cljs.core.sequential_QMARK_(q)){
return datascript.parser.query__GT_map(G__43610);
} else {
return G__43610;
}
})();
var wheres = new cljs.core.Keyword(null,"where","where",-2044795965).cljs$core$IFn$_invoke$arity$1(q__$1);
var context = datascript.query.resolve_ins((new datascript.query.Context(cljs.core.PersistentVector.EMPTY,cljs.core.PersistentArrayMap.EMPTY,cljs.core.PersistentArrayMap.EMPTY,null,null,null)),new cljs.core.Keyword(null,"qin","qin",1372651151).cljs$core$IFn$_invoke$arity$1(parsed_q),inputs);
var resultset = datascript.query.collect(datascript.query._q(context,wheres),all_vars);
var G__43611 = resultset;
var G__43611__$1 = (cljs.core.truth_(new cljs.core.Keyword(null,"with","with",-1536296876).cljs$core$IFn$_invoke$arity$1(q__$1))?cljs.core.mapv.cljs$core$IFn$_invoke$arity$2((function (p1__43605_SHARP_){
return cljs.core.vec(cljs.core.subvec.cljs$core$IFn$_invoke$arity$3(p1__43605_SHARP_,(0),result_arity));
}),G__43611):G__43611);
var G__43611__$2 = (cljs.core.truth_(cljs.core.some(datascript.parser.aggregate_QMARK_,find_elements))?datascript.query.aggregate(find_elements,context,G__43611__$1):G__43611__$1);
var G__43611__$3 = (cljs.core.truth_(cljs.core.some(datascript.parser.pull_QMARK_,find_elements))?datascript.query.pull(find_elements,context,G__43611__$2):G__43611__$2);
return datascript.query._post_process(find,new cljs.core.Keyword(null,"qreturn-map","qreturn-map",1841215994).cljs$core$IFn$_invoke$arity$1(parsed_q),G__43611__$3);

}));

(datascript.query.q.cljs$lang$maxFixedArity = (1));

/** @this {Function} */
(datascript.query.q.cljs$lang$applyTo = (function (seq43606){
var G__43607 = cljs.core.first(seq43606);
var seq43606__$1 = cljs.core.next(seq43606);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__43607,seq43606__$1);
}));


//# sourceMappingURL=datascript.query.js.map
