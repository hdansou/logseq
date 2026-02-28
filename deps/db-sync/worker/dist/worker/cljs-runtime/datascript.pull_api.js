import "./cljs_env.js";
import "./cljs.core.js";
import "./clojure.string.js";
import "./datascript.pull_parser.js";
import "./datascript.db.js";
import "./datascript.lru.js";
import "./datascript.util.js";
import "./me.tonsky.persistent_sorted_set.js";
goog.provide('datascript.pull_api');



datascript.pull_api.first_seq = (function datascript$pull_api$first_seq(xs){
if((xs == null)){
return null;
} else {
return cljs.core._first(xs);
}
});
datascript.pull_api.next_seq = (function datascript$pull_api$next_seq(xs){
if((xs == null)){
return null;
} else {
return cljs.core._next(xs);
}
});
datascript.pull_api.conj_seq = (function datascript$pull_api$conj_seq(xs,x){
if((xs == null)){
return (new cljs.core.List(null,x,null,(1),null));
} else {
return cljs.core._conj(xs,x);
}
});
datascript.pull_api.assoc_some_BANG_ = (function datascript$pull_api$assoc_some_BANG_(m,k,v){
if((v == null)){
return m;
} else {
return cljs.core.assoc_BANG_.cljs$core$IFn$_invoke$arity$3(m,k,v);
}
});
datascript.pull_api.conj_some_BANG_ = (function datascript$pull_api$conj_some_BANG_(xs,v){
if((v == null)){
return xs;
} else {
return cljs.core.conj_BANG_.cljs$core$IFn$_invoke$arity$2(xs,v);
}
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
datascript.pull_api.Context = (function (db,visitor,__meta,__extmap,__hash){
this.db = db;
this.visitor = visitor;
this.__meta = __meta;
this.__extmap = __extmap;
this.__hash = __hash;
this.cljs$lang$protocol_mask$partition0$ = 2230716170;
this.cljs$lang$protocol_mask$partition1$ = 139264;
});
(datascript.pull_api.Context.prototype.cljs$core$ILookup$_lookup$arity$2 = (function (this__5448__auto__,k__5449__auto__){
var self__ = this;
var this__5448__auto____$1 = this;
return this__5448__auto____$1.cljs$core$ILookup$_lookup$arity$3(null,k__5449__auto__,null);
}));

(datascript.pull_api.Context.prototype.cljs$core$ILookup$_lookup$arity$3 = (function (this__5450__auto__,k40522,else__5451__auto__){
var self__ = this;
var this__5450__auto____$1 = this;
var G__40529 = k40522;
var G__40529__$1 = (((G__40529 instanceof cljs.core.Keyword))?G__40529.fqn:null);
switch (G__40529__$1) {
case "db":
return self__.db;

break;
case "visitor":
return self__.visitor;

break;
default:
return cljs.core.get.cljs$core$IFn$_invoke$arity$3(self__.__extmap,k40522,else__5451__auto__);

}
}));

(datascript.pull_api.Context.prototype.cljs$core$IKVReduce$_kv_reduce$arity$3 = (function (this__5468__auto__,f__5469__auto__,init__5470__auto__){
var self__ = this;
var this__5468__auto____$1 = this;
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (ret__5471__auto__,p__40532){
var vec__40533 = p__40532;
var k__5472__auto__ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40533,(0),null);
var v__5473__auto__ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40533,(1),null);
return (f__5469__auto__.cljs$core$IFn$_invoke$arity$3 ? f__5469__auto__.cljs$core$IFn$_invoke$arity$3(ret__5471__auto__,k__5472__auto__,v__5473__auto__) : f__5469__auto__.call(null,ret__5471__auto__,k__5472__auto__,v__5473__auto__));
}),init__5470__auto__,this__5468__auto____$1);
}));

(datascript.pull_api.Context.prototype.cljs$core$IPrintWithWriter$_pr_writer$arity$3 = (function (this__5463__auto__,writer__5464__auto__,opts__5465__auto__){
var self__ = this;
var this__5463__auto____$1 = this;
var pr_pair__5466__auto__ = (function (keyval__5467__auto__){
return cljs.core.pr_sequential_writer(writer__5464__auto__,cljs.core.pr_writer,""," ","",opts__5465__auto__,keyval__5467__auto__);
});
return cljs.core.pr_sequential_writer(writer__5464__auto__,pr_pair__5466__auto__,"#datascript.pull-api.Context{",", ","}",opts__5465__auto__,cljs.core.concat.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"db","db",993250759),self__.db],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"visitor","visitor",-1026865865),self__.visitor],null))], null),self__.__extmap));
}));

(datascript.pull_api.Context.prototype.cljs$core$IIterable$_iterator$arity$1 = (function (G__40521){
var self__ = this;
var G__40521__$1 = this;
return (new cljs.core.RecordIter((0),G__40521__$1,2,new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"db","db",993250759),new cljs.core.Keyword(null,"visitor","visitor",-1026865865)], null),(cljs.core.truth_(self__.__extmap)?cljs.core._iterator(self__.__extmap):cljs.core.nil_iter())));
}));

(datascript.pull_api.Context.prototype.cljs$core$IMeta$_meta$arity$1 = (function (this__5446__auto__){
var self__ = this;
var this__5446__auto____$1 = this;
return self__.__meta;
}));

(datascript.pull_api.Context.prototype.cljs$core$ICloneable$_clone$arity$1 = (function (this__5443__auto__){
var self__ = this;
var this__5443__auto____$1 = this;
return (new datascript.pull_api.Context(self__.db,self__.visitor,self__.__meta,self__.__extmap,self__.__hash));
}));

(datascript.pull_api.Context.prototype.cljs$core$ICounted$_count$arity$1 = (function (this__5452__auto__){
var self__ = this;
var this__5452__auto____$1 = this;
return (2 + cljs.core.count(self__.__extmap));
}));

(datascript.pull_api.Context.prototype.cljs$core$IHash$_hash$arity$1 = (function (this__5444__auto__){
var self__ = this;
var this__5444__auto____$1 = this;
var h__5251__auto__ = self__.__hash;
if((!((h__5251__auto__ == null)))){
return h__5251__auto__;
} else {
var h__5251__auto____$1 = (function (coll__5445__auto__){
return (1727735959 ^ cljs.core.hash_unordered_coll(coll__5445__auto__));
})(this__5444__auto____$1);
(self__.__hash = h__5251__auto____$1);

return h__5251__auto____$1;
}
}));

(datascript.pull_api.Context.prototype.cljs$core$IEquiv$_equiv$arity$2 = (function (this40523,other40524){
var self__ = this;
var this40523__$1 = this;
return (((!((other40524 == null)))) && ((((this40523__$1.constructor === other40524.constructor)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40523__$1.db,other40524.db)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40523__$1.visitor,other40524.visitor)) && (cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40523__$1.__extmap,other40524.__extmap)))))))));
}));

(datascript.pull_api.Context.prototype.cljs$core$IMap$_dissoc$arity$2 = (function (this__5458__auto__,k__5459__auto__){
var self__ = this;
var this__5458__auto____$1 = this;
if(cljs.core.contains_QMARK_(new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"db","db",993250759),null,new cljs.core.Keyword(null,"visitor","visitor",-1026865865),null], null), null),k__5459__auto__)){
return cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(cljs.core._with_meta(cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,this__5458__auto____$1),self__.__meta),k__5459__auto__);
} else {
return (new datascript.pull_api.Context(self__.db,self__.visitor,self__.__meta,cljs.core.not_empty(cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(self__.__extmap,k__5459__auto__)),null));
}
}));

(datascript.pull_api.Context.prototype.cljs$core$IAssociative$_contains_key_QMARK_$arity$2 = (function (this__5455__auto__,k40522){
var self__ = this;
var this__5455__auto____$1 = this;
var G__40546 = k40522;
var G__40546__$1 = (((G__40546 instanceof cljs.core.Keyword))?G__40546.fqn:null);
switch (G__40546__$1) {
case "db":
case "visitor":
return true;

break;
default:
return cljs.core.contains_QMARK_(self__.__extmap,k40522);

}
}));

(datascript.pull_api.Context.prototype.cljs$core$IAssociative$_assoc$arity$3 = (function (this__5456__auto__,k__5457__auto__,G__40521){
var self__ = this;
var this__5456__auto____$1 = this;
var pred__40548 = cljs.core.keyword_identical_QMARK_;
var expr__40549 = k__5457__auto__;
if(cljs.core.truth_((pred__40548.cljs$core$IFn$_invoke$arity$2 ? pred__40548.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"db","db",993250759),expr__40549) : pred__40548.call(null,new cljs.core.Keyword(null,"db","db",993250759),expr__40549)))){
return (new datascript.pull_api.Context(G__40521,self__.visitor,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__40548.cljs$core$IFn$_invoke$arity$2 ? pred__40548.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"visitor","visitor",-1026865865),expr__40549) : pred__40548.call(null,new cljs.core.Keyword(null,"visitor","visitor",-1026865865),expr__40549)))){
return (new datascript.pull_api.Context(self__.db,G__40521,self__.__meta,self__.__extmap,null));
} else {
return (new datascript.pull_api.Context(self__.db,self__.visitor,self__.__meta,cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(self__.__extmap,k__5457__auto__,G__40521),null));
}
}
}));

(datascript.pull_api.Context.prototype.cljs$core$ISeqable$_seq$arity$1 = (function (this__5461__auto__){
var self__ = this;
var this__5461__auto____$1 = this;
return cljs.core.seq(cljs.core.concat.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new cljs.core.MapEntry(new cljs.core.Keyword(null,"db","db",993250759),self__.db,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"visitor","visitor",-1026865865),self__.visitor,null))], null),self__.__extmap));
}));

(datascript.pull_api.Context.prototype.cljs$core$IWithMeta$_with_meta$arity$2 = (function (this__5447__auto__,G__40521){
var self__ = this;
var this__5447__auto____$1 = this;
return (new datascript.pull_api.Context(self__.db,self__.visitor,G__40521,self__.__extmap,self__.__hash));
}));

(datascript.pull_api.Context.prototype.cljs$core$ICollection$_conj$arity$2 = (function (this__5453__auto__,entry__5454__auto__){
var self__ = this;
var this__5453__auto____$1 = this;
if(cljs.core.vector_QMARK_(entry__5454__auto__)){
return this__5453__auto____$1.cljs$core$IAssociative$_assoc$arity$3(null,cljs.core._nth(entry__5454__auto__,(0)),cljs.core._nth(entry__5454__auto__,(1)));
} else {
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(cljs.core._conj,this__5453__auto____$1,entry__5454__auto__);
}
}));

(datascript.pull_api.Context.getBasis = (function (){
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"db","db",-1661185010,null),new cljs.core.Symbol(null,"visitor","visitor",613665662,null)], null);
}));

(datascript.pull_api.Context.cljs$lang$type = true);

(datascript.pull_api.Context.cljs$lang$ctorPrSeq = (function (this__5494__auto__){
return (new cljs.core.List(null,"datascript.pull-api/Context",null,(1),null));
}));

(datascript.pull_api.Context.cljs$lang$ctorPrWriter = (function (this__5494__auto__,writer__5495__auto__){
return cljs.core._write(writer__5495__auto__,"datascript.pull-api/Context");
}));

/**
 * Positional factory function for datascript.pull-api/Context.
 */
datascript.pull_api.__GT_Context = (function datascript$pull_api$__GT_Context(db,visitor){
return (new datascript.pull_api.Context(db,visitor,null,null,null));
});

/**
 * Factory function for datascript.pull-api/Context, taking a map of keywords to field values.
 */
datascript.pull_api.map__GT_Context = (function datascript$pull_api$map__GT_Context(G__40526){
var extmap__5490__auto__ = (function (){var G__40556 = cljs.core.dissoc.cljs$core$IFn$_invoke$arity$variadic(G__40526,new cljs.core.Keyword(null,"db","db",993250759),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"visitor","visitor",-1026865865)], 0));
if(cljs.core.record_QMARK_(G__40526)){
return cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,G__40556);
} else {
return G__40556;
}
})();
return (new datascript.pull_api.Context(new cljs.core.Keyword(null,"db","db",993250759).cljs$core$IFn$_invoke$arity$1(G__40526),new cljs.core.Keyword(null,"visitor","visitor",-1026865865).cljs$core$IFn$_invoke$arity$1(G__40526),null,cljs.core.not_empty(extmap__5490__auto__),null));
});

datascript.pull_api.visit = (function datascript$pull_api$visit(context,pattern,e,a,v){
var temp__5808__auto__ = context.visitor;
if((temp__5808__auto__ == null)){
return null;
} else {
var visitor = temp__5808__auto__;
return (visitor.cljs$core$IFn$_invoke$arity$4 ? visitor.cljs$core$IFn$_invoke$arity$4(pattern,e,a,v) : visitor.call(null,pattern,e,a,v));
}
});

/**
 * @interface
 */
datascript.pull_api.IFrame = function(){};

var datascript$pull_api$IFrame$_merge$dyn_40892 = (function (this$,result){
var x__5498__auto__ = (((this$ == null))?null:this$);
var m__5499__auto__ = (datascript.pull_api._merge[goog.typeOf(x__5498__auto__)]);
if((!((m__5499__auto__ == null)))){
return (m__5499__auto__.cljs$core$IFn$_invoke$arity$2 ? m__5499__auto__.cljs$core$IFn$_invoke$arity$2(this$,result) : m__5499__auto__.call(null,this$,result));
} else {
var m__5497__auto__ = (datascript.pull_api._merge["_"]);
if((!((m__5497__auto__ == null)))){
return (m__5497__auto__.cljs$core$IFn$_invoke$arity$2 ? m__5497__auto__.cljs$core$IFn$_invoke$arity$2(this$,result) : m__5497__auto__.call(null,this$,result));
} else {
throw cljs.core.missing_protocol("IFrame.-merge",this$);
}
}
});
datascript.pull_api._merge = (function datascript$pull_api$_merge(this$,result){
if((((!((this$ == null)))) && ((!((this$.datascript$pull_api$IFrame$_merge$arity$2 == null)))))){
return this$.datascript$pull_api$IFrame$_merge$arity$2(this$,result);
} else {
return datascript$pull_api$IFrame$_merge$dyn_40892(this$,result);
}
});

var datascript$pull_api$IFrame$_run$dyn_40893 = (function (this$,context){
var x__5498__auto__ = (((this$ == null))?null:this$);
var m__5499__auto__ = (datascript.pull_api._run[goog.typeOf(x__5498__auto__)]);
if((!((m__5499__auto__ == null)))){
return (m__5499__auto__.cljs$core$IFn$_invoke$arity$2 ? m__5499__auto__.cljs$core$IFn$_invoke$arity$2(this$,context) : m__5499__auto__.call(null,this$,context));
} else {
var m__5497__auto__ = (datascript.pull_api._run["_"]);
if((!((m__5497__auto__ == null)))){
return (m__5497__auto__.cljs$core$IFn$_invoke$arity$2 ? m__5497__auto__.cljs$core$IFn$_invoke$arity$2(this$,context) : m__5497__auto__.call(null,this$,context));
} else {
throw cljs.core.missing_protocol("IFrame.-run",this$);
}
}
});
datascript.pull_api._run = (function datascript$pull_api$_run(this$,context){
if((((!((this$ == null)))) && ((!((this$.datascript$pull_api$IFrame$_run$arity$2 == null)))))){
return this$.datascript$pull_api$IFrame$_run$arity$2(this$,context);
} else {
return datascript$pull_api$IFrame$_run$dyn_40893(this$,context);
}
});

var datascript$pull_api$IFrame$_str$dyn_40894 = (function (this$){
var x__5498__auto__ = (((this$ == null))?null:this$);
var m__5499__auto__ = (datascript.pull_api._str[goog.typeOf(x__5498__auto__)]);
if((!((m__5499__auto__ == null)))){
return (m__5499__auto__.cljs$core$IFn$_invoke$arity$1 ? m__5499__auto__.cljs$core$IFn$_invoke$arity$1(this$) : m__5499__auto__.call(null,this$));
} else {
var m__5497__auto__ = (datascript.pull_api._str["_"]);
if((!((m__5497__auto__ == null)))){
return (m__5497__auto__.cljs$core$IFn$_invoke$arity$1 ? m__5497__auto__.cljs$core$IFn$_invoke$arity$1(this$) : m__5497__auto__.call(null,this$));
} else {
throw cljs.core.missing_protocol("IFrame.-str",this$);
}
}
});
datascript.pull_api._str = (function datascript$pull_api$_str(this$){
if((((!((this$ == null)))) && ((!((this$.datascript$pull_api$IFrame$_str$arity$1 == null)))))){
return this$.datascript$pull_api$IFrame$_str$arity$1(this$);
} else {
return datascript$pull_api$IFrame$_str$dyn_40894(this$);
}
});

datascript.pull_api.attr_str = (function datascript$pull_api$attr_str(attr){
var or__5142__auto__ = new cljs.core.Keyword(null,"as","as",1148689641).cljs$core$IFn$_invoke$arity$1(attr);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return new cljs.core.Keyword(null,"name","name",1843675177).cljs$core$IFn$_invoke$arity$1(attr);
}
});

/**
* @constructor
 * @implements {cljs.core.IRecord}
 * @implements {datascript.pull_api.IFrame}
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
datascript.pull_api.ResultFrame = (function (value,datoms,__meta,__extmap,__hash){
this.value = value;
this.datoms = datoms;
this.__meta = __meta;
this.__extmap = __extmap;
this.__hash = __hash;
this.cljs$lang$protocol_mask$partition0$ = 2230716170;
this.cljs$lang$protocol_mask$partition1$ = 139264;
});
(datascript.pull_api.ResultFrame.prototype.cljs$core$ILookup$_lookup$arity$2 = (function (this__5448__auto__,k__5449__auto__){
var self__ = this;
var this__5448__auto____$1 = this;
return this__5448__auto____$1.cljs$core$ILookup$_lookup$arity$3(null,k__5449__auto__,null);
}));

(datascript.pull_api.ResultFrame.prototype.cljs$core$ILookup$_lookup$arity$3 = (function (this__5450__auto__,k40571,else__5451__auto__){
var self__ = this;
var this__5450__auto____$1 = this;
var G__40575 = k40571;
var G__40575__$1 = (((G__40575 instanceof cljs.core.Keyword))?G__40575.fqn:null);
switch (G__40575__$1) {
case "value":
return self__.value;

break;
case "datoms":
return self__.datoms;

break;
default:
return cljs.core.get.cljs$core$IFn$_invoke$arity$3(self__.__extmap,k40571,else__5451__auto__);

}
}));

(datascript.pull_api.ResultFrame.prototype.cljs$core$IKVReduce$_kv_reduce$arity$3 = (function (this__5468__auto__,f__5469__auto__,init__5470__auto__){
var self__ = this;
var this__5468__auto____$1 = this;
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (ret__5471__auto__,p__40576){
var vec__40577 = p__40576;
var k__5472__auto__ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40577,(0),null);
var v__5473__auto__ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40577,(1),null);
return (f__5469__auto__.cljs$core$IFn$_invoke$arity$3 ? f__5469__auto__.cljs$core$IFn$_invoke$arity$3(ret__5471__auto__,k__5472__auto__,v__5473__auto__) : f__5469__auto__.call(null,ret__5471__auto__,k__5472__auto__,v__5473__auto__));
}),init__5470__auto__,this__5468__auto____$1);
}));

(datascript.pull_api.ResultFrame.prototype.cljs$core$IPrintWithWriter$_pr_writer$arity$3 = (function (this__5463__auto__,writer__5464__auto__,opts__5465__auto__){
var self__ = this;
var this__5463__auto____$1 = this;
var pr_pair__5466__auto__ = (function (keyval__5467__auto__){
return cljs.core.pr_sequential_writer(writer__5464__auto__,cljs.core.pr_writer,""," ","",opts__5465__auto__,keyval__5467__auto__);
});
return cljs.core.pr_sequential_writer(writer__5464__auto__,pr_pair__5466__auto__,"#datascript.pull-api.ResultFrame{",", ","}",opts__5465__auto__,cljs.core.concat.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"value","value",305978217),self__.value],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"datoms","datoms",-290874434),self__.datoms],null))], null),self__.__extmap));
}));

(datascript.pull_api.ResultFrame.prototype.cljs$core$IIterable$_iterator$arity$1 = (function (G__40570){
var self__ = this;
var G__40570__$1 = this;
return (new cljs.core.RecordIter((0),G__40570__$1,2,new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"value","value",305978217),new cljs.core.Keyword(null,"datoms","datoms",-290874434)], null),(cljs.core.truth_(self__.__extmap)?cljs.core._iterator(self__.__extmap):cljs.core.nil_iter())));
}));

(datascript.pull_api.ResultFrame.prototype.cljs$core$IMeta$_meta$arity$1 = (function (this__5446__auto__){
var self__ = this;
var this__5446__auto____$1 = this;
return self__.__meta;
}));

(datascript.pull_api.ResultFrame.prototype.cljs$core$ICloneable$_clone$arity$1 = (function (this__5443__auto__){
var self__ = this;
var this__5443__auto____$1 = this;
return (new datascript.pull_api.ResultFrame(self__.value,self__.datoms,self__.__meta,self__.__extmap,self__.__hash));
}));

(datascript.pull_api.ResultFrame.prototype.cljs$core$ICounted$_count$arity$1 = (function (this__5452__auto__){
var self__ = this;
var this__5452__auto____$1 = this;
return (2 + cljs.core.count(self__.__extmap));
}));

(datascript.pull_api.ResultFrame.prototype.cljs$core$IHash$_hash$arity$1 = (function (this__5444__auto__){
var self__ = this;
var this__5444__auto____$1 = this;
var h__5251__auto__ = self__.__hash;
if((!((h__5251__auto__ == null)))){
return h__5251__auto__;
} else {
var h__5251__auto____$1 = (function (coll__5445__auto__){
return (836381841 ^ cljs.core.hash_unordered_coll(coll__5445__auto__));
})(this__5444__auto____$1);
(self__.__hash = h__5251__auto____$1);

return h__5251__auto____$1;
}
}));

(datascript.pull_api.ResultFrame.prototype.cljs$core$IEquiv$_equiv$arity$2 = (function (this40572,other40573){
var self__ = this;
var this40572__$1 = this;
return (((!((other40573 == null)))) && ((((this40572__$1.constructor === other40573.constructor)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40572__$1.value,other40573.value)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40572__$1.datoms,other40573.datoms)) && (cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40572__$1.__extmap,other40573.__extmap)))))))));
}));

(datascript.pull_api.ResultFrame.prototype.cljs$core$IMap$_dissoc$arity$2 = (function (this__5458__auto__,k__5459__auto__){
var self__ = this;
var this__5458__auto____$1 = this;
if(cljs.core.contains_QMARK_(new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"value","value",305978217),null,new cljs.core.Keyword(null,"datoms","datoms",-290874434),null], null), null),k__5459__auto__)){
return cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(cljs.core._with_meta(cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,this__5458__auto____$1),self__.__meta),k__5459__auto__);
} else {
return (new datascript.pull_api.ResultFrame(self__.value,self__.datoms,self__.__meta,cljs.core.not_empty(cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(self__.__extmap,k__5459__auto__)),null));
}
}));

(datascript.pull_api.ResultFrame.prototype.cljs$core$IAssociative$_contains_key_QMARK_$arity$2 = (function (this__5455__auto__,k40571){
var self__ = this;
var this__5455__auto____$1 = this;
var G__40580 = k40571;
var G__40580__$1 = (((G__40580 instanceof cljs.core.Keyword))?G__40580.fqn:null);
switch (G__40580__$1) {
case "value":
case "datoms":
return true;

break;
default:
return cljs.core.contains_QMARK_(self__.__extmap,k40571);

}
}));

(datascript.pull_api.ResultFrame.prototype.cljs$core$IAssociative$_assoc$arity$3 = (function (this__5456__auto__,k__5457__auto__,G__40570){
var self__ = this;
var this__5456__auto____$1 = this;
var pred__40581 = cljs.core.keyword_identical_QMARK_;
var expr__40582 = k__5457__auto__;
if(cljs.core.truth_((pred__40581.cljs$core$IFn$_invoke$arity$2 ? pred__40581.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"value","value",305978217),expr__40582) : pred__40581.call(null,new cljs.core.Keyword(null,"value","value",305978217),expr__40582)))){
return (new datascript.pull_api.ResultFrame(G__40570,self__.datoms,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__40581.cljs$core$IFn$_invoke$arity$2 ? pred__40581.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"datoms","datoms",-290874434),expr__40582) : pred__40581.call(null,new cljs.core.Keyword(null,"datoms","datoms",-290874434),expr__40582)))){
return (new datascript.pull_api.ResultFrame(self__.value,G__40570,self__.__meta,self__.__extmap,null));
} else {
return (new datascript.pull_api.ResultFrame(self__.value,self__.datoms,self__.__meta,cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(self__.__extmap,k__5457__auto__,G__40570),null));
}
}
}));

(datascript.pull_api.ResultFrame.prototype.cljs$core$ISeqable$_seq$arity$1 = (function (this__5461__auto__){
var self__ = this;
var this__5461__auto____$1 = this;
return cljs.core.seq(cljs.core.concat.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new cljs.core.MapEntry(new cljs.core.Keyword(null,"value","value",305978217),self__.value,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"datoms","datoms",-290874434),self__.datoms,null))], null),self__.__extmap));
}));

(datascript.pull_api.ResultFrame.prototype.cljs$core$IWithMeta$_with_meta$arity$2 = (function (this__5447__auto__,G__40570){
var self__ = this;
var this__5447__auto____$1 = this;
return (new datascript.pull_api.ResultFrame(self__.value,self__.datoms,G__40570,self__.__extmap,self__.__hash));
}));

(datascript.pull_api.ResultFrame.prototype.cljs$core$ICollection$_conj$arity$2 = (function (this__5453__auto__,entry__5454__auto__){
var self__ = this;
var this__5453__auto____$1 = this;
if(cljs.core.vector_QMARK_(entry__5454__auto__)){
return this__5453__auto____$1.cljs$core$IAssociative$_assoc$arity$3(null,cljs.core._nth(entry__5454__auto__,(0)),cljs.core._nth(entry__5454__auto__,(1)));
} else {
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(cljs.core._conj,this__5453__auto____$1,entry__5454__auto__);
}
}));

(datascript.pull_api.ResultFrame.prototype.datascript$pull_api$IFrame$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.pull_api.ResultFrame.prototype.datascript$pull_api$IFrame$_str$arity$1 = (function (this$){
var self__ = this;
var this$__$1 = this;
return (""+"ResultFrame<value="+cljs.core.str.cljs$core$IFn$_invoke$arity$1(self__.value)+">");
}));

(datascript.pull_api.ResultFrame.getBasis = (function (){
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"value","value",1946509744,null),new cljs.core.Symbol(null,"datoms","datoms",1349657093,null)], null);
}));

(datascript.pull_api.ResultFrame.cljs$lang$type = true);

(datascript.pull_api.ResultFrame.cljs$lang$ctorPrSeq = (function (this__5494__auto__){
return (new cljs.core.List(null,"datascript.pull-api/ResultFrame",null,(1),null));
}));

(datascript.pull_api.ResultFrame.cljs$lang$ctorPrWriter = (function (this__5494__auto__,writer__5495__auto__){
return cljs.core._write(writer__5495__auto__,"datascript.pull-api/ResultFrame");
}));

/**
 * Positional factory function for datascript.pull-api/ResultFrame.
 */
datascript.pull_api.__GT_ResultFrame = (function datascript$pull_api$__GT_ResultFrame(value,datoms){
return (new datascript.pull_api.ResultFrame(value,datoms,null,null,null));
});

/**
 * Factory function for datascript.pull-api/ResultFrame, taking a map of keywords to field values.
 */
datascript.pull_api.map__GT_ResultFrame = (function datascript$pull_api$map__GT_ResultFrame(G__40574){
var extmap__5490__auto__ = (function (){var G__40584 = cljs.core.dissoc.cljs$core$IFn$_invoke$arity$variadic(G__40574,new cljs.core.Keyword(null,"value","value",305978217),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"datoms","datoms",-290874434)], 0));
if(cljs.core.record_QMARK_(G__40574)){
return cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,G__40584);
} else {
return G__40584;
}
})();
return (new datascript.pull_api.ResultFrame(new cljs.core.Keyword(null,"value","value",305978217).cljs$core$IFn$_invoke$arity$1(G__40574),new cljs.core.Keyword(null,"datoms","datoms",-290874434).cljs$core$IFn$_invoke$arity$1(G__40574),null,cljs.core.not_empty(extmap__5490__auto__),null));
});


/**
* @constructor
 * @implements {cljs.core.IRecord}
 * @implements {datascript.pull_api.IFrame}
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
datascript.pull_api.MultivalAttrFrame = (function (acc,attr,datoms,__meta,__extmap,__hash){
this.acc = acc;
this.attr = attr;
this.datoms = datoms;
this.__meta = __meta;
this.__extmap = __extmap;
this.__hash = __hash;
this.cljs$lang$protocol_mask$partition0$ = 2230716170;
this.cljs$lang$protocol_mask$partition1$ = 139264;
});
(datascript.pull_api.MultivalAttrFrame.prototype.cljs$core$ILookup$_lookup$arity$2 = (function (this__5448__auto__,k__5449__auto__){
var self__ = this;
var this__5448__auto____$1 = this;
return this__5448__auto____$1.cljs$core$ILookup$_lookup$arity$3(null,k__5449__auto__,null);
}));

(datascript.pull_api.MultivalAttrFrame.prototype.cljs$core$ILookup$_lookup$arity$3 = (function (this__5450__auto__,k40586,else__5451__auto__){
var self__ = this;
var this__5450__auto____$1 = this;
var G__40590 = k40586;
var G__40590__$1 = (((G__40590 instanceof cljs.core.Keyword))?G__40590.fqn:null);
switch (G__40590__$1) {
case "acc":
return self__.acc;

break;
case "attr":
return self__.attr;

break;
case "datoms":
return self__.datoms;

break;
default:
return cljs.core.get.cljs$core$IFn$_invoke$arity$3(self__.__extmap,k40586,else__5451__auto__);

}
}));

(datascript.pull_api.MultivalAttrFrame.prototype.cljs$core$IKVReduce$_kv_reduce$arity$3 = (function (this__5468__auto__,f__5469__auto__,init__5470__auto__){
var self__ = this;
var this__5468__auto____$1 = this;
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (ret__5471__auto__,p__40591){
var vec__40592 = p__40591;
var k__5472__auto__ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40592,(0),null);
var v__5473__auto__ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40592,(1),null);
return (f__5469__auto__.cljs$core$IFn$_invoke$arity$3 ? f__5469__auto__.cljs$core$IFn$_invoke$arity$3(ret__5471__auto__,k__5472__auto__,v__5473__auto__) : f__5469__auto__.call(null,ret__5471__auto__,k__5472__auto__,v__5473__auto__));
}),init__5470__auto__,this__5468__auto____$1);
}));

(datascript.pull_api.MultivalAttrFrame.prototype.cljs$core$IPrintWithWriter$_pr_writer$arity$3 = (function (this__5463__auto__,writer__5464__auto__,opts__5465__auto__){
var self__ = this;
var this__5463__auto____$1 = this;
var pr_pair__5466__auto__ = (function (keyval__5467__auto__){
return cljs.core.pr_sequential_writer(writer__5464__auto__,cljs.core.pr_writer,""," ","",opts__5465__auto__,keyval__5467__auto__);
});
return cljs.core.pr_sequential_writer(writer__5464__auto__,pr_pair__5466__auto__,"#datascript.pull-api.MultivalAttrFrame{",", ","}",opts__5465__auto__,cljs.core.concat.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"acc","acc",838566312),self__.acc],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"attr","attr",-604132353),self__.attr],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"datoms","datoms",-290874434),self__.datoms],null))], null),self__.__extmap));
}));

(datascript.pull_api.MultivalAttrFrame.prototype.cljs$core$IIterable$_iterator$arity$1 = (function (G__40585){
var self__ = this;
var G__40585__$1 = this;
return (new cljs.core.RecordIter((0),G__40585__$1,3,new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"acc","acc",838566312),new cljs.core.Keyword(null,"attr","attr",-604132353),new cljs.core.Keyword(null,"datoms","datoms",-290874434)], null),(cljs.core.truth_(self__.__extmap)?cljs.core._iterator(self__.__extmap):cljs.core.nil_iter())));
}));

(datascript.pull_api.MultivalAttrFrame.prototype.cljs$core$IMeta$_meta$arity$1 = (function (this__5446__auto__){
var self__ = this;
var this__5446__auto____$1 = this;
return self__.__meta;
}));

(datascript.pull_api.MultivalAttrFrame.prototype.cljs$core$ICloneable$_clone$arity$1 = (function (this__5443__auto__){
var self__ = this;
var this__5443__auto____$1 = this;
return (new datascript.pull_api.MultivalAttrFrame(self__.acc,self__.attr,self__.datoms,self__.__meta,self__.__extmap,self__.__hash));
}));

(datascript.pull_api.MultivalAttrFrame.prototype.cljs$core$ICounted$_count$arity$1 = (function (this__5452__auto__){
var self__ = this;
var this__5452__auto____$1 = this;
return (3 + cljs.core.count(self__.__extmap));
}));

(datascript.pull_api.MultivalAttrFrame.prototype.cljs$core$IHash$_hash$arity$1 = (function (this__5444__auto__){
var self__ = this;
var this__5444__auto____$1 = this;
var h__5251__auto__ = self__.__hash;
if((!((h__5251__auto__ == null)))){
return h__5251__auto__;
} else {
var h__5251__auto____$1 = (function (coll__5445__auto__){
return (1829867699 ^ cljs.core.hash_unordered_coll(coll__5445__auto__));
})(this__5444__auto____$1);
(self__.__hash = h__5251__auto____$1);

return h__5251__auto____$1;
}
}));

(datascript.pull_api.MultivalAttrFrame.prototype.cljs$core$IEquiv$_equiv$arity$2 = (function (this40587,other40588){
var self__ = this;
var this40587__$1 = this;
return (((!((other40588 == null)))) && ((((this40587__$1.constructor === other40588.constructor)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40587__$1.acc,other40588.acc)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40587__$1.attr,other40588.attr)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40587__$1.datoms,other40588.datoms)) && (cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40587__$1.__extmap,other40588.__extmap)))))))))));
}));

(datascript.pull_api.MultivalAttrFrame.prototype.cljs$core$IMap$_dissoc$arity$2 = (function (this__5458__auto__,k__5459__auto__){
var self__ = this;
var this__5458__auto____$1 = this;
if(cljs.core.contains_QMARK_(new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"acc","acc",838566312),null,new cljs.core.Keyword(null,"datoms","datoms",-290874434),null,new cljs.core.Keyword(null,"attr","attr",-604132353),null], null), null),k__5459__auto__)){
return cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(cljs.core._with_meta(cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,this__5458__auto____$1),self__.__meta),k__5459__auto__);
} else {
return (new datascript.pull_api.MultivalAttrFrame(self__.acc,self__.attr,self__.datoms,self__.__meta,cljs.core.not_empty(cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(self__.__extmap,k__5459__auto__)),null));
}
}));

(datascript.pull_api.MultivalAttrFrame.prototype.cljs$core$IAssociative$_contains_key_QMARK_$arity$2 = (function (this__5455__auto__,k40586){
var self__ = this;
var this__5455__auto____$1 = this;
var G__40595 = k40586;
var G__40595__$1 = (((G__40595 instanceof cljs.core.Keyword))?G__40595.fqn:null);
switch (G__40595__$1) {
case "acc":
case "attr":
case "datoms":
return true;

break;
default:
return cljs.core.contains_QMARK_(self__.__extmap,k40586);

}
}));

(datascript.pull_api.MultivalAttrFrame.prototype.cljs$core$IAssociative$_assoc$arity$3 = (function (this__5456__auto__,k__5457__auto__,G__40585){
var self__ = this;
var this__5456__auto____$1 = this;
var pred__40596 = cljs.core.keyword_identical_QMARK_;
var expr__40597 = k__5457__auto__;
if(cljs.core.truth_((pred__40596.cljs$core$IFn$_invoke$arity$2 ? pred__40596.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"acc","acc",838566312),expr__40597) : pred__40596.call(null,new cljs.core.Keyword(null,"acc","acc",838566312),expr__40597)))){
return (new datascript.pull_api.MultivalAttrFrame(G__40585,self__.attr,self__.datoms,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__40596.cljs$core$IFn$_invoke$arity$2 ? pred__40596.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"attr","attr",-604132353),expr__40597) : pred__40596.call(null,new cljs.core.Keyword(null,"attr","attr",-604132353),expr__40597)))){
return (new datascript.pull_api.MultivalAttrFrame(self__.acc,G__40585,self__.datoms,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__40596.cljs$core$IFn$_invoke$arity$2 ? pred__40596.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"datoms","datoms",-290874434),expr__40597) : pred__40596.call(null,new cljs.core.Keyword(null,"datoms","datoms",-290874434),expr__40597)))){
return (new datascript.pull_api.MultivalAttrFrame(self__.acc,self__.attr,G__40585,self__.__meta,self__.__extmap,null));
} else {
return (new datascript.pull_api.MultivalAttrFrame(self__.acc,self__.attr,self__.datoms,self__.__meta,cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(self__.__extmap,k__5457__auto__,G__40585),null));
}
}
}
}));

(datascript.pull_api.MultivalAttrFrame.prototype.cljs$core$ISeqable$_seq$arity$1 = (function (this__5461__auto__){
var self__ = this;
var this__5461__auto____$1 = this;
return cljs.core.seq(cljs.core.concat.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new cljs.core.MapEntry(new cljs.core.Keyword(null,"acc","acc",838566312),self__.acc,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"attr","attr",-604132353),self__.attr,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"datoms","datoms",-290874434),self__.datoms,null))], null),self__.__extmap));
}));

(datascript.pull_api.MultivalAttrFrame.prototype.cljs$core$IWithMeta$_with_meta$arity$2 = (function (this__5447__auto__,G__40585){
var self__ = this;
var this__5447__auto____$1 = this;
return (new datascript.pull_api.MultivalAttrFrame(self__.acc,self__.attr,self__.datoms,G__40585,self__.__extmap,self__.__hash));
}));

(datascript.pull_api.MultivalAttrFrame.prototype.cljs$core$ICollection$_conj$arity$2 = (function (this__5453__auto__,entry__5454__auto__){
var self__ = this;
var this__5453__auto____$1 = this;
if(cljs.core.vector_QMARK_(entry__5454__auto__)){
return this__5453__auto____$1.cljs$core$IAssociative$_assoc$arity$3(null,cljs.core._nth(entry__5454__auto__,(0)),cljs.core._nth(entry__5454__auto__,(1)));
} else {
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(cljs.core._conj,this__5453__auto____$1,entry__5454__auto__);
}
}));

(datascript.pull_api.MultivalAttrFrame.prototype.datascript$pull_api$IFrame$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.pull_api.MultivalAttrFrame.prototype.datascript$pull_api$IFrame$_run$arity$2 = (function (this$,context){
var self__ = this;
var this$__$1 = this;
var acc__$1 = self__.acc;
var datoms__$1 = self__.datoms;
while(true){
var datom = datascript.pull_api.first_seq(datoms__$1);
if((((datom == null)) || (cljs.core.not_EQ_.cljs$core$IFn$_invoke$arity$2(datom.a,self__.attr.name)))){
return new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new datascript.pull_api.ResultFrame(cljs.core.not_empty(cljs.core.persistent_BANG_(acc__$1)),(function (){var or__5142__auto__ = datoms__$1;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return cljs.core.List.EMPTY;
}
})(),null,null,null))], null);
} else {
if(cljs.core.truth_((function (){var and__5140__auto__ = self__.attr.limit;
if(cljs.core.truth_(and__5140__auto__)){
return (((cljs.core.count(acc__$1) >= self__.attr.limit)) && (true));
} else {
return and__5140__auto__;
}
})())){
var datoms__$2 = datoms__$1;
while(true){
var datom__$1 = datascript.pull_api.first_seq(datoms__$2);
if((((datom__$1 == null)) || (cljs.core.not_EQ_.cljs$core$IFn$_invoke$arity$2(datom__$1.a,self__.attr.name)))){
return new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new datascript.pull_api.ResultFrame(cljs.core.persistent_BANG_(acc__$1),(function (){var or__5142__auto__ = datoms__$2;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return cljs.core.List.EMPTY;
}
})(),null,null,null))], null);
} else {
var G__40916 = datascript.pull_api.next_seq(datoms__$2);
datoms__$2 = G__40916;
continue;
}
break;
}
} else {
var G__40917 = cljs.core.conj_BANG_.cljs$core$IFn$_invoke$arity$2(acc__$1,datom.v);
var G__40918 = datascript.pull_api.next_seq(datoms__$1);
acc__$1 = G__40917;
datoms__$1 = G__40918;
continue;

}
}
break;
}
}));

(datascript.pull_api.MultivalAttrFrame.prototype.datascript$pull_api$IFrame$_str$arity$1 = (function (this$){
var self__ = this;
var this$__$1 = this;
return (""+"MultivalAttrFrame<attr="+cljs.core.str.cljs$core$IFn$_invoke$arity$1(datascript.pull_api.attr_str(self__.attr))+">");
}));

(datascript.pull_api.MultivalAttrFrame.getBasis = (function (){
return new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"acc","acc",-1815869457,null),cljs.core.with_meta(new cljs.core.Symbol(null,"attr","attr",1036399174,null),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"tag","tag",-1290361223),new cljs.core.Symbol(null,"PullAttr","PullAttr",1557473458,null)], null)),new cljs.core.Symbol(null,"datoms","datoms",1349657093,null)], null);
}));

(datascript.pull_api.MultivalAttrFrame.cljs$lang$type = true);

(datascript.pull_api.MultivalAttrFrame.cljs$lang$ctorPrSeq = (function (this__5494__auto__){
return (new cljs.core.List(null,"datascript.pull-api/MultivalAttrFrame",null,(1),null));
}));

(datascript.pull_api.MultivalAttrFrame.cljs$lang$ctorPrWriter = (function (this__5494__auto__,writer__5495__auto__){
return cljs.core._write(writer__5495__auto__,"datascript.pull-api/MultivalAttrFrame");
}));

/**
 * Positional factory function for datascript.pull-api/MultivalAttrFrame.
 */
datascript.pull_api.__GT_MultivalAttrFrame = (function datascript$pull_api$__GT_MultivalAttrFrame(acc,attr,datoms){
return (new datascript.pull_api.MultivalAttrFrame(acc,attr,datoms,null,null,null));
});

/**
 * Factory function for datascript.pull-api/MultivalAttrFrame, taking a map of keywords to field values.
 */
datascript.pull_api.map__GT_MultivalAttrFrame = (function datascript$pull_api$map__GT_MultivalAttrFrame(G__40589){
var extmap__5490__auto__ = (function (){var G__40607 = cljs.core.dissoc.cljs$core$IFn$_invoke$arity$variadic(G__40589,new cljs.core.Keyword(null,"acc","acc",838566312),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"attr","attr",-604132353),new cljs.core.Keyword(null,"datoms","datoms",-290874434)], 0));
if(cljs.core.record_QMARK_(G__40589)){
return cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,G__40607);
} else {
return G__40607;
}
})();
return (new datascript.pull_api.MultivalAttrFrame(new cljs.core.Keyword(null,"acc","acc",838566312).cljs$core$IFn$_invoke$arity$1(G__40589),new cljs.core.Keyword(null,"attr","attr",-604132353).cljs$core$IFn$_invoke$arity$1(G__40589),new cljs.core.Keyword(null,"datoms","datoms",-290874434).cljs$core$IFn$_invoke$arity$1(G__40589),null,cljs.core.not_empty(extmap__5490__auto__),null));
});


/**
* @constructor
 * @implements {cljs.core.IRecord}
 * @implements {datascript.pull_api.IFrame}
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
datascript.pull_api.MultivalRefAttrFrame = (function (seen,recursion_limits,acc,pattern,attr,datoms,__meta,__extmap,__hash){
this.seen = seen;
this.recursion_limits = recursion_limits;
this.acc = acc;
this.pattern = pattern;
this.attr = attr;
this.datoms = datoms;
this.__meta = __meta;
this.__extmap = __extmap;
this.__hash = __hash;
this.cljs$lang$protocol_mask$partition0$ = 2230716170;
this.cljs$lang$protocol_mask$partition1$ = 139264;
});
(datascript.pull_api.MultivalRefAttrFrame.prototype.cljs$core$ILookup$_lookup$arity$2 = (function (this__5448__auto__,k__5449__auto__){
var self__ = this;
var this__5448__auto____$1 = this;
return this__5448__auto____$1.cljs$core$ILookup$_lookup$arity$3(null,k__5449__auto__,null);
}));

(datascript.pull_api.MultivalRefAttrFrame.prototype.cljs$core$ILookup$_lookup$arity$3 = (function (this__5450__auto__,k40609,else__5451__auto__){
var self__ = this;
var this__5450__auto____$1 = this;
var G__40615 = k40609;
var G__40615__$1 = (((G__40615 instanceof cljs.core.Keyword))?G__40615.fqn:null);
switch (G__40615__$1) {
case "seen":
return self__.seen;

break;
case "recursion-limits":
return self__.recursion_limits;

break;
case "acc":
return self__.acc;

break;
case "pattern":
return self__.pattern;

break;
case "attr":
return self__.attr;

break;
case "datoms":
return self__.datoms;

break;
default:
return cljs.core.get.cljs$core$IFn$_invoke$arity$3(self__.__extmap,k40609,else__5451__auto__);

}
}));

(datascript.pull_api.MultivalRefAttrFrame.prototype.cljs$core$IKVReduce$_kv_reduce$arity$3 = (function (this__5468__auto__,f__5469__auto__,init__5470__auto__){
var self__ = this;
var this__5468__auto____$1 = this;
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (ret__5471__auto__,p__40616){
var vec__40617 = p__40616;
var k__5472__auto__ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40617,(0),null);
var v__5473__auto__ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40617,(1),null);
return (f__5469__auto__.cljs$core$IFn$_invoke$arity$3 ? f__5469__auto__.cljs$core$IFn$_invoke$arity$3(ret__5471__auto__,k__5472__auto__,v__5473__auto__) : f__5469__auto__.call(null,ret__5471__auto__,k__5472__auto__,v__5473__auto__));
}),init__5470__auto__,this__5468__auto____$1);
}));

(datascript.pull_api.MultivalRefAttrFrame.prototype.cljs$core$IPrintWithWriter$_pr_writer$arity$3 = (function (this__5463__auto__,writer__5464__auto__,opts__5465__auto__){
var self__ = this;
var this__5463__auto____$1 = this;
var pr_pair__5466__auto__ = (function (keyval__5467__auto__){
return cljs.core.pr_sequential_writer(writer__5464__auto__,cljs.core.pr_writer,""," ","",opts__5465__auto__,keyval__5467__auto__);
});
return cljs.core.pr_sequential_writer(writer__5464__auto__,pr_pair__5466__auto__,"#datascript.pull-api.MultivalRefAttrFrame{",", ","}",opts__5465__auto__,cljs.core.concat.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentVector(null, 6, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"seen","seen",-518999789),self__.seen],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814),self__.recursion_limits],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"acc","acc",838566312),self__.acc],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"pattern","pattern",242135423),self__.pattern],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"attr","attr",-604132353),self__.attr],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"datoms","datoms",-290874434),self__.datoms],null))], null),self__.__extmap));
}));

(datascript.pull_api.MultivalRefAttrFrame.prototype.cljs$core$IIterable$_iterator$arity$1 = (function (G__40608){
var self__ = this;
var G__40608__$1 = this;
return (new cljs.core.RecordIter((0),G__40608__$1,6,new cljs.core.PersistentVector(null, 6, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"seen","seen",-518999789),new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814),new cljs.core.Keyword(null,"acc","acc",838566312),new cljs.core.Keyword(null,"pattern","pattern",242135423),new cljs.core.Keyword(null,"attr","attr",-604132353),new cljs.core.Keyword(null,"datoms","datoms",-290874434)], null),(cljs.core.truth_(self__.__extmap)?cljs.core._iterator(self__.__extmap):cljs.core.nil_iter())));
}));

(datascript.pull_api.MultivalRefAttrFrame.prototype.cljs$core$IMeta$_meta$arity$1 = (function (this__5446__auto__){
var self__ = this;
var this__5446__auto____$1 = this;
return self__.__meta;
}));

(datascript.pull_api.MultivalRefAttrFrame.prototype.cljs$core$ICloneable$_clone$arity$1 = (function (this__5443__auto__){
var self__ = this;
var this__5443__auto____$1 = this;
return (new datascript.pull_api.MultivalRefAttrFrame(self__.seen,self__.recursion_limits,self__.acc,self__.pattern,self__.attr,self__.datoms,self__.__meta,self__.__extmap,self__.__hash));
}));

(datascript.pull_api.MultivalRefAttrFrame.prototype.cljs$core$ICounted$_count$arity$1 = (function (this__5452__auto__){
var self__ = this;
var this__5452__auto____$1 = this;
return (6 + cljs.core.count(self__.__extmap));
}));

(datascript.pull_api.MultivalRefAttrFrame.prototype.cljs$core$IHash$_hash$arity$1 = (function (this__5444__auto__){
var self__ = this;
var this__5444__auto____$1 = this;
var h__5251__auto__ = self__.__hash;
if((!((h__5251__auto__ == null)))){
return h__5251__auto__;
} else {
var h__5251__auto____$1 = (function (coll__5445__auto__){
return (1769511936 ^ cljs.core.hash_unordered_coll(coll__5445__auto__));
})(this__5444__auto____$1);
(self__.__hash = h__5251__auto____$1);

return h__5251__auto____$1;
}
}));

(datascript.pull_api.MultivalRefAttrFrame.prototype.cljs$core$IEquiv$_equiv$arity$2 = (function (this40610,other40611){
var self__ = this;
var this40610__$1 = this;
return (((!((other40611 == null)))) && ((((this40610__$1.constructor === other40611.constructor)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40610__$1.seen,other40611.seen)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40610__$1.recursion_limits,other40611.recursion_limits)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40610__$1.acc,other40611.acc)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40610__$1.pattern,other40611.pattern)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40610__$1.attr,other40611.attr)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40610__$1.datoms,other40611.datoms)) && (cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40610__$1.__extmap,other40611.__extmap)))))))))))))))));
}));

(datascript.pull_api.MultivalRefAttrFrame.prototype.cljs$core$IMap$_dissoc$arity$2 = (function (this__5458__auto__,k__5459__auto__){
var self__ = this;
var this__5458__auto____$1 = this;
if(cljs.core.contains_QMARK_(new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 6, [new cljs.core.Keyword(null,"acc","acc",838566312),null,new cljs.core.Keyword(null,"seen","seen",-518999789),null,new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814),null,new cljs.core.Keyword(null,"datoms","datoms",-290874434),null,new cljs.core.Keyword(null,"pattern","pattern",242135423),null,new cljs.core.Keyword(null,"attr","attr",-604132353),null], null), null),k__5459__auto__)){
return cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(cljs.core._with_meta(cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,this__5458__auto____$1),self__.__meta),k__5459__auto__);
} else {
return (new datascript.pull_api.MultivalRefAttrFrame(self__.seen,self__.recursion_limits,self__.acc,self__.pattern,self__.attr,self__.datoms,self__.__meta,cljs.core.not_empty(cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(self__.__extmap,k__5459__auto__)),null));
}
}));

(datascript.pull_api.MultivalRefAttrFrame.prototype.cljs$core$IAssociative$_contains_key_QMARK_$arity$2 = (function (this__5455__auto__,k40609){
var self__ = this;
var this__5455__auto____$1 = this;
var G__40622 = k40609;
var G__40622__$1 = (((G__40622 instanceof cljs.core.Keyword))?G__40622.fqn:null);
switch (G__40622__$1) {
case "seen":
case "recursion-limits":
case "acc":
case "pattern":
case "attr":
case "datoms":
return true;

break;
default:
return cljs.core.contains_QMARK_(self__.__extmap,k40609);

}
}));

(datascript.pull_api.MultivalRefAttrFrame.prototype.cljs$core$IAssociative$_assoc$arity$3 = (function (this__5456__auto__,k__5457__auto__,G__40608){
var self__ = this;
var this__5456__auto____$1 = this;
var pred__40624 = cljs.core.keyword_identical_QMARK_;
var expr__40625 = k__5457__auto__;
if(cljs.core.truth_((pred__40624.cljs$core$IFn$_invoke$arity$2 ? pred__40624.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"seen","seen",-518999789),expr__40625) : pred__40624.call(null,new cljs.core.Keyword(null,"seen","seen",-518999789),expr__40625)))){
return (new datascript.pull_api.MultivalRefAttrFrame(G__40608,self__.recursion_limits,self__.acc,self__.pattern,self__.attr,self__.datoms,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__40624.cljs$core$IFn$_invoke$arity$2 ? pred__40624.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814),expr__40625) : pred__40624.call(null,new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814),expr__40625)))){
return (new datascript.pull_api.MultivalRefAttrFrame(self__.seen,G__40608,self__.acc,self__.pattern,self__.attr,self__.datoms,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__40624.cljs$core$IFn$_invoke$arity$2 ? pred__40624.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"acc","acc",838566312),expr__40625) : pred__40624.call(null,new cljs.core.Keyword(null,"acc","acc",838566312),expr__40625)))){
return (new datascript.pull_api.MultivalRefAttrFrame(self__.seen,self__.recursion_limits,G__40608,self__.pattern,self__.attr,self__.datoms,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__40624.cljs$core$IFn$_invoke$arity$2 ? pred__40624.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"pattern","pattern",242135423),expr__40625) : pred__40624.call(null,new cljs.core.Keyword(null,"pattern","pattern",242135423),expr__40625)))){
return (new datascript.pull_api.MultivalRefAttrFrame(self__.seen,self__.recursion_limits,self__.acc,G__40608,self__.attr,self__.datoms,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__40624.cljs$core$IFn$_invoke$arity$2 ? pred__40624.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"attr","attr",-604132353),expr__40625) : pred__40624.call(null,new cljs.core.Keyword(null,"attr","attr",-604132353),expr__40625)))){
return (new datascript.pull_api.MultivalRefAttrFrame(self__.seen,self__.recursion_limits,self__.acc,self__.pattern,G__40608,self__.datoms,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__40624.cljs$core$IFn$_invoke$arity$2 ? pred__40624.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"datoms","datoms",-290874434),expr__40625) : pred__40624.call(null,new cljs.core.Keyword(null,"datoms","datoms",-290874434),expr__40625)))){
return (new datascript.pull_api.MultivalRefAttrFrame(self__.seen,self__.recursion_limits,self__.acc,self__.pattern,self__.attr,G__40608,self__.__meta,self__.__extmap,null));
} else {
return (new datascript.pull_api.MultivalRefAttrFrame(self__.seen,self__.recursion_limits,self__.acc,self__.pattern,self__.attr,self__.datoms,self__.__meta,cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(self__.__extmap,k__5457__auto__,G__40608),null));
}
}
}
}
}
}
}));

(datascript.pull_api.MultivalRefAttrFrame.prototype.cljs$core$ISeqable$_seq$arity$1 = (function (this__5461__auto__){
var self__ = this;
var this__5461__auto____$1 = this;
return cljs.core.seq(cljs.core.concat.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentVector(null, 6, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new cljs.core.MapEntry(new cljs.core.Keyword(null,"seen","seen",-518999789),self__.seen,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814),self__.recursion_limits,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"acc","acc",838566312),self__.acc,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"pattern","pattern",242135423),self__.pattern,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"attr","attr",-604132353),self__.attr,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"datoms","datoms",-290874434),self__.datoms,null))], null),self__.__extmap));
}));

(datascript.pull_api.MultivalRefAttrFrame.prototype.cljs$core$IWithMeta$_with_meta$arity$2 = (function (this__5447__auto__,G__40608){
var self__ = this;
var this__5447__auto____$1 = this;
return (new datascript.pull_api.MultivalRefAttrFrame(self__.seen,self__.recursion_limits,self__.acc,self__.pattern,self__.attr,self__.datoms,G__40608,self__.__extmap,self__.__hash));
}));

(datascript.pull_api.MultivalRefAttrFrame.prototype.cljs$core$ICollection$_conj$arity$2 = (function (this__5453__auto__,entry__5454__auto__){
var self__ = this;
var this__5453__auto____$1 = this;
if(cljs.core.vector_QMARK_(entry__5454__auto__)){
return this__5453__auto____$1.cljs$core$IAssociative$_assoc$arity$3(null,cljs.core._nth(entry__5454__auto__,(0)),cljs.core._nth(entry__5454__auto__,(1)));
} else {
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(cljs.core._conj,this__5453__auto____$1,entry__5454__auto__);
}
}));

(datascript.pull_api.MultivalRefAttrFrame.prototype.datascript$pull_api$IFrame$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.pull_api.MultivalRefAttrFrame.prototype.datascript$pull_api$IFrame$_merge$arity$2 = (function (this$,result){
var self__ = this;
var this$__$1 = this;
return (new datascript.pull_api.MultivalRefAttrFrame(self__.seen,self__.recursion_limits,datascript.pull_api.conj_some_BANG_(self__.acc,result.value),self__.pattern,self__.attr,datascript.pull_api.next_seq(self__.datoms),null,null,null));
}));

(datascript.pull_api.MultivalRefAttrFrame.prototype.datascript$pull_api$IFrame$_run$arity$2 = (function (this$,context){
var self__ = this;
var this$__$1 = this;
var datom = datascript.pull_api.first_seq(self__.datoms);
if((((datom == null)) || (cljs.core.not_EQ_.cljs$core$IFn$_invoke$arity$2(datom.a,self__.attr.name)))){
return new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new datascript.pull_api.ResultFrame(cljs.core.not_empty(cljs.core.persistent_BANG_(self__.acc)),(function (){var or__5142__auto__ = self__.datoms;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return cljs.core.List.EMPTY;
}
})(),null,null,null))], null);
} else {
if(cljs.core.truth_((function (){var and__5140__auto__ = self__.attr.limit;
if(cljs.core.truth_(and__5140__auto__)){
return (((cljs.core.count(self__.acc) >= self__.attr.limit)) && (true));
} else {
return and__5140__auto__;
}
})())){
var datoms__$1 = self__.datoms;
while(true){
var datom__$1 = datascript.pull_api.first_seq(datoms__$1);
if((((datom__$1 == null)) || (cljs.core.not_EQ_.cljs$core$IFn$_invoke$arity$2(datom__$1.a,self__.attr.name)))){
return new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new datascript.pull_api.ResultFrame(cljs.core.persistent_BANG_(self__.acc),(function (){var or__5142__auto__ = datoms__$1;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return cljs.core.List.EMPTY;
}
})(),null,null,null))], null);
} else {
var G__40955 = datascript.pull_api.next_seq(datoms__$1);
datoms__$1 = G__40955;
continue;
}
break;
}
} else {
var id = (cljs.core.truth_(self__.attr.reverse_QMARK_)?datom.e:datom.v);
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [this$__$1,(datascript.pull_api.ref_frame.cljs$core$IFn$_invoke$arity$6 ? datascript.pull_api.ref_frame.cljs$core$IFn$_invoke$arity$6(context,self__.seen,self__.recursion_limits,self__.pattern,self__.attr,id) : datascript.pull_api.ref_frame.call(null,context,self__.seen,self__.recursion_limits,self__.pattern,self__.attr,id))], null);

}
}
}));

(datascript.pull_api.MultivalRefAttrFrame.prototype.datascript$pull_api$IFrame$_str$arity$1 = (function (this$){
var self__ = this;
var this$__$1 = this;
return (""+"MultivalAttrFrame<attr="+cljs.core.str.cljs$core$IFn$_invoke$arity$1(datascript.pull_api.attr_str(self__.attr))+">");
}));

(datascript.pull_api.MultivalRefAttrFrame.getBasis = (function (){
return new cljs.core.PersistentVector(null, 6, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"seen","seen",1121531738,null),new cljs.core.Symbol(null,"recursion-limits","recursion-limits",-891577955,null),new cljs.core.Symbol(null,"acc","acc",-1815869457,null),new cljs.core.Symbol(null,"pattern","pattern",1882666950,null),cljs.core.with_meta(new cljs.core.Symbol(null,"attr","attr",1036399174,null),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"tag","tag",-1290361223),new cljs.core.Symbol(null,"PullAttr","PullAttr",1557473458,null)], null)),new cljs.core.Symbol(null,"datoms","datoms",1349657093,null)], null);
}));

(datascript.pull_api.MultivalRefAttrFrame.cljs$lang$type = true);

(datascript.pull_api.MultivalRefAttrFrame.cljs$lang$ctorPrSeq = (function (this__5494__auto__){
return (new cljs.core.List(null,"datascript.pull-api/MultivalRefAttrFrame",null,(1),null));
}));

(datascript.pull_api.MultivalRefAttrFrame.cljs$lang$ctorPrWriter = (function (this__5494__auto__,writer__5495__auto__){
return cljs.core._write(writer__5495__auto__,"datascript.pull-api/MultivalRefAttrFrame");
}));

/**
 * Positional factory function for datascript.pull-api/MultivalRefAttrFrame.
 */
datascript.pull_api.__GT_MultivalRefAttrFrame = (function datascript$pull_api$__GT_MultivalRefAttrFrame(seen,recursion_limits,acc,pattern,attr,datoms){
return (new datascript.pull_api.MultivalRefAttrFrame(seen,recursion_limits,acc,pattern,attr,datoms,null,null,null));
});

/**
 * Factory function for datascript.pull-api/MultivalRefAttrFrame, taking a map of keywords to field values.
 */
datascript.pull_api.map__GT_MultivalRefAttrFrame = (function datascript$pull_api$map__GT_MultivalRefAttrFrame(G__40613){
var extmap__5490__auto__ = (function (){var G__40653 = cljs.core.dissoc.cljs$core$IFn$_invoke$arity$variadic(G__40613,new cljs.core.Keyword(null,"seen","seen",-518999789),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814),new cljs.core.Keyword(null,"acc","acc",838566312),new cljs.core.Keyword(null,"pattern","pattern",242135423),new cljs.core.Keyword(null,"attr","attr",-604132353),new cljs.core.Keyword(null,"datoms","datoms",-290874434)], 0));
if(cljs.core.record_QMARK_(G__40613)){
return cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,G__40653);
} else {
return G__40653;
}
})();
return (new datascript.pull_api.MultivalRefAttrFrame(new cljs.core.Keyword(null,"seen","seen",-518999789).cljs$core$IFn$_invoke$arity$1(G__40613),new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814).cljs$core$IFn$_invoke$arity$1(G__40613),new cljs.core.Keyword(null,"acc","acc",838566312).cljs$core$IFn$_invoke$arity$1(G__40613),new cljs.core.Keyword(null,"pattern","pattern",242135423).cljs$core$IFn$_invoke$arity$1(G__40613),new cljs.core.Keyword(null,"attr","attr",-604132353).cljs$core$IFn$_invoke$arity$1(G__40613),new cljs.core.Keyword(null,"datoms","datoms",-290874434).cljs$core$IFn$_invoke$arity$1(G__40613),null,cljs.core.not_empty(extmap__5490__auto__),null));
});


/**
* @constructor
 * @implements {cljs.core.IRecord}
 * @implements {datascript.pull_api.IFrame}
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
datascript.pull_api.AttrsFrame = (function (seen,recursion_limits,acc,pattern,attr,attrs,datoms,id,__meta,__extmap,__hash){
this.seen = seen;
this.recursion_limits = recursion_limits;
this.acc = acc;
this.pattern = pattern;
this.attr = attr;
this.attrs = attrs;
this.datoms = datoms;
this.id = id;
this.__meta = __meta;
this.__extmap = __extmap;
this.__hash = __hash;
this.cljs$lang$protocol_mask$partition0$ = 2230716170;
this.cljs$lang$protocol_mask$partition1$ = 139264;
});
(datascript.pull_api.AttrsFrame.prototype.cljs$core$ILookup$_lookup$arity$2 = (function (this__5448__auto__,k__5449__auto__){
var self__ = this;
var this__5448__auto____$1 = this;
return this__5448__auto____$1.cljs$core$ILookup$_lookup$arity$3(null,k__5449__auto__,null);
}));

(datascript.pull_api.AttrsFrame.prototype.cljs$core$ILookup$_lookup$arity$3 = (function (this__5450__auto__,k40662,else__5451__auto__){
var self__ = this;
var this__5450__auto____$1 = this;
var G__40669 = k40662;
var G__40669__$1 = (((G__40669 instanceof cljs.core.Keyword))?G__40669.fqn:null);
switch (G__40669__$1) {
case "seen":
return self__.seen;

break;
case "recursion-limits":
return self__.recursion_limits;

break;
case "acc":
return self__.acc;

break;
case "pattern":
return self__.pattern;

break;
case "attr":
return self__.attr;

break;
case "attrs":
return self__.attrs;

break;
case "datoms":
return self__.datoms;

break;
case "id":
return self__.id;

break;
default:
return cljs.core.get.cljs$core$IFn$_invoke$arity$3(self__.__extmap,k40662,else__5451__auto__);

}
}));

(datascript.pull_api.AttrsFrame.prototype.cljs$core$IKVReduce$_kv_reduce$arity$3 = (function (this__5468__auto__,f__5469__auto__,init__5470__auto__){
var self__ = this;
var this__5468__auto____$1 = this;
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (ret__5471__auto__,p__40674){
var vec__40675 = p__40674;
var k__5472__auto__ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40675,(0),null);
var v__5473__auto__ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40675,(1),null);
return (f__5469__auto__.cljs$core$IFn$_invoke$arity$3 ? f__5469__auto__.cljs$core$IFn$_invoke$arity$3(ret__5471__auto__,k__5472__auto__,v__5473__auto__) : f__5469__auto__.call(null,ret__5471__auto__,k__5472__auto__,v__5473__auto__));
}),init__5470__auto__,this__5468__auto____$1);
}));

(datascript.pull_api.AttrsFrame.prototype.cljs$core$IPrintWithWriter$_pr_writer$arity$3 = (function (this__5463__auto__,writer__5464__auto__,opts__5465__auto__){
var self__ = this;
var this__5463__auto____$1 = this;
var pr_pair__5466__auto__ = (function (keyval__5467__auto__){
return cljs.core.pr_sequential_writer(writer__5464__auto__,cljs.core.pr_writer,""," ","",opts__5465__auto__,keyval__5467__auto__);
});
return cljs.core.pr_sequential_writer(writer__5464__auto__,pr_pair__5466__auto__,"#datascript.pull-api.AttrsFrame{",", ","}",opts__5465__auto__,cljs.core.concat.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentVector(null, 8, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"seen","seen",-518999789),self__.seen],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814),self__.recursion_limits],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"acc","acc",838566312),self__.acc],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"pattern","pattern",242135423),self__.pattern],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"attr","attr",-604132353),self__.attr],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"attrs","attrs",-2090668713),self__.attrs],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"datoms","datoms",-290874434),self__.datoms],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"id","id",-1388402092),self__.id],null))], null),self__.__extmap));
}));

(datascript.pull_api.AttrsFrame.prototype.cljs$core$IIterable$_iterator$arity$1 = (function (G__40661){
var self__ = this;
var G__40661__$1 = this;
return (new cljs.core.RecordIter((0),G__40661__$1,8,new cljs.core.PersistentVector(null, 8, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"seen","seen",-518999789),new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814),new cljs.core.Keyword(null,"acc","acc",838566312),new cljs.core.Keyword(null,"pattern","pattern",242135423),new cljs.core.Keyword(null,"attr","attr",-604132353),new cljs.core.Keyword(null,"attrs","attrs",-2090668713),new cljs.core.Keyword(null,"datoms","datoms",-290874434),new cljs.core.Keyword(null,"id","id",-1388402092)], null),(cljs.core.truth_(self__.__extmap)?cljs.core._iterator(self__.__extmap):cljs.core.nil_iter())));
}));

(datascript.pull_api.AttrsFrame.prototype.cljs$core$IMeta$_meta$arity$1 = (function (this__5446__auto__){
var self__ = this;
var this__5446__auto____$1 = this;
return self__.__meta;
}));

(datascript.pull_api.AttrsFrame.prototype.cljs$core$ICloneable$_clone$arity$1 = (function (this__5443__auto__){
var self__ = this;
var this__5443__auto____$1 = this;
return (new datascript.pull_api.AttrsFrame(self__.seen,self__.recursion_limits,self__.acc,self__.pattern,self__.attr,self__.attrs,self__.datoms,self__.id,self__.__meta,self__.__extmap,self__.__hash));
}));

(datascript.pull_api.AttrsFrame.prototype.cljs$core$ICounted$_count$arity$1 = (function (this__5452__auto__){
var self__ = this;
var this__5452__auto____$1 = this;
return (8 + cljs.core.count(self__.__extmap));
}));

(datascript.pull_api.AttrsFrame.prototype.cljs$core$IHash$_hash$arity$1 = (function (this__5444__auto__){
var self__ = this;
var this__5444__auto____$1 = this;
var h__5251__auto__ = self__.__hash;
if((!((h__5251__auto__ == null)))){
return h__5251__auto__;
} else {
var h__5251__auto____$1 = (function (coll__5445__auto__){
return (1127179791 ^ cljs.core.hash_unordered_coll(coll__5445__auto__));
})(this__5444__auto____$1);
(self__.__hash = h__5251__auto____$1);

return h__5251__auto____$1;
}
}));

(datascript.pull_api.AttrsFrame.prototype.cljs$core$IEquiv$_equiv$arity$2 = (function (this40663,other40664){
var self__ = this;
var this40663__$1 = this;
return (((!((other40664 == null)))) && ((((this40663__$1.constructor === other40664.constructor)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40663__$1.seen,other40664.seen)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40663__$1.recursion_limits,other40664.recursion_limits)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40663__$1.acc,other40664.acc)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40663__$1.pattern,other40664.pattern)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40663__$1.attr,other40664.attr)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40663__$1.attrs,other40664.attrs)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40663__$1.datoms,other40664.datoms)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40663__$1.id,other40664.id)) && (cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40663__$1.__extmap,other40664.__extmap)))))))))))))))))))));
}));

(datascript.pull_api.AttrsFrame.prototype.cljs$core$IMap$_dissoc$arity$2 = (function (this__5458__auto__,k__5459__auto__){
var self__ = this;
var this__5458__auto____$1 = this;
if(cljs.core.contains_QMARK_(new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 8, [new cljs.core.Keyword(null,"acc","acc",838566312),null,new cljs.core.Keyword(null,"seen","seen",-518999789),null,new cljs.core.Keyword(null,"id","id",-1388402092),null,new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814),null,new cljs.core.Keyword(null,"attrs","attrs",-2090668713),null,new cljs.core.Keyword(null,"datoms","datoms",-290874434),null,new cljs.core.Keyword(null,"pattern","pattern",242135423),null,new cljs.core.Keyword(null,"attr","attr",-604132353),null], null), null),k__5459__auto__)){
return cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(cljs.core._with_meta(cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,this__5458__auto____$1),self__.__meta),k__5459__auto__);
} else {
return (new datascript.pull_api.AttrsFrame(self__.seen,self__.recursion_limits,self__.acc,self__.pattern,self__.attr,self__.attrs,self__.datoms,self__.id,self__.__meta,cljs.core.not_empty(cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(self__.__extmap,k__5459__auto__)),null));
}
}));

(datascript.pull_api.AttrsFrame.prototype.cljs$core$IAssociative$_contains_key_QMARK_$arity$2 = (function (this__5455__auto__,k40662){
var self__ = this;
var this__5455__auto____$1 = this;
var G__40707 = k40662;
var G__40707__$1 = (((G__40707 instanceof cljs.core.Keyword))?G__40707.fqn:null);
switch (G__40707__$1) {
case "seen":
case "recursion-limits":
case "acc":
case "pattern":
case "attr":
case "attrs":
case "datoms":
case "id":
return true;

break;
default:
return cljs.core.contains_QMARK_(self__.__extmap,k40662);

}
}));

(datascript.pull_api.AttrsFrame.prototype.cljs$core$IAssociative$_assoc$arity$3 = (function (this__5456__auto__,k__5457__auto__,G__40661){
var self__ = this;
var this__5456__auto____$1 = this;
var pred__40708 = cljs.core.keyword_identical_QMARK_;
var expr__40709 = k__5457__auto__;
if(cljs.core.truth_((pred__40708.cljs$core$IFn$_invoke$arity$2 ? pred__40708.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"seen","seen",-518999789),expr__40709) : pred__40708.call(null,new cljs.core.Keyword(null,"seen","seen",-518999789),expr__40709)))){
return (new datascript.pull_api.AttrsFrame(G__40661,self__.recursion_limits,self__.acc,self__.pattern,self__.attr,self__.attrs,self__.datoms,self__.id,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__40708.cljs$core$IFn$_invoke$arity$2 ? pred__40708.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814),expr__40709) : pred__40708.call(null,new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814),expr__40709)))){
return (new datascript.pull_api.AttrsFrame(self__.seen,G__40661,self__.acc,self__.pattern,self__.attr,self__.attrs,self__.datoms,self__.id,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__40708.cljs$core$IFn$_invoke$arity$2 ? pred__40708.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"acc","acc",838566312),expr__40709) : pred__40708.call(null,new cljs.core.Keyword(null,"acc","acc",838566312),expr__40709)))){
return (new datascript.pull_api.AttrsFrame(self__.seen,self__.recursion_limits,G__40661,self__.pattern,self__.attr,self__.attrs,self__.datoms,self__.id,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__40708.cljs$core$IFn$_invoke$arity$2 ? pred__40708.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"pattern","pattern",242135423),expr__40709) : pred__40708.call(null,new cljs.core.Keyword(null,"pattern","pattern",242135423),expr__40709)))){
return (new datascript.pull_api.AttrsFrame(self__.seen,self__.recursion_limits,self__.acc,G__40661,self__.attr,self__.attrs,self__.datoms,self__.id,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__40708.cljs$core$IFn$_invoke$arity$2 ? pred__40708.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"attr","attr",-604132353),expr__40709) : pred__40708.call(null,new cljs.core.Keyword(null,"attr","attr",-604132353),expr__40709)))){
return (new datascript.pull_api.AttrsFrame(self__.seen,self__.recursion_limits,self__.acc,self__.pattern,G__40661,self__.attrs,self__.datoms,self__.id,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__40708.cljs$core$IFn$_invoke$arity$2 ? pred__40708.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"attrs","attrs",-2090668713),expr__40709) : pred__40708.call(null,new cljs.core.Keyword(null,"attrs","attrs",-2090668713),expr__40709)))){
return (new datascript.pull_api.AttrsFrame(self__.seen,self__.recursion_limits,self__.acc,self__.pattern,self__.attr,G__40661,self__.datoms,self__.id,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__40708.cljs$core$IFn$_invoke$arity$2 ? pred__40708.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"datoms","datoms",-290874434),expr__40709) : pred__40708.call(null,new cljs.core.Keyword(null,"datoms","datoms",-290874434),expr__40709)))){
return (new datascript.pull_api.AttrsFrame(self__.seen,self__.recursion_limits,self__.acc,self__.pattern,self__.attr,self__.attrs,G__40661,self__.id,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__40708.cljs$core$IFn$_invoke$arity$2 ? pred__40708.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"id","id",-1388402092),expr__40709) : pred__40708.call(null,new cljs.core.Keyword(null,"id","id",-1388402092),expr__40709)))){
return (new datascript.pull_api.AttrsFrame(self__.seen,self__.recursion_limits,self__.acc,self__.pattern,self__.attr,self__.attrs,self__.datoms,G__40661,self__.__meta,self__.__extmap,null));
} else {
return (new datascript.pull_api.AttrsFrame(self__.seen,self__.recursion_limits,self__.acc,self__.pattern,self__.attr,self__.attrs,self__.datoms,self__.id,self__.__meta,cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(self__.__extmap,k__5457__auto__,G__40661),null));
}
}
}
}
}
}
}
}
}));

(datascript.pull_api.AttrsFrame.prototype.cljs$core$ISeqable$_seq$arity$1 = (function (this__5461__auto__){
var self__ = this;
var this__5461__auto____$1 = this;
return cljs.core.seq(cljs.core.concat.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentVector(null, 8, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new cljs.core.MapEntry(new cljs.core.Keyword(null,"seen","seen",-518999789),self__.seen,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814),self__.recursion_limits,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"acc","acc",838566312),self__.acc,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"pattern","pattern",242135423),self__.pattern,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"attr","attr",-604132353),self__.attr,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"attrs","attrs",-2090668713),self__.attrs,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"datoms","datoms",-290874434),self__.datoms,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"id","id",-1388402092),self__.id,null))], null),self__.__extmap));
}));

(datascript.pull_api.AttrsFrame.prototype.cljs$core$IWithMeta$_with_meta$arity$2 = (function (this__5447__auto__,G__40661){
var self__ = this;
var this__5447__auto____$1 = this;
return (new datascript.pull_api.AttrsFrame(self__.seen,self__.recursion_limits,self__.acc,self__.pattern,self__.attr,self__.attrs,self__.datoms,self__.id,G__40661,self__.__extmap,self__.__hash));
}));

(datascript.pull_api.AttrsFrame.prototype.cljs$core$ICollection$_conj$arity$2 = (function (this__5453__auto__,entry__5454__auto__){
var self__ = this;
var this__5453__auto____$1 = this;
if(cljs.core.vector_QMARK_(entry__5454__auto__)){
return this__5453__auto____$1.cljs$core$IAssociative$_assoc$arity$3(null,cljs.core._nth(entry__5454__auto__,(0)),cljs.core._nth(entry__5454__auto__,(1)));
} else {
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(cljs.core._conj,this__5453__auto____$1,entry__5454__auto__);
}
}));

(datascript.pull_api.AttrsFrame.prototype.datascript$pull_api$IFrame$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.pull_api.AttrsFrame.prototype.datascript$pull_api$IFrame$_merge$arity$2 = (function (this$,result){
var self__ = this;
var this$__$1 = this;
return (new datascript.pull_api.AttrsFrame(self__.seen,self__.recursion_limits,datascript.pull_api.assoc_some_BANG_(self__.acc,self__.attr.as,(function (){var G__40716 = result.value;
var fexpr__40715 = self__.attr.xform;
return (fexpr__40715.cljs$core$IFn$_invoke$arity$1 ? fexpr__40715.cljs$core$IFn$_invoke$arity$1(G__40716) : fexpr__40715.call(null,G__40716));
})()),self__.pattern,datascript.pull_api.first_seq(self__.attrs),datascript.pull_api.next_seq(self__.attrs),cljs.core.not_empty((function (){var or__5142__auto__ = result.datoms;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return datascript.pull_api.next_seq(self__.datoms);
}
})()),self__.id,null,null,null));
}));

(datascript.pull_api.AttrsFrame.prototype.datascript$pull_api$IFrame$_run$arity$2 = (function (this$,context){
var self__ = this;
var this$__$1 = this;
var acc__$1 = self__.acc;
var attr__$1 = self__.attr;
var attrs__$1 = self__.attrs;
var datoms__$1 = self__.datoms;
while(true){
if((((datoms__$1 == null)) && ((((attr__$1 == null)) && (true))))){
return new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [(function (){var G__40743 = self__.seen;
var G__40744 = self__.recursion_limits;
var G__40745 = acc__$1;
var G__40746 = self__.pattern;
var G__40747 = datascript.pull_api.first_seq(self__.pattern.reverse_attrs);
var G__40748 = datascript.pull_api.next_seq(self__.pattern.reverse_attrs);
var G__40749 = self__.id;
return (datascript.pull_api.__GT_ReverseAttrsFrame.cljs$core$IFn$_invoke$arity$7 ? datascript.pull_api.__GT_ReverseAttrsFrame.cljs$core$IFn$_invoke$arity$7(G__40743,G__40744,G__40745,G__40746,G__40747,G__40748,G__40749) : datascript.pull_api.__GT_ReverseAttrsFrame.call(null,G__40743,G__40744,G__40745,G__40746,G__40747,G__40748,G__40749));
})()], null);
} else {
if((((!((attr__$1 == null)))) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db","id","db/id",-1388397098),attr__$1.name)) && (true))))){
var G__41038 = cljs.core.assoc_BANG_.cljs$core$IFn$_invoke$arity$3(acc__$1,attr__$1.as,(function (){var fexpr__40750 = attr__$1.xform;
return (fexpr__40750.cljs$core$IFn$_invoke$arity$1 ? fexpr__40750.cljs$core$IFn$_invoke$arity$1(self__.id) : fexpr__40750.call(null,self__.id));
})());
var G__41039 = datascript.pull_api.first_seq(attrs__$1);
var G__41040 = datascript.pull_api.next_seq(attrs__$1);
var G__41041 = datoms__$1;
acc__$1 = G__41038;
attr__$1 = G__41039;
attrs__$1 = G__41040;
datoms__$1 = G__41041;
continue;
} else {
var datom = datascript.pull_api.first_seq(datoms__$1);
var cmp = (cljs.core.truth_((function (){var and__5140__auto__ = datom;
if(cljs.core.truth_(and__5140__auto__)){
return attr__$1;
} else {
return and__5140__auto__;
}
})())?cljs.core.compare(attr__$1.name,datom.a):null);
var attr_ahead_QMARK_ = (function (){var or__5142__auto__ = (attr__$1 == null);
if(or__5142__auto__){
return or__5142__auto__;
} else {
var and__5140__auto__ = cmp;
if(cljs.core.truth_(and__5140__auto__)){
return (cmp > (0));
} else {
return and__5140__auto__;
}
}
})();
var datom_ahead_QMARK_ = (function (){var or__5142__auto__ = (datom == null);
if(or__5142__auto__){
return or__5142__auto__;
} else {
var and__5140__auto__ = cmp;
if(cljs.core.truth_(and__5140__auto__)){
return (cmp < (0));
} else {
return and__5140__auto__;
}
}
})();
if(cljs.core.truth_((function (){var and__5140__auto__ = self__.pattern.wildcard_QMARK_;
if(cljs.core.truth_(and__5140__auto__)){
var and__5140__auto____$1 = (!((datom == null)));
if(and__5140__auto____$1){
var and__5140__auto____$2 = attr_ahead_QMARK_;
if(cljs.core.truth_(and__5140__auto____$2)){
return true;
} else {
return and__5140__auto____$2;
}
} else {
return and__5140__auto____$1;
}
} else {
return and__5140__auto__;
}
})())){
var datom_attr = datascript.lru._get(datascript.db.unfiltered_db(context.db).pull_attrs,datom.a,((function (acc__$1,attr__$1,attrs__$1,datoms__$1,datom,cmp,attr_ahead_QMARK_,datom_ahead_QMARK_,this$__$1){
return (function (){
return datascript.pull_parser.parse_attr_name(context.db,datom.a);
});})(acc__$1,attr__$1,attrs__$1,datoms__$1,datom,cmp,attr_ahead_QMARK_,datom_ahead_QMARK_,this$__$1))
);
var G__41065 = acc__$1;
var G__41066 = datom_attr;
var G__41067 = (cljs.core.truth_(attr__$1)?datascript.pull_api.conj_seq(attrs__$1,attr__$1):null);
var G__41068 = datoms__$1;
acc__$1 = G__41065;
attr__$1 = G__41066;
attrs__$1 = G__41067;
datoms__$1 = G__41068;
continue;
} else {
if(cljs.core.truth_(attr_ahead_QMARK_)){
var G__41074 = acc__$1;
var G__41075 = attr__$1;
var G__41076 = attrs__$1;
var G__41077 = datascript.pull_api.next_seq(datoms__$1);
acc__$1 = G__41074;
attr__$1 = G__41075;
attrs__$1 = G__41076;
datoms__$1 = G__41077;
continue;
} else {
datascript.pull_api.visit(context,new cljs.core.Keyword("db.pull","attr","db.pull/attr",-533298746),self__.id,attr__$1.name,null);

if(cljs.core.truth_((function (){var and__5140__auto__ = datom_ahead_QMARK_;
if(cljs.core.truth_(and__5140__auto__)){
return (((attr__$1 == null)) && (true));
} else {
return and__5140__auto__;
}
})())){
var G__41079 = acc__$1;
var G__41080 = datascript.pull_api.first_seq(attrs__$1);
var G__41081 = datascript.pull_api.next_seq(attrs__$1);
var G__41082 = datoms__$1;
acc__$1 = G__41079;
attr__$1 = G__41080;
attrs__$1 = G__41081;
datoms__$1 = G__41082;
continue;
} else {
if(cljs.core.truth_((function (){var and__5140__auto__ = datom_ahead_QMARK_;
if(cljs.core.truth_(and__5140__auto__)){
return (((!((new cljs.core.Keyword(null,"default","default",-1987822328).cljs$core$IFn$_invoke$arity$1(attr__$1) == null)))) && (true));
} else {
return and__5140__auto__;
}
})())){
var G__41088 = cljs.core.assoc_BANG_.cljs$core$IFn$_invoke$arity$3(acc__$1,attr__$1.as,new cljs.core.Keyword(null,"default","default",-1987822328).cljs$core$IFn$_invoke$arity$1(attr__$1));
var G__41089 = datascript.pull_api.first_seq(attrs__$1);
var G__41090 = datascript.pull_api.next_seq(attrs__$1);
var G__41091 = datoms__$1;
acc__$1 = G__41088;
attr__$1 = G__41089;
attrs__$1 = G__41090;
datoms__$1 = G__41091;
continue;
} else {
if(cljs.core.truth_(datom_ahead_QMARK_)){
var temp__5806__auto__ = (function (){var fexpr__40751 = attr__$1.xform;
return (fexpr__40751.cljs$core$IFn$_invoke$arity$1 ? fexpr__40751.cljs$core$IFn$_invoke$arity$1(null) : fexpr__40751.call(null,null));
})();
if((temp__5806__auto__ == null)){
var G__41095 = acc__$1;
var G__41096 = datascript.pull_api.first_seq(attrs__$1);
var G__41097 = datascript.pull_api.next_seq(attrs__$1);
var G__41098 = datoms__$1;
acc__$1 = G__41095;
attr__$1 = G__41096;
attrs__$1 = G__41097;
datoms__$1 = G__41098;
continue;
} else {
var value = temp__5806__auto__;
var G__41105 = cljs.core.assoc_BANG_.cljs$core$IFn$_invoke$arity$3(acc__$1,attr__$1.as,value);
var G__41106 = datascript.pull_api.first_seq(attrs__$1);
var G__41107 = datascript.pull_api.next_seq(attrs__$1);
var G__41108 = datoms__$1;
acc__$1 = G__41105;
attr__$1 = G__41106;
attrs__$1 = G__41107;
datoms__$1 = G__41108;
continue;
}
} else {
if(cljs.core.truth_((function (){var and__5140__auto__ = attr__$1.multival_QMARK_;
if(cljs.core.truth_(and__5140__auto__)){
var and__5140__auto____$1 = attr__$1.ref_QMARK_;
if(cljs.core.truth_(and__5140__auto____$1)){
return true;
} else {
return and__5140__auto____$1;
}
} else {
return and__5140__auto__;
}
})())){
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new datascript.pull_api.AttrsFrame(self__.seen,self__.recursion_limits,acc__$1,self__.pattern,attr__$1,attrs__$1,datoms__$1,self__.id,null,null,null)),(new datascript.pull_api.MultivalRefAttrFrame(self__.seen,self__.recursion_limits,cljs.core.transient$(cljs.core.PersistentVector.EMPTY),self__.pattern,attr__$1,datoms__$1,null,null,null))], null);
} else {
if(cljs.core.truth_(attr__$1.multival_QMARK_)){
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new datascript.pull_api.AttrsFrame(self__.seen,self__.recursion_limits,acc__$1,self__.pattern,attr__$1,attrs__$1,datoms__$1,self__.id,null,null,null)),(new datascript.pull_api.MultivalAttrFrame(cljs.core.transient$(cljs.core.PersistentVector.EMPTY),attr__$1,datoms__$1,null,null,null))], null);
} else {
if(cljs.core.truth_(attr__$1.ref_QMARK_)){
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new datascript.pull_api.AttrsFrame(self__.seen,self__.recursion_limits,acc__$1,self__.pattern,attr__$1,attrs__$1,datoms__$1,self__.id,null,null,null)),(function (){var G__40758 = context;
var G__40759 = self__.seen;
var G__40760 = self__.recursion_limits;
var G__40761 = self__.pattern;
var G__40762 = attr__$1;
var G__40763 = datom.v;
return (datascript.pull_api.ref_frame.cljs$core$IFn$_invoke$arity$6 ? datascript.pull_api.ref_frame.cljs$core$IFn$_invoke$arity$6(G__40758,G__40759,G__40760,G__40761,G__40762,G__40763) : datascript.pull_api.ref_frame.call(null,G__40758,G__40759,G__40760,G__40761,G__40762,G__40763));
})()], null);
} else {
var G__41125 = cljs.core.assoc_BANG_.cljs$core$IFn$_invoke$arity$3(acc__$1,attr__$1.as,(function (){var G__40765 = datom.v;
var fexpr__40764 = attr__$1.xform;
return (fexpr__40764.cljs$core$IFn$_invoke$arity$1 ? fexpr__40764.cljs$core$IFn$_invoke$arity$1(G__40765) : fexpr__40764.call(null,G__40765));
})());
var G__41126 = datascript.pull_api.first_seq(attrs__$1);
var G__41127 = datascript.pull_api.next_seq(attrs__$1);
var G__41128 = datascript.pull_api.next_seq(datoms__$1);
acc__$1 = G__41125;
attr__$1 = G__41126;
attrs__$1 = G__41127;
datoms__$1 = G__41128;
continue;

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

(datascript.pull_api.AttrsFrame.prototype.datascript$pull_api$IFrame$_str$arity$1 = (function (this$){
var self__ = this;
var this$__$1 = this;
return (""+"AttrsFrame<id="+cljs.core.str.cljs$core$IFn$_invoke$arity$1(self__.id)+", attr="+cljs.core.str.cljs$core$IFn$_invoke$arity$1(datascript.pull_api.attr_str(self__.attr))+", attrs="+cljs.core.str.cljs$core$IFn$_invoke$arity$1(clojure.string.join.cljs$core$IFn$_invoke$arity$2(" ",cljs.core.map.cljs$core$IFn$_invoke$arity$2(datascript.pull_api.attr_str,self__.attrs)))+">");
}));

(datascript.pull_api.AttrsFrame.getBasis = (function (){
return new cljs.core.PersistentVector(null, 8, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"seen","seen",1121531738,null),new cljs.core.Symbol(null,"recursion-limits","recursion-limits",-891577955,null),new cljs.core.Symbol(null,"acc","acc",-1815869457,null),cljs.core.with_meta(new cljs.core.Symbol(null,"pattern","pattern",1882666950,null),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"tag","tag",-1290361223),new cljs.core.Symbol(null,"PullPattern","PullPattern",1378528189,null)], null)),cljs.core.with_meta(new cljs.core.Symbol(null,"attr","attr",1036399174,null),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"tag","tag",-1290361223),new cljs.core.Symbol(null,"PullAttr","PullAttr",1557473458,null)], null)),new cljs.core.Symbol(null,"attrs","attrs",-450137186,null),new cljs.core.Symbol(null,"datoms","datoms",1349657093,null),new cljs.core.Symbol(null,"id","id",252129435,null)], null);
}));

(datascript.pull_api.AttrsFrame.cljs$lang$type = true);

(datascript.pull_api.AttrsFrame.cljs$lang$ctorPrSeq = (function (this__5494__auto__){
return (new cljs.core.List(null,"datascript.pull-api/AttrsFrame",null,(1),null));
}));

(datascript.pull_api.AttrsFrame.cljs$lang$ctorPrWriter = (function (this__5494__auto__,writer__5495__auto__){
return cljs.core._write(writer__5495__auto__,"datascript.pull-api/AttrsFrame");
}));

/**
 * Positional factory function for datascript.pull-api/AttrsFrame.
 */
datascript.pull_api.__GT_AttrsFrame = (function datascript$pull_api$__GT_AttrsFrame(seen,recursion_limits,acc,pattern,attr,attrs,datoms,id){
return (new datascript.pull_api.AttrsFrame(seen,recursion_limits,acc,pattern,attr,attrs,datoms,id,null,null,null));
});

/**
 * Factory function for datascript.pull-api/AttrsFrame, taking a map of keywords to field values.
 */
datascript.pull_api.map__GT_AttrsFrame = (function datascript$pull_api$map__GT_AttrsFrame(G__40665){
var extmap__5490__auto__ = (function (){var G__40774 = cljs.core.dissoc.cljs$core$IFn$_invoke$arity$variadic(G__40665,new cljs.core.Keyword(null,"seen","seen",-518999789),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814),new cljs.core.Keyword(null,"acc","acc",838566312),new cljs.core.Keyword(null,"pattern","pattern",242135423),new cljs.core.Keyword(null,"attr","attr",-604132353),new cljs.core.Keyword(null,"attrs","attrs",-2090668713),new cljs.core.Keyword(null,"datoms","datoms",-290874434),new cljs.core.Keyword(null,"id","id",-1388402092)], 0));
if(cljs.core.record_QMARK_(G__40665)){
return cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,G__40774);
} else {
return G__40774;
}
})();
return (new datascript.pull_api.AttrsFrame(new cljs.core.Keyword(null,"seen","seen",-518999789).cljs$core$IFn$_invoke$arity$1(G__40665),new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814).cljs$core$IFn$_invoke$arity$1(G__40665),new cljs.core.Keyword(null,"acc","acc",838566312).cljs$core$IFn$_invoke$arity$1(G__40665),new cljs.core.Keyword(null,"pattern","pattern",242135423).cljs$core$IFn$_invoke$arity$1(G__40665),new cljs.core.Keyword(null,"attr","attr",-604132353).cljs$core$IFn$_invoke$arity$1(G__40665),new cljs.core.Keyword(null,"attrs","attrs",-2090668713).cljs$core$IFn$_invoke$arity$1(G__40665),new cljs.core.Keyword(null,"datoms","datoms",-290874434).cljs$core$IFn$_invoke$arity$1(G__40665),new cljs.core.Keyword(null,"id","id",-1388402092).cljs$core$IFn$_invoke$arity$1(G__40665),null,cljs.core.not_empty(extmap__5490__auto__),null));
});


/**
* @constructor
 * @implements {cljs.core.IRecord}
 * @implements {datascript.pull_api.IFrame}
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
datascript.pull_api.ReverseAttrsFrame = (function (seen,recursion_limits,acc,pattern,attr,attrs,id,__meta,__extmap,__hash){
this.seen = seen;
this.recursion_limits = recursion_limits;
this.acc = acc;
this.pattern = pattern;
this.attr = attr;
this.attrs = attrs;
this.id = id;
this.__meta = __meta;
this.__extmap = __extmap;
this.__hash = __hash;
this.cljs$lang$protocol_mask$partition0$ = 2230716170;
this.cljs$lang$protocol_mask$partition1$ = 139264;
});
(datascript.pull_api.ReverseAttrsFrame.prototype.cljs$core$ILookup$_lookup$arity$2 = (function (this__5448__auto__,k__5449__auto__){
var self__ = this;
var this__5448__auto____$1 = this;
return this__5448__auto____$1.cljs$core$ILookup$_lookup$arity$3(null,k__5449__auto__,null);
}));

(datascript.pull_api.ReverseAttrsFrame.prototype.cljs$core$ILookup$_lookup$arity$3 = (function (this__5450__auto__,k40783,else__5451__auto__){
var self__ = this;
var this__5450__auto____$1 = this;
var G__40793 = k40783;
var G__40793__$1 = (((G__40793 instanceof cljs.core.Keyword))?G__40793.fqn:null);
switch (G__40793__$1) {
case "seen":
return self__.seen;

break;
case "recursion-limits":
return self__.recursion_limits;

break;
case "acc":
return self__.acc;

break;
case "pattern":
return self__.pattern;

break;
case "attr":
return self__.attr;

break;
case "attrs":
return self__.attrs;

break;
case "id":
return self__.id;

break;
default:
return cljs.core.get.cljs$core$IFn$_invoke$arity$3(self__.__extmap,k40783,else__5451__auto__);

}
}));

(datascript.pull_api.ReverseAttrsFrame.prototype.cljs$core$IKVReduce$_kv_reduce$arity$3 = (function (this__5468__auto__,f__5469__auto__,init__5470__auto__){
var self__ = this;
var this__5468__auto____$1 = this;
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (ret__5471__auto__,p__40795){
var vec__40796 = p__40795;
var k__5472__auto__ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40796,(0),null);
var v__5473__auto__ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40796,(1),null);
return (f__5469__auto__.cljs$core$IFn$_invoke$arity$3 ? f__5469__auto__.cljs$core$IFn$_invoke$arity$3(ret__5471__auto__,k__5472__auto__,v__5473__auto__) : f__5469__auto__.call(null,ret__5471__auto__,k__5472__auto__,v__5473__auto__));
}),init__5470__auto__,this__5468__auto____$1);
}));

(datascript.pull_api.ReverseAttrsFrame.prototype.cljs$core$IPrintWithWriter$_pr_writer$arity$3 = (function (this__5463__auto__,writer__5464__auto__,opts__5465__auto__){
var self__ = this;
var this__5463__auto____$1 = this;
var pr_pair__5466__auto__ = (function (keyval__5467__auto__){
return cljs.core.pr_sequential_writer(writer__5464__auto__,cljs.core.pr_writer,""," ","",opts__5465__auto__,keyval__5467__auto__);
});
return cljs.core.pr_sequential_writer(writer__5464__auto__,pr_pair__5466__auto__,"#datascript.pull-api.ReverseAttrsFrame{",", ","}",opts__5465__auto__,cljs.core.concat.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentVector(null, 7, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"seen","seen",-518999789),self__.seen],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814),self__.recursion_limits],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"acc","acc",838566312),self__.acc],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"pattern","pattern",242135423),self__.pattern],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"attr","attr",-604132353),self__.attr],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"attrs","attrs",-2090668713),self__.attrs],null)),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[new cljs.core.Keyword(null,"id","id",-1388402092),self__.id],null))], null),self__.__extmap));
}));

(datascript.pull_api.ReverseAttrsFrame.prototype.cljs$core$IIterable$_iterator$arity$1 = (function (G__40782){
var self__ = this;
var G__40782__$1 = this;
return (new cljs.core.RecordIter((0),G__40782__$1,7,new cljs.core.PersistentVector(null, 7, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"seen","seen",-518999789),new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814),new cljs.core.Keyword(null,"acc","acc",838566312),new cljs.core.Keyword(null,"pattern","pattern",242135423),new cljs.core.Keyword(null,"attr","attr",-604132353),new cljs.core.Keyword(null,"attrs","attrs",-2090668713),new cljs.core.Keyword(null,"id","id",-1388402092)], null),(cljs.core.truth_(self__.__extmap)?cljs.core._iterator(self__.__extmap):cljs.core.nil_iter())));
}));

(datascript.pull_api.ReverseAttrsFrame.prototype.cljs$core$IMeta$_meta$arity$1 = (function (this__5446__auto__){
var self__ = this;
var this__5446__auto____$1 = this;
return self__.__meta;
}));

(datascript.pull_api.ReverseAttrsFrame.prototype.cljs$core$ICloneable$_clone$arity$1 = (function (this__5443__auto__){
var self__ = this;
var this__5443__auto____$1 = this;
return (new datascript.pull_api.ReverseAttrsFrame(self__.seen,self__.recursion_limits,self__.acc,self__.pattern,self__.attr,self__.attrs,self__.id,self__.__meta,self__.__extmap,self__.__hash));
}));

(datascript.pull_api.ReverseAttrsFrame.prototype.cljs$core$ICounted$_count$arity$1 = (function (this__5452__auto__){
var self__ = this;
var this__5452__auto____$1 = this;
return (7 + cljs.core.count(self__.__extmap));
}));

(datascript.pull_api.ReverseAttrsFrame.prototype.cljs$core$IHash$_hash$arity$1 = (function (this__5444__auto__){
var self__ = this;
var this__5444__auto____$1 = this;
var h__5251__auto__ = self__.__hash;
if((!((h__5251__auto__ == null)))){
return h__5251__auto__;
} else {
var h__5251__auto____$1 = (function (coll__5445__auto__){
return (470210558 ^ cljs.core.hash_unordered_coll(coll__5445__auto__));
})(this__5444__auto____$1);
(self__.__hash = h__5251__auto____$1);

return h__5251__auto____$1;
}
}));

(datascript.pull_api.ReverseAttrsFrame.prototype.cljs$core$IEquiv$_equiv$arity$2 = (function (this40784,other40785){
var self__ = this;
var this40784__$1 = this;
return (((!((other40785 == null)))) && ((((this40784__$1.constructor === other40785.constructor)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40784__$1.seen,other40785.seen)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40784__$1.recursion_limits,other40785.recursion_limits)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40784__$1.acc,other40785.acc)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40784__$1.pattern,other40785.pattern)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40784__$1.attr,other40785.attr)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40784__$1.attrs,other40785.attrs)) && (((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40784__$1.id,other40785.id)) && (cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(this40784__$1.__extmap,other40785.__extmap)))))))))))))))))));
}));

(datascript.pull_api.ReverseAttrsFrame.prototype.cljs$core$IMap$_dissoc$arity$2 = (function (this__5458__auto__,k__5459__auto__){
var self__ = this;
var this__5458__auto____$1 = this;
if(cljs.core.contains_QMARK_(new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 7, [new cljs.core.Keyword(null,"acc","acc",838566312),null,new cljs.core.Keyword(null,"seen","seen",-518999789),null,new cljs.core.Keyword(null,"id","id",-1388402092),null,new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814),null,new cljs.core.Keyword(null,"attrs","attrs",-2090668713),null,new cljs.core.Keyword(null,"pattern","pattern",242135423),null,new cljs.core.Keyword(null,"attr","attr",-604132353),null], null), null),k__5459__auto__)){
return cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(cljs.core._with_meta(cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,this__5458__auto____$1),self__.__meta),k__5459__auto__);
} else {
return (new datascript.pull_api.ReverseAttrsFrame(self__.seen,self__.recursion_limits,self__.acc,self__.pattern,self__.attr,self__.attrs,self__.id,self__.__meta,cljs.core.not_empty(cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(self__.__extmap,k__5459__auto__)),null));
}
}));

(datascript.pull_api.ReverseAttrsFrame.prototype.cljs$core$IAssociative$_contains_key_QMARK_$arity$2 = (function (this__5455__auto__,k40783){
var self__ = this;
var this__5455__auto____$1 = this;
var G__40799 = k40783;
var G__40799__$1 = (((G__40799 instanceof cljs.core.Keyword))?G__40799.fqn:null);
switch (G__40799__$1) {
case "seen":
case "recursion-limits":
case "acc":
case "pattern":
case "attr":
case "attrs":
case "id":
return true;

break;
default:
return cljs.core.contains_QMARK_(self__.__extmap,k40783);

}
}));

(datascript.pull_api.ReverseAttrsFrame.prototype.cljs$core$IAssociative$_assoc$arity$3 = (function (this__5456__auto__,k__5457__auto__,G__40782){
var self__ = this;
var this__5456__auto____$1 = this;
var pred__40800 = cljs.core.keyword_identical_QMARK_;
var expr__40801 = k__5457__auto__;
if(cljs.core.truth_((pred__40800.cljs$core$IFn$_invoke$arity$2 ? pred__40800.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"seen","seen",-518999789),expr__40801) : pred__40800.call(null,new cljs.core.Keyword(null,"seen","seen",-518999789),expr__40801)))){
return (new datascript.pull_api.ReverseAttrsFrame(G__40782,self__.recursion_limits,self__.acc,self__.pattern,self__.attr,self__.attrs,self__.id,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__40800.cljs$core$IFn$_invoke$arity$2 ? pred__40800.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814),expr__40801) : pred__40800.call(null,new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814),expr__40801)))){
return (new datascript.pull_api.ReverseAttrsFrame(self__.seen,G__40782,self__.acc,self__.pattern,self__.attr,self__.attrs,self__.id,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__40800.cljs$core$IFn$_invoke$arity$2 ? pred__40800.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"acc","acc",838566312),expr__40801) : pred__40800.call(null,new cljs.core.Keyword(null,"acc","acc",838566312),expr__40801)))){
return (new datascript.pull_api.ReverseAttrsFrame(self__.seen,self__.recursion_limits,G__40782,self__.pattern,self__.attr,self__.attrs,self__.id,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__40800.cljs$core$IFn$_invoke$arity$2 ? pred__40800.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"pattern","pattern",242135423),expr__40801) : pred__40800.call(null,new cljs.core.Keyword(null,"pattern","pattern",242135423),expr__40801)))){
return (new datascript.pull_api.ReverseAttrsFrame(self__.seen,self__.recursion_limits,self__.acc,G__40782,self__.attr,self__.attrs,self__.id,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__40800.cljs$core$IFn$_invoke$arity$2 ? pred__40800.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"attr","attr",-604132353),expr__40801) : pred__40800.call(null,new cljs.core.Keyword(null,"attr","attr",-604132353),expr__40801)))){
return (new datascript.pull_api.ReverseAttrsFrame(self__.seen,self__.recursion_limits,self__.acc,self__.pattern,G__40782,self__.attrs,self__.id,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__40800.cljs$core$IFn$_invoke$arity$2 ? pred__40800.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"attrs","attrs",-2090668713),expr__40801) : pred__40800.call(null,new cljs.core.Keyword(null,"attrs","attrs",-2090668713),expr__40801)))){
return (new datascript.pull_api.ReverseAttrsFrame(self__.seen,self__.recursion_limits,self__.acc,self__.pattern,self__.attr,G__40782,self__.id,self__.__meta,self__.__extmap,null));
} else {
if(cljs.core.truth_((pred__40800.cljs$core$IFn$_invoke$arity$2 ? pred__40800.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"id","id",-1388402092),expr__40801) : pred__40800.call(null,new cljs.core.Keyword(null,"id","id",-1388402092),expr__40801)))){
return (new datascript.pull_api.ReverseAttrsFrame(self__.seen,self__.recursion_limits,self__.acc,self__.pattern,self__.attr,self__.attrs,G__40782,self__.__meta,self__.__extmap,null));
} else {
return (new datascript.pull_api.ReverseAttrsFrame(self__.seen,self__.recursion_limits,self__.acc,self__.pattern,self__.attr,self__.attrs,self__.id,self__.__meta,cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(self__.__extmap,k__5457__auto__,G__40782),null));
}
}
}
}
}
}
}
}));

(datascript.pull_api.ReverseAttrsFrame.prototype.cljs$core$ISeqable$_seq$arity$1 = (function (this__5461__auto__){
var self__ = this;
var this__5461__auto____$1 = this;
return cljs.core.seq(cljs.core.concat.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentVector(null, 7, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new cljs.core.MapEntry(new cljs.core.Keyword(null,"seen","seen",-518999789),self__.seen,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814),self__.recursion_limits,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"acc","acc",838566312),self__.acc,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"pattern","pattern",242135423),self__.pattern,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"attr","attr",-604132353),self__.attr,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"attrs","attrs",-2090668713),self__.attrs,null)),(new cljs.core.MapEntry(new cljs.core.Keyword(null,"id","id",-1388402092),self__.id,null))], null),self__.__extmap));
}));

(datascript.pull_api.ReverseAttrsFrame.prototype.cljs$core$IWithMeta$_with_meta$arity$2 = (function (this__5447__auto__,G__40782){
var self__ = this;
var this__5447__auto____$1 = this;
return (new datascript.pull_api.ReverseAttrsFrame(self__.seen,self__.recursion_limits,self__.acc,self__.pattern,self__.attr,self__.attrs,self__.id,G__40782,self__.__extmap,self__.__hash));
}));

(datascript.pull_api.ReverseAttrsFrame.prototype.cljs$core$ICollection$_conj$arity$2 = (function (this__5453__auto__,entry__5454__auto__){
var self__ = this;
var this__5453__auto____$1 = this;
if(cljs.core.vector_QMARK_(entry__5454__auto__)){
return this__5453__auto____$1.cljs$core$IAssociative$_assoc$arity$3(null,cljs.core._nth(entry__5454__auto__,(0)),cljs.core._nth(entry__5454__auto__,(1)));
} else {
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(cljs.core._conj,this__5453__auto____$1,entry__5454__auto__);
}
}));

(datascript.pull_api.ReverseAttrsFrame.prototype.datascript$pull_api$IFrame$ = cljs.core.PROTOCOL_SENTINEL);

(datascript.pull_api.ReverseAttrsFrame.prototype.datascript$pull_api$IFrame$_merge$arity$2 = (function (this$,result){
var self__ = this;
var this$__$1 = this;
return (new datascript.pull_api.ReverseAttrsFrame(self__.seen,self__.recursion_limits,datascript.pull_api.assoc_some_BANG_(self__.acc,self__.attr.as,(function (){var G__40805 = result.value;
var fexpr__40804 = self__.attr.xform;
return (fexpr__40804.cljs$core$IFn$_invoke$arity$1 ? fexpr__40804.cljs$core$IFn$_invoke$arity$1(G__40805) : fexpr__40804.call(null,G__40805));
})()),self__.pattern,datascript.pull_api.first_seq(self__.attrs),datascript.pull_api.next_seq(self__.attrs),self__.id,null,null,null));
}));

(datascript.pull_api.ReverseAttrsFrame.prototype.datascript$pull_api$IFrame$_run$arity$2 = (function (this$,context){
var self__ = this;
var this$__$1 = this;
var acc__$1 = self__.acc;
var attr__$1 = self__.attr;
var attrs__$1 = self__.attrs;
while(true){
if((attr__$1 == null)){
return new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new datascript.pull_api.ResultFrame(cljs.core.not_empty(cljs.core.persistent_BANG_(acc__$1)),null,null,null,null))], null);
} else {
var name = attr__$1.name;
var db = context.db;
var datoms = (((db instanceof datascript.db.DB))?me.tonsky.persistent_sorted_set.slice.cljs$core$IFn$_invoke$arity$3(db.avet,datascript.db.datom.cljs$core$IFn$_invoke$arity$4((0),name,self__.id,(536870912)),datascript.db.datom.cljs$core$IFn$_invoke$arity$4((2147483647),name,self__.id,(2147483647))):datascript.db._search(db,new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [null,name,self__.id], null)));
datascript.pull_api.visit(context,new cljs.core.Keyword("db.pull","reverse","db.pull/reverse",1999788297),null,name,self__.id);

if(((cljs.core.empty_QMARK_(datoms)) && ((((!((new cljs.core.Keyword(null,"default","default",-1987822328).cljs$core$IFn$_invoke$arity$1(attr__$1) == null)))) && (true))))){
var G__41200 = cljs.core.assoc_BANG_.cljs$core$IFn$_invoke$arity$3(acc__$1,attr__$1.as,new cljs.core.Keyword(null,"default","default",-1987822328).cljs$core$IFn$_invoke$arity$1(attr__$1));
var G__41201 = datascript.pull_api.first_seq(attrs__$1);
var G__41202 = datascript.pull_api.next_seq(attrs__$1);
acc__$1 = G__41200;
attr__$1 = G__41201;
attrs__$1 = G__41202;
continue;
} else {
if(cljs.core.empty_QMARK_(datoms)){
var G__41205 = acc__$1;
var G__41206 = datascript.pull_api.first_seq(attrs__$1);
var G__41207 = datascript.pull_api.next_seq(attrs__$1);
acc__$1 = G__41205;
attr__$1 = G__41206;
attrs__$1 = G__41207;
continue;
} else {
if(cljs.core.truth_(attr__$1.component_QMARK_)){
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new datascript.pull_api.ReverseAttrsFrame(self__.seen,self__.recursion_limits,acc__$1,self__.pattern,attr__$1,attrs__$1,self__.id,null,null,null)),(function (){var G__40813 = context;
var G__40814 = self__.seen;
var G__40815 = self__.recursion_limits;
var G__40816 = self__.pattern;
var G__40817 = attr__$1;
var G__40818 = datascript.pull_api.first_seq(datoms).e;
return (datascript.pull_api.ref_frame.cljs$core$IFn$_invoke$arity$6 ? datascript.pull_api.ref_frame.cljs$core$IFn$_invoke$arity$6(G__40813,G__40814,G__40815,G__40816,G__40817,G__40818) : datascript.pull_api.ref_frame.call(null,G__40813,G__40814,G__40815,G__40816,G__40817,G__40818));
})()], null);
} else {
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new datascript.pull_api.ReverseAttrsFrame(self__.seen,self__.recursion_limits,acc__$1,self__.pattern,attr__$1,attrs__$1,self__.id,null,null,null)),(new datascript.pull_api.MultivalRefAttrFrame(self__.seen,self__.recursion_limits,cljs.core.transient$(cljs.core.PersistentVector.EMPTY),self__.pattern,attr__$1,datoms,null,null,null))], null);

}
}
}
}
break;
}
}));

(datascript.pull_api.ReverseAttrsFrame.prototype.datascript$pull_api$IFrame$_str$arity$1 = (function (this$){
var self__ = this;
var this$__$1 = this;
return (""+"ReverseAttrsFrame<id="+cljs.core.str.cljs$core$IFn$_invoke$arity$1(self__.id)+", attr="+cljs.core.str.cljs$core$IFn$_invoke$arity$1(datascript.pull_api.attr_str(self__.attr))+", attrs="+cljs.core.str.cljs$core$IFn$_invoke$arity$1(clojure.string.join.cljs$core$IFn$_invoke$arity$2(" ",cljs.core.map.cljs$core$IFn$_invoke$arity$2(datascript.pull_api.attr_str,self__.attrs)))+">");
}));

(datascript.pull_api.ReverseAttrsFrame.getBasis = (function (){
return new cljs.core.PersistentVector(null, 7, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"seen","seen",1121531738,null),new cljs.core.Symbol(null,"recursion-limits","recursion-limits",-891577955,null),new cljs.core.Symbol(null,"acc","acc",-1815869457,null),new cljs.core.Symbol(null,"pattern","pattern",1882666950,null),cljs.core.with_meta(new cljs.core.Symbol(null,"attr","attr",1036399174,null),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"tag","tag",-1290361223),new cljs.core.Symbol(null,"PullAttr","PullAttr",1557473458,null)], null)),new cljs.core.Symbol(null,"attrs","attrs",-450137186,null),new cljs.core.Symbol(null,"id","id",252129435,null)], null);
}));

(datascript.pull_api.ReverseAttrsFrame.cljs$lang$type = true);

(datascript.pull_api.ReverseAttrsFrame.cljs$lang$ctorPrSeq = (function (this__5494__auto__){
return (new cljs.core.List(null,"datascript.pull-api/ReverseAttrsFrame",null,(1),null));
}));

(datascript.pull_api.ReverseAttrsFrame.cljs$lang$ctorPrWriter = (function (this__5494__auto__,writer__5495__auto__){
return cljs.core._write(writer__5495__auto__,"datascript.pull-api/ReverseAttrsFrame");
}));

/**
 * Positional factory function for datascript.pull-api/ReverseAttrsFrame.
 */
datascript.pull_api.__GT_ReverseAttrsFrame = (function datascript$pull_api$__GT_ReverseAttrsFrame(seen,recursion_limits,acc,pattern,attr,attrs,id){
return (new datascript.pull_api.ReverseAttrsFrame(seen,recursion_limits,acc,pattern,attr,attrs,id,null,null,null));
});

/**
 * Factory function for datascript.pull-api/ReverseAttrsFrame, taking a map of keywords to field values.
 */
datascript.pull_api.map__GT_ReverseAttrsFrame = (function datascript$pull_api$map__GT_ReverseAttrsFrame(G__40786){
var extmap__5490__auto__ = (function (){var G__40831 = cljs.core.dissoc.cljs$core$IFn$_invoke$arity$variadic(G__40786,new cljs.core.Keyword(null,"seen","seen",-518999789),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814),new cljs.core.Keyword(null,"acc","acc",838566312),new cljs.core.Keyword(null,"pattern","pattern",242135423),new cljs.core.Keyword(null,"attr","attr",-604132353),new cljs.core.Keyword(null,"attrs","attrs",-2090668713),new cljs.core.Keyword(null,"id","id",-1388402092)], 0));
if(cljs.core.record_QMARK_(G__40786)){
return cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentArrayMap.EMPTY,G__40831);
} else {
return G__40831;
}
})();
return (new datascript.pull_api.ReverseAttrsFrame(new cljs.core.Keyword(null,"seen","seen",-518999789).cljs$core$IFn$_invoke$arity$1(G__40786),new cljs.core.Keyword(null,"recursion-limits","recursion-limits",1762857814).cljs$core$IFn$_invoke$arity$1(G__40786),new cljs.core.Keyword(null,"acc","acc",838566312).cljs$core$IFn$_invoke$arity$1(G__40786),new cljs.core.Keyword(null,"pattern","pattern",242135423).cljs$core$IFn$_invoke$arity$1(G__40786),new cljs.core.Keyword(null,"attr","attr",-604132353).cljs$core$IFn$_invoke$arity$1(G__40786),new cljs.core.Keyword(null,"attrs","attrs",-2090668713).cljs$core$IFn$_invoke$arity$1(G__40786),new cljs.core.Keyword(null,"id","id",-1388402092).cljs$core$IFn$_invoke$arity$1(G__40786),null,cljs.core.not_empty(extmap__5490__auto__),null));
});

datascript.pull_api.auto_expanding_QMARK_ = (function datascript$pull_api$auto_expanding_QMARK_(attr){
var or__5142__auto__ = attr.recursive_QMARK_;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
var and__5140__auto__ = attr.component_QMARK_;
if(cljs.core.truth_(and__5140__auto__)){
return attr.pattern.wildcard_QMARK_;
} else {
return and__5140__auto__;
}
}
});
datascript.pull_api.ref_frame = (function datascript$pull_api$ref_frame(context,seen,recursion_limits,pattern,attr,id){
if(cljs.core.not(datascript.pull_api.auto_expanding_QMARK_(attr))){
var G__40832 = context;
var G__40833 = seen;
var G__40834 = recursion_limits;
var G__40835 = attr.pattern;
var G__40836 = id;
return (datascript.pull_api.attrs_frame.cljs$core$IFn$_invoke$arity$5 ? datascript.pull_api.attrs_frame.cljs$core$IFn$_invoke$arity$5(G__40832,G__40833,G__40834,G__40835,G__40836) : datascript.pull_api.attrs_frame.call(null,G__40832,G__40833,G__40834,G__40835,G__40836));
} else {
if(cljs.core.truth_((seen.cljs$core$IFn$_invoke$arity$1 ? seen.cljs$core$IFn$_invoke$arity$1(id) : seen.call(null,id)))){
return (new datascript.pull_api.ResultFrame(new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword("db","id","db/id",-1388397098),id], null),null,null,null,null));
} else {
var lim = (recursion_limits.cljs$core$IFn$_invoke$arity$1 ? recursion_limits.cljs$core$IFn$_invoke$arity$1(attr) : recursion_limits.call(null,attr));
if(cljs.core.truth_((function (){var and__5140__auto__ = lim;
if(cljs.core.truth_(and__5140__auto__)){
return (((lim <= (0))) && (true));
} else {
return and__5140__auto__;
}
})())){
return (new datascript.pull_api.ResultFrame(null,null,null,null,null));
} else {
var seen_SINGLEQUOTE_ = cljs.core.conj.cljs$core$IFn$_invoke$arity$2(seen,id);
var recursion_limits_SINGLEQUOTE_ = (cljs.core.truth_(lim)?cljs.core.update.cljs$core$IFn$_invoke$arity$3(recursion_limits,attr,cljs.core.dec):(cljs.core.truth_(attr.recursion_limit)?cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(recursion_limits,attr,(attr.recursion_limit - (1))):recursion_limits
));
var G__40837 = context;
var G__40838 = seen_SINGLEQUOTE_;
var G__40839 = recursion_limits_SINGLEQUOTE_;
var G__40840 = (cljs.core.truth_(attr.recursive_QMARK_)?pattern:attr.pattern);
var G__40841 = id;
return (datascript.pull_api.attrs_frame.cljs$core$IFn$_invoke$arity$5 ? datascript.pull_api.attrs_frame.cljs$core$IFn$_invoke$arity$5(G__40837,G__40838,G__40839,G__40840,G__40841) : datascript.pull_api.attrs_frame.call(null,G__40837,G__40838,G__40839,G__40840,G__40841));

}
}
}
});
datascript.pull_api.attrs_frame = (function datascript$pull_api$attrs_frame(context,seen,recursion_limits,pattern,id){
var db = context.db;
var datoms = (function (){if(cljs.core.truth_((function (){var and__5140__auto__ = pattern.wildcard_QMARK_;
if(cljs.core.truth_(and__5140__auto__)){
return (((db instanceof datascript.db.DB)) && (true));
} else {
return and__5140__auto__;
}
})())){
return me.tonsky.persistent_sorted_set.slice.cljs$core$IFn$_invoke$arity$3(db.eavt,datascript.db.datom.cljs$core$IFn$_invoke$arity$4(id,null,null,(536870912)),datascript.db.datom.cljs$core$IFn$_invoke$arity$4(id,null,null,(2147483647)));
} else {
if(cljs.core.truth_(pattern.wildcard_QMARK_)){
return datascript.db._search(db,new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [id], null));
} else {
if((pattern.first_attr == null)){
return null;
} else {
var from = pattern.first_attr.name;
var to = pattern.last_attr.name;
if((db instanceof datascript.db.DB)){
return me.tonsky.persistent_sorted_set.slice.cljs$core$IFn$_invoke$arity$3(db.eavt,datascript.db.datom.cljs$core$IFn$_invoke$arity$4(id,from,null,(536870912)),datascript.db.datom.cljs$core$IFn$_invoke$arity$4(id,to,null,(2147483647)));
} else {
return datascript.db._seek_datoms(db,new cljs.core.Keyword(null,"eavt","eavt",-666437073),id,null,null,null);

}
}
}
}
})();
if(cljs.core.truth_(pattern.wildcard_QMARK_)){
datascript.pull_api.visit(context,new cljs.core.Keyword("db.pull","wildcard","db.pull/wildcard",116316031),id,null,null);
} else {
}

return (new datascript.pull_api.AttrsFrame(seen,recursion_limits,cljs.core.transient$(cljs.core.PersistentArrayMap.EMPTY),pattern,datascript.pull_api.first_seq(pattern.attrs),datascript.pull_api.next_seq(pattern.attrs),datoms,id,null,null,null));
});
datascript.pull_api.pull_impl = (function datascript$pull_api$pull_impl(parsed_opts,id){
var map__40843 = parsed_opts;
var map__40843__$1 = cljs.core.__destructure_map(map__40843);
var context = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40843__$1,new cljs.core.Keyword(null,"context","context",-830191113));
var pattern = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40843__$1,new cljs.core.Keyword(null,"pattern","pattern",242135423));
var temp__5808__auto__ = datascript.db.entid(context.db,id);
if((temp__5808__auto__ == null)){
return null;
} else {
var eid = temp__5808__auto__;
var stack = (new cljs.core.List(null,datascript.pull_api.attrs_frame(context,cljs.core.PersistentHashSet.EMPTY,cljs.core.PersistentArrayMap.EMPTY,pattern,eid),null,(1),null));
while(true){
var last = datascript.pull_api.first_seq(stack);
var stack_SINGLEQUOTE_ = datascript.pull_api.next_seq(stack);
if((!((last instanceof datascript.pull_api.ResultFrame)))){
var G__41235 = cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(datascript.pull_api.conj_seq,stack_SINGLEQUOTE_,datascript.pull_api._run(last,context));
stack = G__41235;
continue;
} else {
if((stack_SINGLEQUOTE_ == null)){
return last.value;
} else {
var penultimate = datascript.pull_api.first_seq(stack_SINGLEQUOTE_);
var stack_SINGLEQUOTE__SINGLEQUOTE_ = datascript.pull_api.next_seq(stack_SINGLEQUOTE_);
var G__41236 = datascript.pull_api.conj_seq(stack_SINGLEQUOTE__SINGLEQUOTE_,datascript.pull_api._merge(penultimate,last));
stack = G__41236;
continue;

}
}
break;
}
}
});
datascript.pull_api.parse_opts = (function datascript$pull_api$parse_opts(var_args){
var G__40858 = arguments.length;
switch (G__40858) {
case 2:
return datascript.pull_api.parse_opts.cljs$core$IFn$_invoke$arity$2((arguments[(0)]),(arguments[(1)]));

break;
case 3:
return datascript.pull_api.parse_opts.cljs$core$IFn$_invoke$arity$3((arguments[(0)]),(arguments[(1)]),(arguments[(2)]));

break;
default:
throw (new Error(["Invalid arity: ",arguments.length].join("")));

}
});

(datascript.pull_api.parse_opts.cljs$core$IFn$_invoke$arity$2 = (function (db,pattern){
return datascript.pull_api.parse_opts.cljs$core$IFn$_invoke$arity$3(db,pattern,null);
}));

(datascript.pull_api.parse_opts.cljs$core$IFn$_invoke$arity$3 = (function (db,pattern,p__40860){
var map__40861 = p__40860;
var map__40861__$1 = cljs.core.__destructure_map(map__40861);
var visitor = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40861__$1,new cljs.core.Keyword(null,"visitor","visitor",-1026865865));
return new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"pattern","pattern",242135423),datascript.lru._get(datascript.db.unfiltered_db(db).pull_patterns,pattern,(function (){
return datascript.pull_parser.parse_pattern(db,pattern);
})),new cljs.core.Keyword(null,"context","context",-830191113),(new datascript.pull_api.Context(db,visitor,null,null,null))], null);
}));

(datascript.pull_api.parse_opts.cljs$lang$maxFixedArity = 3);

/**
 * Supported opts:
 * 
 * :visitor a fn of 4 arguments, will be called for every entity/attribute pull touches
 * 
 * (:db.pull/attr     e   a   nil) - when pulling a normal attribute, no matter if it has value or not
 * (:db.pull/wildcard e   nil nil) - when pulling every attribute on an entity
 * (:db.pull/reverse  nil a   v  ) - when pulling reverse attribute
 */
datascript.pull_api.pull = (function datascript$pull_api$pull(var_args){
var G__40867 = arguments.length;
switch (G__40867) {
case 3:
return datascript.pull_api.pull.cljs$core$IFn$_invoke$arity$3((arguments[(0)]),(arguments[(1)]),(arguments[(2)]));

break;
case 4:
return datascript.pull_api.pull.cljs$core$IFn$_invoke$arity$4((arguments[(0)]),(arguments[(1)]),(arguments[(2)]),(arguments[(3)]));

break;
default:
throw (new Error(["Invalid arity: ",arguments.length].join("")));

}
});

(datascript.pull_api.pull.cljs$core$IFn$_invoke$arity$3 = (function (db,pattern,id){
return datascript.pull_api.pull.cljs$core$IFn$_invoke$arity$4(db,pattern,id,cljs.core.PersistentArrayMap.EMPTY);
}));

(datascript.pull_api.pull.cljs$core$IFn$_invoke$arity$4 = (function (db,pattern,id,opts){
if(datascript.db.db_QMARK_(db)){
} else {
throw (new Error("Assert failed: (db/db? db)"));
}

var parsed_opts = datascript.pull_api.parse_opts.cljs$core$IFn$_invoke$arity$3(db,pattern,opts);
return datascript.pull_api.pull_impl(parsed_opts,id);
}));

(datascript.pull_api.pull.cljs$lang$maxFixedArity = 4);

datascript.pull_api.pull_many = (function datascript$pull_api$pull_many(var_args){
var G__40870 = arguments.length;
switch (G__40870) {
case 3:
return datascript.pull_api.pull_many.cljs$core$IFn$_invoke$arity$3((arguments[(0)]),(arguments[(1)]),(arguments[(2)]));

break;
case 4:
return datascript.pull_api.pull_many.cljs$core$IFn$_invoke$arity$4((arguments[(0)]),(arguments[(1)]),(arguments[(2)]),(arguments[(3)]));

break;
default:
throw (new Error(["Invalid arity: ",arguments.length].join("")));

}
});

(datascript.pull_api.pull_many.cljs$core$IFn$_invoke$arity$3 = (function (db,pattern,ids){
return datascript.pull_api.pull_many.cljs$core$IFn$_invoke$arity$4(db,pattern,ids,cljs.core.PersistentArrayMap.EMPTY);
}));

(datascript.pull_api.pull_many.cljs$core$IFn$_invoke$arity$4 = (function (db,pattern,ids,opts){
if(datascript.db.db_QMARK_(db)){
} else {
throw (new Error("Assert failed: (db/db? db)"));
}

var parsed_opts = datascript.pull_api.parse_opts.cljs$core$IFn$_invoke$arity$3(db,pattern,opts);
return cljs.core.mapv.cljs$core$IFn$_invoke$arity$2((function (p1__40868_SHARP_){
return datascript.pull_api.pull_impl(parsed_opts,p1__40868_SHARP_);
}),ids);
}));

(datascript.pull_api.pull_many.cljs$lang$maxFixedArity = 4);


//# sourceMappingURL=datascript.pull_api.js.map
