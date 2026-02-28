import "./cljs_env.js";
import "./cljs.core.js";
goog.provide('me.tonsky.persistent_sorted_set.protocol');

/**
 * @interface
 */
me.tonsky.persistent_sorted_set.protocol.IStorage = function(){};

var me$tonsky$persistent_sorted_set$protocol$IStorage$restore$dyn_32832 = (function (this$,address){
var x__5498__auto__ = (((this$ == null))?null:this$);
var m__5499__auto__ = (me.tonsky.persistent_sorted_set.protocol.restore[goog.typeOf(x__5498__auto__)]);
if((!((m__5499__auto__ == null)))){
return (m__5499__auto__.cljs$core$IFn$_invoke$arity$2 ? m__5499__auto__.cljs$core$IFn$_invoke$arity$2(this$,address) : m__5499__auto__.call(null,this$,address));
} else {
var m__5497__auto__ = (me.tonsky.persistent_sorted_set.protocol.restore["_"]);
if((!((m__5497__auto__ == null)))){
return (m__5497__auto__.cljs$core$IFn$_invoke$arity$2 ? m__5497__auto__.cljs$core$IFn$_invoke$arity$2(this$,address) : m__5497__auto__.call(null,this$,address));
} else {
throw cljs.core.missing_protocol("IStorage.restore",this$);
}
}
});
me.tonsky.persistent_sorted_set.protocol.restore = (function me$tonsky$persistent_sorted_set$protocol$restore(this$,address){
if((((!((this$ == null)))) && ((!((this$.me$tonsky$persistent_sorted_set$protocol$IStorage$restore$arity$2 == null)))))){
return this$.me$tonsky$persistent_sorted_set$protocol$IStorage$restore$arity$2(this$,address);
} else {
return me$tonsky$persistent_sorted_set$protocol$IStorage$restore$dyn_32832(this$,address);
}
});

var me$tonsky$persistent_sorted_set$protocol$IStorage$accessed$dyn_32836 = (function (this$,address){
var x__5498__auto__ = (((this$ == null))?null:this$);
var m__5499__auto__ = (me.tonsky.persistent_sorted_set.protocol.accessed[goog.typeOf(x__5498__auto__)]);
if((!((m__5499__auto__ == null)))){
return (m__5499__auto__.cljs$core$IFn$_invoke$arity$2 ? m__5499__auto__.cljs$core$IFn$_invoke$arity$2(this$,address) : m__5499__auto__.call(null,this$,address));
} else {
var m__5497__auto__ = (me.tonsky.persistent_sorted_set.protocol.accessed["_"]);
if((!((m__5497__auto__ == null)))){
return (m__5497__auto__.cljs$core$IFn$_invoke$arity$2 ? m__5497__auto__.cljs$core$IFn$_invoke$arity$2(this$,address) : m__5497__auto__.call(null,this$,address));
} else {
throw cljs.core.missing_protocol("IStorage.accessed",this$);
}
}
});
me.tonsky.persistent_sorted_set.protocol.accessed = (function me$tonsky$persistent_sorted_set$protocol$accessed(this$,address){
if((((!((this$ == null)))) && ((!((this$.me$tonsky$persistent_sorted_set$protocol$IStorage$accessed$arity$2 == null)))))){
return this$.me$tonsky$persistent_sorted_set$protocol$IStorage$accessed$arity$2(this$,address);
} else {
return me$tonsky$persistent_sorted_set$protocol$IStorage$accessed$dyn_32836(this$,address);
}
});

var me$tonsky$persistent_sorted_set$protocol$IStorage$store$dyn_32837 = (function (this$,node,address){
var x__5498__auto__ = (((this$ == null))?null:this$);
var m__5499__auto__ = (me.tonsky.persistent_sorted_set.protocol.store[goog.typeOf(x__5498__auto__)]);
if((!((m__5499__auto__ == null)))){
return (m__5499__auto__.cljs$core$IFn$_invoke$arity$3 ? m__5499__auto__.cljs$core$IFn$_invoke$arity$3(this$,node,address) : m__5499__auto__.call(null,this$,node,address));
} else {
var m__5497__auto__ = (me.tonsky.persistent_sorted_set.protocol.store["_"]);
if((!((m__5497__auto__ == null)))){
return (m__5497__auto__.cljs$core$IFn$_invoke$arity$3 ? m__5497__auto__.cljs$core$IFn$_invoke$arity$3(this$,node,address) : m__5497__auto__.call(null,this$,node,address));
} else {
throw cljs.core.missing_protocol("IStorage.store",this$);
}
}
});
me.tonsky.persistent_sorted_set.protocol.store = (function me$tonsky$persistent_sorted_set$protocol$store(this$,node,address){
if((((!((this$ == null)))) && ((!((this$.me$tonsky$persistent_sorted_set$protocol$IStorage$store$arity$3 == null)))))){
return this$.me$tonsky$persistent_sorted_set$protocol$IStorage$store$arity$3(this$,node,address);
} else {
return me$tonsky$persistent_sorted_set$protocol$IStorage$store$dyn_32837(this$,node,address);
}
});

var me$tonsky$persistent_sorted_set$protocol$IStorage$delete$dyn_32841 = (function (this$,unused_addresses){
var x__5498__auto__ = (((this$ == null))?null:this$);
var m__5499__auto__ = (me.tonsky.persistent_sorted_set.protocol.delete$[goog.typeOf(x__5498__auto__)]);
if((!((m__5499__auto__ == null)))){
return (m__5499__auto__.cljs$core$IFn$_invoke$arity$2 ? m__5499__auto__.cljs$core$IFn$_invoke$arity$2(this$,unused_addresses) : m__5499__auto__.call(null,this$,unused_addresses));
} else {
var m__5497__auto__ = (me.tonsky.persistent_sorted_set.protocol.delete$["_"]);
if((!((m__5497__auto__ == null)))){
return (m__5497__auto__.cljs$core$IFn$_invoke$arity$2 ? m__5497__auto__.cljs$core$IFn$_invoke$arity$2(this$,unused_addresses) : m__5497__auto__.call(null,this$,unused_addresses));
} else {
throw cljs.core.missing_protocol("IStorage.delete",this$);
}
}
});
me.tonsky.persistent_sorted_set.protocol.delete$ = (function me$tonsky$persistent_sorted_set$protocol$delete(this$,unused_addresses){
if((((!((this$ == null)))) && ((!((this$.me$tonsky$persistent_sorted_set$protocol$IStorage$delete$arity$2 == null)))))){
return this$.me$tonsky$persistent_sorted_set$protocol$IStorage$delete$arity$2(this$,unused_addresses);
} else {
return me$tonsky$persistent_sorted_set$protocol$IStorage$delete$dyn_32841(this$,unused_addresses);
}
});


//# sourceMappingURL=me.tonsky.persistent_sorted_set.protocol.js.map
