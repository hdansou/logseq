import "./cljs_env.js";
import "./cljs.core.js";
goog.provide('malli.registry');
/**
 * @define {string}
 * @type {string}
 */
malli.registry.mode = goog.define("malli.registry.mode","default");
/**
 * @define {string}
 * @type {string}
 */
malli.registry.type = goog.define("malli.registry.type","default");

/**
 * @interface
 */
malli.registry.Registry = function(){};

var malli$registry$Registry$_schema$dyn_43470 = (function (this$,type){
var x__5498__auto__ = (((this$ == null))?null:this$);
var m__5499__auto__ = (malli.registry._schema[goog.typeOf(x__5498__auto__)]);
if((!((m__5499__auto__ == null)))){
return (m__5499__auto__.cljs$core$IFn$_invoke$arity$2 ? m__5499__auto__.cljs$core$IFn$_invoke$arity$2(this$,type) : m__5499__auto__.call(null,this$,type));
} else {
var m__5497__auto__ = (malli.registry._schema["_"]);
if((!((m__5497__auto__ == null)))){
return (m__5497__auto__.cljs$core$IFn$_invoke$arity$2 ? m__5497__auto__.cljs$core$IFn$_invoke$arity$2(this$,type) : m__5497__auto__.call(null,this$,type));
} else {
throw cljs.core.missing_protocol("Registry.-schema",this$);
}
}
});
/**
 * returns the schema from a registry
 */
malli.registry._schema = (function malli$registry$_schema(this$,type){
if((((!((this$ == null)))) && ((!((this$.malli$registry$Registry$_schema$arity$2 == null)))))){
return this$.malli$registry$Registry$_schema$arity$2(this$,type);
} else {
return malli$registry$Registry$_schema$dyn_43470(this$,type);
}
});

var malli$registry$Registry$_schemas$dyn_43472 = (function (this$){
var x__5498__auto__ = (((this$ == null))?null:this$);
var m__5499__auto__ = (malli.registry._schemas[goog.typeOf(x__5498__auto__)]);
if((!((m__5499__auto__ == null)))){
return (m__5499__auto__.cljs$core$IFn$_invoke$arity$1 ? m__5499__auto__.cljs$core$IFn$_invoke$arity$1(this$) : m__5499__auto__.call(null,this$));
} else {
var m__5497__auto__ = (malli.registry._schemas["_"]);
if((!((m__5497__auto__ == null)))){
return (m__5497__auto__.cljs$core$IFn$_invoke$arity$1 ? m__5497__auto__.cljs$core$IFn$_invoke$arity$1(this$) : m__5497__auto__.call(null,this$));
} else {
throw cljs.core.missing_protocol("Registry.-schemas",this$);
}
}
});
/**
 * returns all schemas from a registry
 */
malli.registry._schemas = (function malli$registry$_schemas(this$){
if((((!((this$ == null)))) && ((!((this$.malli$registry$Registry$_schemas$arity$1 == null)))))){
return this$.malli$registry$Registry$_schemas$arity$1(this$);
} else {
return malli$registry$Registry$_schemas$dyn_43472(this$);
}
});

malli.registry.registry_QMARK_ = (function malli$registry$registry_QMARK_(x){
if((!((x == null)))){
if(((false) || ((cljs.core.PROTOCOL_SENTINEL === x.malli$registry$Registry$)))){
return true;
} else {
return false;
}
} else {
return false;
}
});

/**
* @constructor
 * @implements {malli.registry.Registry}
 * @implements {cljs.core.IMeta}
 * @implements {cljs.core.IWithMeta}
*/
malli.registry.t_malli$registry42995 = (function (m,fm,meta42996){
this.m = m;
this.fm = fm;
this.meta42996 = meta42996;
this.cljs$lang$protocol_mask$partition0$ = 393216;
this.cljs$lang$protocol_mask$partition1$ = 0;
});
(malli.registry.t_malli$registry42995.prototype.cljs$core$IWithMeta$_with_meta$arity$2 = (function (_42997,meta42996__$1){
var self__ = this;
var _42997__$1 = this;
return (new malli.registry.t_malli$registry42995(self__.m,self__.fm,meta42996__$1));
}));

(malli.registry.t_malli$registry42995.prototype.cljs$core$IMeta$_meta$arity$1 = (function (_42997){
var self__ = this;
var _42997__$1 = this;
return self__.meta42996;
}));

(malli.registry.t_malli$registry42995.prototype.malli$registry$Registry$ = cljs.core.PROTOCOL_SENTINEL);

(malli.registry.t_malli$registry42995.prototype.malli$registry$Registry$_schema$arity$2 = (function (_,type){
var self__ = this;
var ___$1 = this;
return self__.fm.get(type);
}));

(malli.registry.t_malli$registry42995.prototype.malli$registry$Registry$_schemas$arity$1 = (function (_){
var self__ = this;
var ___$1 = this;
return self__.m;
}));

(malli.registry.t_malli$registry42995.getBasis = (function (){
return new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"m","m",-1021758608,null),new cljs.core.Symbol(null,"fm","fm",-1190690268,null),new cljs.core.Symbol(null,"meta42996","meta42996",608603985,null)], null);
}));

(malli.registry.t_malli$registry42995.cljs$lang$type = true);

(malli.registry.t_malli$registry42995.cljs$lang$ctorStr = "malli.registry/t_malli$registry42995");

(malli.registry.t_malli$registry42995.cljs$lang$ctorPrWriter = (function (this__5434__auto__,writer__5435__auto__,opt__5436__auto__){
return cljs.core._write(writer__5435__auto__,"malli.registry/t_malli$registry42995");
}));

/**
 * Positional factory function for malli.registry/t_malli$registry42995.
 */
malli.registry.__GT_t_malli$registry42995 = (function malli$registry$__GT_t_malli$registry42995(m,fm,meta42996){
return (new malli.registry.t_malli$registry42995(m,fm,meta42996));
});


malli.registry.fast_registry = (function malli$registry$fast_registry(m){
var fm = m;
return (new malli.registry.t_malli$registry42995(m,fm,cljs.core.PersistentArrayMap.EMPTY));
});

/**
* @constructor
 * @implements {malli.registry.Registry}
 * @implements {cljs.core.IMeta}
 * @implements {cljs.core.IWithMeta}
*/
malli.registry.t_malli$registry43019 = (function (m,meta43020){
this.m = m;
this.meta43020 = meta43020;
this.cljs$lang$protocol_mask$partition0$ = 393216;
this.cljs$lang$protocol_mask$partition1$ = 0;
});
(malli.registry.t_malli$registry43019.prototype.cljs$core$IWithMeta$_with_meta$arity$2 = (function (_43021,meta43020__$1){
var self__ = this;
var _43021__$1 = this;
return (new malli.registry.t_malli$registry43019(self__.m,meta43020__$1));
}));

(malli.registry.t_malli$registry43019.prototype.cljs$core$IMeta$_meta$arity$1 = (function (_43021){
var self__ = this;
var _43021__$1 = this;
return self__.meta43020;
}));

(malli.registry.t_malli$registry43019.prototype.malli$registry$Registry$ = cljs.core.PROTOCOL_SENTINEL);

(malli.registry.t_malli$registry43019.prototype.malli$registry$Registry$_schema$arity$2 = (function (_,type){
var self__ = this;
var ___$1 = this;
return (self__.m.cljs$core$IFn$_invoke$arity$1 ? self__.m.cljs$core$IFn$_invoke$arity$1(type) : self__.m.call(null,type));
}));

(malli.registry.t_malli$registry43019.prototype.malli$registry$Registry$_schemas$arity$1 = (function (_){
var self__ = this;
var ___$1 = this;
return self__.m;
}));

(malli.registry.t_malli$registry43019.getBasis = (function (){
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"m","m",-1021758608,null),new cljs.core.Symbol(null,"meta43020","meta43020",-1487179915,null)], null);
}));

(malli.registry.t_malli$registry43019.cljs$lang$type = true);

(malli.registry.t_malli$registry43019.cljs$lang$ctorStr = "malli.registry/t_malli$registry43019");

(malli.registry.t_malli$registry43019.cljs$lang$ctorPrWriter = (function (this__5434__auto__,writer__5435__auto__,opt__5436__auto__){
return cljs.core._write(writer__5435__auto__,"malli.registry/t_malli$registry43019");
}));

/**
 * Positional factory function for malli.registry/t_malli$registry43019.
 */
malli.registry.__GT_t_malli$registry43019 = (function malli$registry$__GT_t_malli$registry43019(m,meta43020){
return (new malli.registry.t_malli$registry43019(m,meta43020));
});


malli.registry.simple_registry = (function malli$registry$simple_registry(m){
return (new malli.registry.t_malli$registry43019(m,cljs.core.PersistentArrayMap.EMPTY));
});
malli.registry.registry = (function malli$registry$registry(_QMARK_registry){
if((_QMARK_registry == null)){
return null;
} else {
if(malli.registry.registry_QMARK_(_QMARK_registry)){
return _QMARK_registry;
} else {
if(cljs.core.map_QMARK_(_QMARK_registry)){
return malli.registry.simple_registry(_QMARK_registry);
} else {
if((((!((_QMARK_registry == null))))?((((false) || ((cljs.core.PROTOCOL_SENTINEL === _QMARK_registry.malli$registry$Registry$))))?true:(((!_QMARK_registry.cljs$lang$protocol_mask$partition$))?cljs.core.native_satisfies_QMARK_(malli.registry.Registry,_QMARK_registry):false)):cljs.core.native_satisfies_QMARK_(malli.registry.Registry,_QMARK_registry))){
return _QMARK_registry;
} else {
return null;
}
}
}
}
});
malli.registry.registry_STAR_ = cljs.core.atom.cljs$core$IFn$_invoke$arity$1(malli.registry.simple_registry(cljs.core.PersistentArrayMap.EMPTY));
malli.registry.set_default_registry_BANG_ = (function malli$registry$set_default_registry_BANG_(_QMARK_registry){
if((!((malli.registry.mode === "strict")))){
return cljs.core.reset_BANG_(malli.registry.registry_STAR_,malli.registry.registry(_QMARK_registry));
} else {
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2("can't set default registry, invalid mode",new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"mode","mode",654403691),malli.registry.mode,new cljs.core.Keyword(null,"type","type",1174270348),malli.registry.type], null));
}
});

/**
* @constructor
 * @implements {malli.registry.Registry}
 * @implements {cljs.core.IMeta}
 * @implements {cljs.core.IWithMeta}
*/
malli.registry.t_malli$registry43087 = (function (meta43088){
this.meta43088 = meta43088;
this.cljs$lang$protocol_mask$partition0$ = 393216;
this.cljs$lang$protocol_mask$partition1$ = 0;
});
(malli.registry.t_malli$registry43087.prototype.cljs$core$IWithMeta$_with_meta$arity$2 = (function (_43089,meta43088__$1){
var self__ = this;
var _43089__$1 = this;
return (new malli.registry.t_malli$registry43087(meta43088__$1));
}));

(malli.registry.t_malli$registry43087.prototype.cljs$core$IMeta$_meta$arity$1 = (function (_43089){
var self__ = this;
var _43089__$1 = this;
return self__.meta43088;
}));

(malli.registry.t_malli$registry43087.prototype.malli$registry$Registry$ = cljs.core.PROTOCOL_SENTINEL);

(malli.registry.t_malli$registry43087.prototype.malli$registry$Registry$_schema$arity$2 = (function (_,type){
var self__ = this;
var ___$1 = this;
return malli.registry._schema(cljs.core.deref(malli.registry.registry_STAR_),type);
}));

(malli.registry.t_malli$registry43087.prototype.malli$registry$Registry$_schemas$arity$1 = (function (_){
var self__ = this;
var ___$1 = this;
return malli.registry._schemas(cljs.core.deref(malli.registry.registry_STAR_));
}));

(malli.registry.t_malli$registry43087.getBasis = (function (){
return new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"meta43088","meta43088",-1919816680,null)], null);
}));

(malli.registry.t_malli$registry43087.cljs$lang$type = true);

(malli.registry.t_malli$registry43087.cljs$lang$ctorStr = "malli.registry/t_malli$registry43087");

(malli.registry.t_malli$registry43087.cljs$lang$ctorPrWriter = (function (this__5434__auto__,writer__5435__auto__,opt__5436__auto__){
return cljs.core._write(writer__5435__auto__,"malli.registry/t_malli$registry43087");
}));

/**
 * Positional factory function for malli.registry/t_malli$registry43087.
 */
malli.registry.__GT_t_malli$registry43087 = (function malli$registry$__GT_t_malli$registry43087(meta43088){
return (new malli.registry.t_malli$registry43087(meta43088));
});


malli.registry.custom_default_registry = (function malli$registry$custom_default_registry(){
return (new malli.registry.t_malli$registry43087(cljs.core.PersistentArrayMap.EMPTY));
});

/**
* @constructor
 * @implements {malli.registry.Registry}
 * @implements {cljs.core.IMeta}
 * @implements {cljs.core.IWithMeta}
*/
malli.registry.t_malli$registry43263 = (function (_QMARK_registries,registries,meta43264){
this._QMARK_registries = _QMARK_registries;
this.registries = registries;
this.meta43264 = meta43264;
this.cljs$lang$protocol_mask$partition0$ = 393216;
this.cljs$lang$protocol_mask$partition1$ = 0;
});
(malli.registry.t_malli$registry43263.prototype.cljs$core$IWithMeta$_with_meta$arity$2 = (function (_43265,meta43264__$1){
var self__ = this;
var _43265__$1 = this;
return (new malli.registry.t_malli$registry43263(self__._QMARK_registries,self__.registries,meta43264__$1));
}));

(malli.registry.t_malli$registry43263.prototype.cljs$core$IMeta$_meta$arity$1 = (function (_43265){
var self__ = this;
var _43265__$1 = this;
return self__.meta43264;
}));

(malli.registry.t_malli$registry43263.prototype.malli$registry$Registry$ = cljs.core.PROTOCOL_SENTINEL);

(malli.registry.t_malli$registry43263.prototype.malli$registry$Registry$_schema$arity$2 = (function (_,type){
var self__ = this;
var ___$1 = this;
return cljs.core.some((function (p1__43177_SHARP_){
return malli.registry._schema(p1__43177_SHARP_,type);
}),self__.registries);
}));

(malli.registry.t_malli$registry43263.prototype.malli$registry$Registry$_schemas$arity$1 = (function (_){
var self__ = this;
var ___$1 = this;
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$2(cljs.core.merge,cljs.core.map.cljs$core$IFn$_invoke$arity$2(malli.registry._schemas,cljs.core.reverse(self__.registries)));
}));

(malli.registry.t_malli$registry43263.getBasis = (function (){
return new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?registries","?registries",2135368100,null),new cljs.core.Symbol(null,"registries","registries",-1366064418,null),new cljs.core.Symbol(null,"meta43264","meta43264",-1076638143,null)], null);
}));

(malli.registry.t_malli$registry43263.cljs$lang$type = true);

(malli.registry.t_malli$registry43263.cljs$lang$ctorStr = "malli.registry/t_malli$registry43263");

(malli.registry.t_malli$registry43263.cljs$lang$ctorPrWriter = (function (this__5434__auto__,writer__5435__auto__,opt__5436__auto__){
return cljs.core._write(writer__5435__auto__,"malli.registry/t_malli$registry43263");
}));

/**
 * Positional factory function for malli.registry/t_malli$registry43263.
 */
malli.registry.__GT_t_malli$registry43263 = (function malli$registry$__GT_t_malli$registry43263(_QMARK_registries,registries,meta43264){
return (new malli.registry.t_malli$registry43263(_QMARK_registries,registries,meta43264));
});


malli.registry.composite_registry = (function malli$registry$composite_registry(var_args){
var args__5882__auto__ = [];
var len__5876__auto___43484 = arguments.length;
var i__5877__auto___43485 = (0);
while(true){
if((i__5877__auto___43485 < len__5876__auto___43484)){
args__5882__auto__.push((arguments[i__5877__auto___43485]));

var G__43487 = (i__5877__auto___43485 + (1));
i__5877__auto___43485 = G__43487;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((0) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((0)),(0),null)):null);
return malli.registry.composite_registry.cljs$core$IFn$_invoke$arity$variadic(argseq__5883__auto__);
});

(malli.registry.composite_registry.cljs$core$IFn$_invoke$arity$variadic = (function (_QMARK_registries){
var registries = cljs.core.mapv.cljs$core$IFn$_invoke$arity$2(malli.registry.registry,_QMARK_registries);
return (new malli.registry.t_malli$registry43263(_QMARK_registries,registries,cljs.core.PersistentArrayMap.EMPTY));
}));

(malli.registry.composite_registry.cljs$lang$maxFixedArity = (0));

/** @this {Function} */
(malli.registry.composite_registry.cljs$lang$applyTo = (function (seq43196){
var self__5862__auto__ = this;
return self__5862__auto__.cljs$core$IFn$_invoke$arity$variadic(cljs.core.seq(seq43196));
}));


/**
* @constructor
 * @implements {malli.registry.Registry}
 * @implements {cljs.core.IMeta}
 * @implements {cljs.core.IWithMeta}
*/
malli.registry.t_malli$registry43292 = (function (db,meta43293){
this.db = db;
this.meta43293 = meta43293;
this.cljs$lang$protocol_mask$partition0$ = 393216;
this.cljs$lang$protocol_mask$partition1$ = 0;
});
(malli.registry.t_malli$registry43292.prototype.cljs$core$IWithMeta$_with_meta$arity$2 = (function (_43294,meta43293__$1){
var self__ = this;
var _43294__$1 = this;
return (new malli.registry.t_malli$registry43292(self__.db,meta43293__$1));
}));

(malli.registry.t_malli$registry43292.prototype.cljs$core$IMeta$_meta$arity$1 = (function (_43294){
var self__ = this;
var _43294__$1 = this;
return self__.meta43293;
}));

(malli.registry.t_malli$registry43292.prototype.malli$registry$Registry$ = cljs.core.PROTOCOL_SENTINEL);

(malli.registry.t_malli$registry43292.prototype.malli$registry$Registry$_schema$arity$2 = (function (_,type){
var self__ = this;
var ___$1 = this;
return malli.registry._schema(malli.registry.registry(cljs.core.deref(self__.db)),type);
}));

(malli.registry.t_malli$registry43292.prototype.malli$registry$Registry$_schemas$arity$1 = (function (_){
var self__ = this;
var ___$1 = this;
return malli.registry._schemas(malli.registry.registry(cljs.core.deref(self__.db)));
}));

(malli.registry.t_malli$registry43292.getBasis = (function (){
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"db","db",-1661185010,null),new cljs.core.Symbol(null,"meta43293","meta43293",-194505364,null)], null);
}));

(malli.registry.t_malli$registry43292.cljs$lang$type = true);

(malli.registry.t_malli$registry43292.cljs$lang$ctorStr = "malli.registry/t_malli$registry43292");

(malli.registry.t_malli$registry43292.cljs$lang$ctorPrWriter = (function (this__5434__auto__,writer__5435__auto__,opt__5436__auto__){
return cljs.core._write(writer__5435__auto__,"malli.registry/t_malli$registry43292");
}));

/**
 * Positional factory function for malli.registry/t_malli$registry43292.
 */
malli.registry.__GT_t_malli$registry43292 = (function malli$registry$__GT_t_malli$registry43292(db,meta43293){
return (new malli.registry.t_malli$registry43292(db,meta43293));
});


malli.registry.mutable_registry = (function malli$registry$mutable_registry(db){
return (new malli.registry.t_malli$registry43292(db,cljs.core.PersistentArrayMap.EMPTY));
});

/**
* @constructor
 * @implements {malli.registry.Registry}
 * @implements {cljs.core.IMeta}
 * @implements {cljs.core.IWithMeta}
*/
malli.registry.t_malli$registry43311 = (function (meta43312){
this.meta43312 = meta43312;
this.cljs$lang$protocol_mask$partition0$ = 393216;
this.cljs$lang$protocol_mask$partition1$ = 0;
});
(malli.registry.t_malli$registry43311.prototype.cljs$core$IWithMeta$_with_meta$arity$2 = (function (_43313,meta43312__$1){
var self__ = this;
var _43313__$1 = this;
return (new malli.registry.t_malli$registry43311(meta43312__$1));
}));

(malli.registry.t_malli$registry43311.prototype.cljs$core$IMeta$_meta$arity$1 = (function (_43313){
var self__ = this;
var _43313__$1 = this;
return self__.meta43312;
}));

(malli.registry.t_malli$registry43311.prototype.malli$registry$Registry$ = cljs.core.PROTOCOL_SENTINEL);

(malli.registry.t_malli$registry43311.prototype.malli$registry$Registry$_schema$arity$2 = (function (_,type){
var self__ = this;
var ___$1 = this;
if(cljs.core.var_QMARK_(type)){
return cljs.core.deref(type);
} else {
return null;
}
}));

(malli.registry.t_malli$registry43311.prototype.malli$registry$Registry$_schemas$arity$1 = (function (_){
var self__ = this;
var ___$1 = this;
return null;
}));

(malli.registry.t_malli$registry43311.getBasis = (function (){
return new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"meta43312","meta43312",-899441445,null)], null);
}));

(malli.registry.t_malli$registry43311.cljs$lang$type = true);

(malli.registry.t_malli$registry43311.cljs$lang$ctorStr = "malli.registry/t_malli$registry43311");

(malli.registry.t_malli$registry43311.cljs$lang$ctorPrWriter = (function (this__5434__auto__,writer__5435__auto__,opt__5436__auto__){
return cljs.core._write(writer__5435__auto__,"malli.registry/t_malli$registry43311");
}));

/**
 * Positional factory function for malli.registry/t_malli$registry43311.
 */
malli.registry.__GT_t_malli$registry43311 = (function malli$registry$__GT_t_malli$registry43311(meta43312){
return (new malli.registry.t_malli$registry43311(meta43312));
});


malli.registry.var_registry = (function malli$registry$var_registry(){
return (new malli.registry.t_malli$registry43311(cljs.core.PersistentArrayMap.EMPTY));
});
malli.registry._STAR_registry_STAR_ = cljs.core.PersistentArrayMap.EMPTY;

/**
* @constructor
 * @implements {malli.registry.Registry}
 * @implements {cljs.core.IMeta}
 * @implements {cljs.core.IWithMeta}
*/
malli.registry.t_malli$registry43328 = (function (meta43329){
this.meta43329 = meta43329;
this.cljs$lang$protocol_mask$partition0$ = 393216;
this.cljs$lang$protocol_mask$partition1$ = 0;
});
(malli.registry.t_malli$registry43328.prototype.cljs$core$IWithMeta$_with_meta$arity$2 = (function (_43330,meta43329__$1){
var self__ = this;
var _43330__$1 = this;
return (new malli.registry.t_malli$registry43328(meta43329__$1));
}));

(malli.registry.t_malli$registry43328.prototype.cljs$core$IMeta$_meta$arity$1 = (function (_43330){
var self__ = this;
var _43330__$1 = this;
return self__.meta43329;
}));

(malli.registry.t_malli$registry43328.prototype.malli$registry$Registry$ = cljs.core.PROTOCOL_SENTINEL);

(malli.registry.t_malli$registry43328.prototype.malli$registry$Registry$_schema$arity$2 = (function (_,type){
var self__ = this;
var ___$1 = this;
return malli.registry._schema(malli.registry.registry(malli.registry._STAR_registry_STAR_),type);
}));

(malli.registry.t_malli$registry43328.prototype.malli$registry$Registry$_schemas$arity$1 = (function (_){
var self__ = this;
var ___$1 = this;
return malli.registry._schemas(malli.registry.registry(malli.registry._STAR_registry_STAR_));
}));

(malli.registry.t_malli$registry43328.getBasis = (function (){
return new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"meta43329","meta43329",-29664913,null)], null);
}));

(malli.registry.t_malli$registry43328.cljs$lang$type = true);

(malli.registry.t_malli$registry43328.cljs$lang$ctorStr = "malli.registry/t_malli$registry43328");

(malli.registry.t_malli$registry43328.cljs$lang$ctorPrWriter = (function (this__5434__auto__,writer__5435__auto__,opt__5436__auto__){
return cljs.core._write(writer__5435__auto__,"malli.registry/t_malli$registry43328");
}));

/**
 * Positional factory function for malli.registry/t_malli$registry43328.
 */
malli.registry.__GT_t_malli$registry43328 = (function malli$registry$__GT_t_malli$registry43328(meta43329){
return (new malli.registry.t_malli$registry43328(meta43329));
});


malli.registry.dynamic_registry = (function malli$registry$dynamic_registry(){
return (new malli.registry.t_malli$registry43328(cljs.core.PersistentArrayMap.EMPTY));
});

/**
* @constructor
 * @implements {malli.registry.Registry}
 * @implements {cljs.core.IMeta}
 * @implements {cljs.core.IWithMeta}
*/
malli.registry.t_malli$registry43438 = (function (default_registry,provider,cache_STAR_,registry_STAR_,meta43439){
this.default_registry = default_registry;
this.provider = provider;
this.cache_STAR_ = cache_STAR_;
this.registry_STAR_ = registry_STAR_;
this.meta43439 = meta43439;
this.cljs$lang$protocol_mask$partition0$ = 393216;
this.cljs$lang$protocol_mask$partition1$ = 0;
});
(malli.registry.t_malli$registry43438.prototype.cljs$core$IWithMeta$_with_meta$arity$2 = (function (_43440,meta43439__$1){
var self__ = this;
var _43440__$1 = this;
return (new malli.registry.t_malli$registry43438(self__.default_registry,self__.provider,self__.cache_STAR_,self__.registry_STAR_,meta43439__$1));
}));

(malli.registry.t_malli$registry43438.prototype.cljs$core$IMeta$_meta$arity$1 = (function (_43440){
var self__ = this;
var _43440__$1 = this;
return self__.meta43439;
}));

(malli.registry.t_malli$registry43438.prototype.malli$registry$Registry$ = cljs.core.PROTOCOL_SENTINEL);

(malli.registry.t_malli$registry43438.prototype.malli$registry$Registry$_schema$arity$2 = (function (_,name){
var self__ = this;
var ___$1 = this;
var or__5142__auto__ = (function (){var fexpr__43446 = cljs.core.deref(self__.cache_STAR_);
return (fexpr__43446.cljs$core$IFn$_invoke$arity$1 ? fexpr__43446.cljs$core$IFn$_invoke$arity$1(name) : fexpr__43446.call(null,name));
})();
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
var temp__5804__auto__ = (function (){var G__43447 = name;
var G__43448 = cljs.core.deref(self__.registry_STAR_);
return (self__.provider.cljs$core$IFn$_invoke$arity$2 ? self__.provider.cljs$core$IFn$_invoke$arity$2(G__43447,G__43448) : self__.provider.call(null,G__43447,G__43448));
})();
if(cljs.core.truth_(temp__5804__auto__)){
var schema = temp__5804__auto__;
cljs.core.swap_BANG_.cljs$core$IFn$_invoke$arity$4(self__.cache_STAR_,cljs.core.assoc,name,schema);

return schema;
} else {
return null;
}
}
}));

(malli.registry.t_malli$registry43438.prototype.malli$registry$Registry$_schemas$arity$1 = (function (_){
var self__ = this;
var ___$1 = this;
return cljs.core.deref(self__.cache_STAR_);
}));

(malli.registry.t_malli$registry43438.getBasis = (function (){
return new cljs.core.PersistentVector(null, 5, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"default-registry","default-registry",732204441,null),new cljs.core.Symbol(null,"provider","provider",1338474627,null),new cljs.core.Symbol(null,"cache*","cache*",-548597526,null),new cljs.core.Symbol(null,"registry*","registry*",-268031273,null),new cljs.core.Symbol(null,"meta43439","meta43439",1183397402,null)], null);
}));

(malli.registry.t_malli$registry43438.cljs$lang$type = true);

(malli.registry.t_malli$registry43438.cljs$lang$ctorStr = "malli.registry/t_malli$registry43438");

(malli.registry.t_malli$registry43438.cljs$lang$ctorPrWriter = (function (this__5434__auto__,writer__5435__auto__,opt__5436__auto__){
return cljs.core._write(writer__5435__auto__,"malli.registry/t_malli$registry43438");
}));

/**
 * Positional factory function for malli.registry/t_malli$registry43438.
 */
malli.registry.__GT_t_malli$registry43438 = (function malli$registry$__GT_t_malli$registry43438(default_registry,provider,cache_STAR_,registry_STAR_,meta43439){
return (new malli.registry.t_malli$registry43438(default_registry,provider,cache_STAR_,registry_STAR_,meta43439));
});


malli.registry.lazy_registry = (function malli$registry$lazy_registry(default_registry,provider){
var cache_STAR_ = cljs.core.atom.cljs$core$IFn$_invoke$arity$1(cljs.core.PersistentArrayMap.EMPTY);
var registry_STAR_ = cljs.core.atom.cljs$core$IFn$_invoke$arity$1(default_registry);
return cljs.core.reset_BANG_(registry_STAR_,malli.registry.composite_registry.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([default_registry,(new malli.registry.t_malli$registry43438(default_registry,provider,cache_STAR_,registry_STAR_,cljs.core.PersistentArrayMap.EMPTY))], 0)));
});
/**
 * finds a schema from a registry
 */
malli.registry.schema = (function malli$registry$schema(registry,type){
return malli.registry._schema(registry,type);
});
/**
 * finds all schemas from a registry
 */
malli.registry.schemas = (function malli$registry$schemas(registry){
return malli.registry._schemas(registry);
});

//# sourceMappingURL=malli.registry.js.map
