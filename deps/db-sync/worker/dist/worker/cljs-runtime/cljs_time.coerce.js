import "./cljs_env.js";
import "./cljs.core.js";
import "./cljs_time.core.js";
import "./cljs_time.format.js";
import "./goog.date.utcdatetime.js";
goog.provide('cljs_time.coerce');

/**
 * @interface
 */
cljs_time.coerce.ICoerce = function(){};

var cljs_time$coerce$ICoerce$to_date_time$dyn_37216 = (function (obj){
var x__5498__auto__ = (((obj == null))?null:obj);
var m__5499__auto__ = (cljs_time.coerce.to_date_time[goog.typeOf(x__5498__auto__)]);
if((!((m__5499__auto__ == null)))){
return (m__5499__auto__.cljs$core$IFn$_invoke$arity$1 ? m__5499__auto__.cljs$core$IFn$_invoke$arity$1(obj) : m__5499__auto__.call(null,obj));
} else {
var m__5497__auto__ = (cljs_time.coerce.to_date_time["_"]);
if((!((m__5497__auto__ == null)))){
return (m__5497__auto__.cljs$core$IFn$_invoke$arity$1 ? m__5497__auto__.cljs$core$IFn$_invoke$arity$1(obj) : m__5497__auto__.call(null,obj));
} else {
throw cljs.core.missing_protocol("ICoerce.to-date-time",obj);
}
}
});
/**
 * Convert `obj` to a goog.date.DateTime instance.
 */
cljs_time.coerce.to_date_time = (function cljs_time$coerce$to_date_time(obj){
if((((!((obj == null)))) && ((!((obj.cljs_time$coerce$ICoerce$to_date_time$arity$1 == null)))))){
return obj.cljs_time$coerce$ICoerce$to_date_time$arity$1(obj);
} else {
return cljs_time$coerce$ICoerce$to_date_time$dyn_37216(obj);
}
});

/**
 * Returns a DateTime instance in the UTC time zone corresponding to the given
 *   number of milliseconds after the Unix epoch.
 */
cljs_time.coerce.from_long = (function cljs_time$coerce$from_long(millis){
var G__37189 = millis;
if((G__37189 == null)){
return null;
} else {
return (goog.date.UtcDateTime.fromTimestamp.cljs$core$IFn$_invoke$arity$1 ? goog.date.UtcDateTime.fromTimestamp.cljs$core$IFn$_invoke$arity$1(G__37189) : goog.date.UtcDateTime.fromTimestamp.call(null,G__37189));
}
});
/**
 * Returns DateTime instance from string using formatters in cljs-time.format,
 *   returning first which parses
 */
cljs_time.coerce.from_string = (function cljs_time$coerce$from_string(s){
if(cljs.core.truth_(s)){
return cljs.core.first((function (){var iter__5628__auto__ = (function cljs_time$coerce$from_string_$_iter__37190(s__37191){
return (new cljs.core.LazySeq(null,(function (){
var s__37191__$1 = s__37191;
while(true){
var temp__5804__auto__ = cljs.core.seq(s__37191__$1);
if(temp__5804__auto__){
var s__37191__$2 = temp__5804__auto__;
if(cljs.core.chunked_seq_QMARK_(s__37191__$2)){
var c__5626__auto__ = cljs.core.chunk_first(s__37191__$2);
var size__5627__auto__ = cljs.core.count(c__5626__auto__);
var b__37193 = cljs.core.chunk_buffer(size__5627__auto__);
if((function (){var i__37192 = (0);
while(true){
if((i__37192 < size__5627__auto__)){
var f = cljs.core._nth(c__5626__auto__,i__37192);
var d = (function (){try{return cljs_time.format.parse.cljs$core$IFn$_invoke$arity$2(f,s);
}catch (e37194){if((e37194 instanceof Error)){
var _ = e37194;
return null;
} else {
throw e37194;

}
}})();
if(cljs.core.truth_(d)){
cljs.core.chunk_append(b__37193,d);

var G__37217 = (i__37192 + (1));
i__37192 = G__37217;
continue;
} else {
var G__37218 = (i__37192 + (1));
i__37192 = G__37218;
continue;
}
} else {
return true;
}
break;
}
})()){
return cljs.core.chunk_cons(cljs.core.chunk(b__37193),cljs_time$coerce$from_string_$_iter__37190(cljs.core.chunk_rest(s__37191__$2)));
} else {
return cljs.core.chunk_cons(cljs.core.chunk(b__37193),null);
}
} else {
var f = cljs.core.first(s__37191__$2);
var d = (function (){try{return cljs_time.format.parse.cljs$core$IFn$_invoke$arity$2(f,s);
}catch (e37201){if((e37201 instanceof Error)){
var _ = e37201;
return null;
} else {
throw e37201;

}
}})();
if(cljs.core.truth_(d)){
return cljs.core.cons(d,cljs_time$coerce$from_string_$_iter__37190(cljs.core.rest(s__37191__$2)));
} else {
var G__37222 = cljs.core.rest(s__37191__$2);
s__37191__$1 = G__37222;
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
return iter__5628__auto__(cljs.core.vals(cljs_time.format.formatters));
})());
} else {
return null;
}
});
/**
 * Returns a DateTime instance in the UTC time zone corresponding to the given
 *   js Date object.
 */
cljs_time.coerce.from_date = (function cljs_time$coerce$from_date(date){
var G__37202 = date;
var G__37202__$1 = (((G__37202 == null))?null:G__37202.getTime());
if((G__37202__$1 == null)){
return null;
} else {
return cljs_time.coerce.from_long(G__37202__$1);
}
});
/**
 * Convert `obj` to the number of milliseconds after the Unix epoch.
 */
cljs_time.coerce.to_long = (function cljs_time$coerce$to_long(obj){
var G__37203 = obj;
var G__37203__$1 = (((G__37203 == null))?null:cljs_time.coerce.to_date_time(G__37203));
if((G__37203__$1 == null)){
return null;
} else {
return G__37203__$1.getTime();
}
});
/**
 * Convert `obj` to Unix epoch.
 */
cljs_time.coerce.to_epoch = (function cljs_time$coerce$to_epoch(obj){
var millis = cljs_time.coerce.to_long(obj);
var and__5140__auto__ = millis;
if(cljs.core.truth_(and__5140__auto__)){
return cljs.core.quot(millis,(1000));
} else {
return and__5140__auto__;
}
});
/**
 * Convert `obj` to a JavaScript Date instance.
 */
cljs_time.coerce.to_date = (function cljs_time$coerce$to_date(obj){
var G__37204 = obj;
var G__37204__$1 = (((G__37204 == null))?null:cljs_time.coerce.to_date_time(G__37204));
var G__37204__$2 = (((G__37204__$1 == null))?null:G__37204__$1.getTime());
if((G__37204__$2 == null)){
return null;
} else {
return (new Date(G__37204__$2));
}
});
/**
 * Returns a string representation of obj in UTC time-zone
 *   using "yyyy-MM-dd'T'HH:mm:ss.SSSZZ" date-time representation.
 */
cljs_time.coerce.to_string = (function cljs_time$coerce$to_string(obj){
var G__37205 = obj;
var G__37205__$1 = (((G__37205 == null))?null:cljs_time.coerce.to_date_time(G__37205));
if((G__37205__$1 == null)){
return null;
} else {
return cljs_time.format.unparse(new cljs.core.Keyword(null,"date-time","date-time",177938180).cljs$core$IFn$_invoke$arity$1(cljs_time.format.formatters),G__37205__$1);
}
});
/**
 * Convert `obj` to a goog.date.Date instance
 */
cljs_time.coerce.to_local_date = (function cljs_time$coerce$to_local_date(obj){
if(cljs.core.truth_(obj)){
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(goog.date.Date,cljs.core.type(obj))){
return obj;
} else {
var temp__5802__auto__ = cljs_time.coerce.to_date_time(obj);
if(cljs.core.truth_(temp__5802__auto__)){
var dt = temp__5802__auto__;
return (new goog.date.Date(dt.getYear(),dt.getMonth(),dt.getDate()));
} else {
return null;
}
}
} else {
return null;
}
});
/**
 * Convert `obj` to a goog.date.DateTime instance
 */
cljs_time.coerce.to_local_date_time = (function cljs_time$coerce$to_local_date_time(obj){
if(cljs.core.truth_(obj)){
var temp__5802__auto__ = cljs_time.coerce.to_date_time(obj);
if(cljs.core.truth_(temp__5802__auto__)){
var dt = temp__5802__auto__;
var G__37209 = (new goog.date.DateTime(dt.getYear(),dt.getMonth(),dt.getDate()));
G__37209.setHours(dt.getHours());

G__37209.setMinutes(dt.getMinutes());

G__37209.setSeconds(dt.getSeconds());

G__37209.setMilliseconds(dt.getMilliseconds());

return G__37209;
} else {
return null;
}
} else {
return null;
}
});
(cljs_time.coerce.ICoerce["null"] = true);

(cljs_time.coerce.to_date_time["null"] = (function (_){
return null;
}));

(Date.prototype.cljs_time$coerce$ICoerce$ = cljs.core.PROTOCOL_SENTINEL);

(Date.prototype.cljs_time$coerce$ICoerce$to_date_time$arity$1 = (function (date){
var date__$1 = this;
return cljs_time.coerce.from_date(date__$1);
}));

(goog.date.Date.prototype.cljs_time$coerce$ICoerce$ = cljs.core.PROTOCOL_SENTINEL);

(goog.date.Date.prototype.cljs_time$coerce$ICoerce$to_date_time$arity$1 = (function (local_date){
var local_date__$1 = this;
if(cljs.core.truth_(local_date__$1)){
return cljs_time.core.to_utc_time_zone(local_date__$1);
} else {
return null;
}
}));

(goog.date.DateTime.prototype.cljs_time$coerce$ICoerce$ = cljs.core.PROTOCOL_SENTINEL);

(goog.date.DateTime.prototype.cljs_time$coerce$ICoerce$to_date_time$arity$1 = (function (local_date_time){
var local_date_time__$1 = this;
if(cljs.core.truth_(local_date_time__$1)){
return cljs_time.core.to_utc_time_zone(local_date_time__$1);
} else {
return null;
}
}));

(goog.date.UtcDateTime.prototype.cljs_time$coerce$ICoerce$ = cljs.core.PROTOCOL_SENTINEL);

(goog.date.UtcDateTime.prototype.cljs_time$coerce$ICoerce$to_date_time$arity$1 = (function (date_time){
var date_time__$1 = this;
return date_time__$1;
}));

(cljs_time.coerce.ICoerce["number"] = true);

(cljs_time.coerce.to_date_time["number"] = (function (long$){
return cljs_time.coerce.from_long(long$);
}));

(cljs_time.coerce.ICoerce["string"] = true);

(cljs_time.coerce.to_date_time["string"] = (function (string){
return cljs_time.coerce.from_string(string);
}));

//# sourceMappingURL=cljs_time.coerce.js.map
