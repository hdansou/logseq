import "./cljs_env.js";
import "./cljs.core.js";
import "./lambdaisland.glogi.js";
import "./lambdaisland.glogi.print.js";
import "./goog.object.object.js";
import "./goog.debug.console.js";
goog.provide('lambdaisland.glogi.console');
goog.scope(function(){
  lambdaisland.glogi.console.goog$module$goog$object = goog.module.get('goog.object');
});
/**
 * @define {string}
 * @type {string}
 */
lambdaisland.glogi.console.colorize = goog.define("lambdaisland.glogi.console.colorize","auto");
/**
 * @define {string}
 * @type {string}
 */
lambdaisland.glogi.console.timestamp = goog.define("lambdaisland.glogi.console.timestamp","false");
lambdaisland.glogi.console.log_method = (function lambdaisland$glogi$console$log_method(level){
var pred__42851 = (function (p1__42850_SHARP_,p2__42849_SHARP_){
return (p2__42849_SHARP_ >= p1__42850_SHARP_);
});
var expr__42852 = lambdaisland.glogi.level_value(level);
if(pred__42851(lambdaisland.glogi.level_value(new cljs.core.Keyword(null,"severe","severe",-1364500238)),expr__42852)){
return "error";
} else {
if(pred__42851(lambdaisland.glogi.level_value(new cljs.core.Keyword(null,"warning","warning",-1685650671)),expr__42852)){
return "warn";
} else {
if(pred__42851(lambdaisland.glogi.level_value(new cljs.core.Keyword(null,"info","info",-317069002)),expr__42852)){
return "info";
} else {
if(pred__42851(lambdaisland.glogi.level_value(new cljs.core.Keyword(null,"config","config",994861415)),expr__42852)){
return "log";
} else {
return "log";
}
}
}
}
});
lambdaisland.glogi.console.timestamp_now = (function lambdaisland$glogi$console$timestamp_now(){
return cljs.core.subs.cljs$core$IFn$_invoke$arity$3((new Date()).toISOString(),(11),(23));
});
lambdaisland.glogi.console.format_raw = (function lambdaisland$glogi$console$format_raw(p__42856){
var map__42857 = p__42856;
var map__42857__$1 = cljs.core.__destructure_map(map__42857);
var level = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__42857__$1,new cljs.core.Keyword(null,"level","level",1290497552));
var logger_name = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__42857__$1,new cljs.core.Keyword(null,"logger-name","logger-name",-1988126927));
var message = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__42857__$1,new cljs.core.Keyword(null,"message","message",-406056002));
var exception = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__42857__$1,new cljs.core.Keyword(null,"exception","exception",-335277064));
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(""+"["+cljs.core.str.cljs$core$IFn$_invoke$arity$1(logger_name)+"]"),message], null);
});
lambdaisland.glogi.console.format_css = (function lambdaisland$glogi$console$format_css(p__42859){
var map__42861 = p__42859;
var map__42861__$1 = cljs.core.__destructure_map(map__42861);
var level = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__42861__$1,new cljs.core.Keyword(null,"level","level",1290497552));
var logger_name = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__42861__$1,new cljs.core.Keyword(null,"logger-name","logger-name",-1988126927));
var message = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__42861__$1,new cljs.core.Keyword(null,"message","message",-406056002));
var exception = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__42861__$1,new cljs.core.Keyword(null,"exception","exception",-335277064));
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2("true",lambdaisland.glogi.console.timestamp)){
return lambdaisland.glogi.print.format.cljs$core$IFn$_invoke$arity$4(lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$4(lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$4(new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["",cljs.core.PersistentVector.EMPTY], null),lambdaisland.glogi.console.timestamp_now(),new cljs.core.Keyword(null,"white","white",-483998618),new cljs.core.Keyword(null,"black","black",1294279647))," ",new cljs.core.Keyword(null,"black","black",1294279647),new cljs.core.Keyword(null,"white","white",-483998618)),level,logger_name,message);
} else {
return lambdaisland.glogi.print.format.cljs$core$IFn$_invoke$arity$3(level,logger_name,message);
}
});
lambdaisland.glogi.console.format_plain = (function lambdaisland$glogi$console$format_plain(p__42865){
var map__42866 = p__42865;
var map__42866__$1 = cljs.core.__destructure_map(map__42866);
var level = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__42866__$1,new cljs.core.Keyword(null,"level","level",1290497552));
var logger_name = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__42866__$1,new cljs.core.Keyword(null,"logger-name","logger-name",-1988126927));
var message = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__42866__$1,new cljs.core.Keyword(null,"message","message",-406056002));
var exception = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__42866__$1,new cljs.core.Keyword(null,"exception","exception",-335277064));
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2("true",lambdaisland.glogi.console.timestamp))?(""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(lambdaisland.glogi.console.timestamp_now())+" "):null))+"["+cljs.core.str.cljs$core$IFn$_invoke$arity$1(logger_name)+"]"),cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([message], 0))], null);
});
lambdaisland.glogi.console.make_console_log = (function lambdaisland$glogi$console$make_console_log(format){
return (function (p__42867){
var map__42869 = p__42867;
var map__42869__$1 = cljs.core.__destructure_map(map__42869);
var record = map__42869__$1;
var logger_name = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__42869__$1,new cljs.core.Keyword(null,"logger-name","logger-name",-1988126927));
var level = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__42869__$1,new cljs.core.Keyword(null,"level","level",1290497552));
var exception = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__42869__$1,new cljs.core.Keyword(null,"exception","exception",-335277064));
var method_name = lambdaisland.glogi.console.log_method(level);
var method = (function (){var or__5142__auto__ = lambdaisland.glogi.console.goog$module$goog$object.get(console,method_name);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return console.log;
}
})();
cljs.core.apply.cljs$core$IFn$_invoke$arity$2(method,(format.cljs$core$IFn$_invoke$arity$1 ? format.cljs$core$IFn$_invoke$arity$1(record) : format.call(null,record)));

if(cljs.core.truth_(exception)){
var G__42871 = (""+"["+cljs.core.str.cljs$core$IFn$_invoke$arity$1(logger_name)+"]");
var G__42872 = (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(exception));
var G__42873 = "\n";
var G__42874 = exception.stack;
return (method.cljs$core$IFn$_invoke$arity$4 ? method.cljs$core$IFn$_invoke$arity$4(G__42871,G__42872,G__42873,G__42874) : method.call(null,G__42871,G__42872,G__42873,G__42874));
} else {
return null;
}
});
});
if((typeof lambdaisland !== 'undefined') && (typeof lambdaisland.glogi !== 'undefined') && (typeof lambdaisland.glogi.console !== 'undefined') && (typeof lambdaisland.glogi.console.console_log_raw !== 'undefined')){
} else {
lambdaisland.glogi.console.console_log_raw = lambdaisland.glogi.console.make_console_log(lambdaisland.glogi.console.format_raw);
}
if((typeof lambdaisland !== 'undefined') && (typeof lambdaisland.glogi !== 'undefined') && (typeof lambdaisland.glogi.console !== 'undefined') && (typeof lambdaisland.glogi.console.console_log_css !== 'undefined')){
} else {
lambdaisland.glogi.console.console_log_css = lambdaisland.glogi.console.make_console_log(lambdaisland.glogi.console.format_css);
}
if((typeof lambdaisland !== 'undefined') && (typeof lambdaisland.glogi !== 'undefined') && (typeof lambdaisland.glogi.console !== 'undefined') && (typeof lambdaisland.glogi.console.console_log_plain !== 'undefined')){
} else {
lambdaisland.glogi.console.console_log_plain = lambdaisland.glogi.console.make_console_log(lambdaisland.glogi.console.format_plain);
}
if((typeof lambdaisland !== 'undefined') && (typeof lambdaisland.glogi !== 'undefined') && (typeof lambdaisland.glogi.console !== 'undefined') && (typeof lambdaisland.glogi.console.format !== 'undefined')){
} else {
lambdaisland.glogi.console.format = lambdaisland.glogi.console.format_plain;
}
if((typeof lambdaisland !== 'undefined') && (typeof lambdaisland.glogi !== 'undefined') && (typeof lambdaisland.glogi.console !== 'undefined') && (typeof lambdaisland.glogi.console.console_log !== 'undefined')){
} else {
lambdaisland.glogi.console.console_log = lambdaisland.glogi.console.console_log_plain;
}
lambdaisland.glogi.console.devtools_installed_QMARK_ = (function lambdaisland$glogi$console$devtools_installed_QMARK_(){
var and__5140__auto__ = (typeof devtools !== 'undefined') && (typeof devtools.core !== 'undefined') && (typeof devtools.core.installed_QMARK_ !== 'undefined');
if(and__5140__auto__){
return devtools.core.installed_QMARK_();
} else {
return and__5140__auto__;
}
});
lambdaisland.glogi.console.browser_QMARK_ = (function lambdaisland$glogi$console$browser_QMARK_(){
return (typeof window !== 'undefined');
});
lambdaisland.glogi.console.ie_QMARK_ = (function lambdaisland$glogi$console$ie_QMARK_(){
return ((lambdaisland.glogi.console.browser_QMARK_()) && ((((typeof window !== 'undefined') && (typeof window.navigator !== 'undefined')) && ((((typeof window !== 'undefined') && (typeof window.navigator !== 'undefined') && (typeof window.navigator.userAgent !== 'undefined')) && ((((window.navigator.userAgent.indexOf("MSIE") > (-1))) || ((window.navigator.userAgent.indexOf("Trident") > (-1))))))))));
});
lambdaisland.glogi.console.select_handler = (function lambdaisland$glogi$console$select_handler(){
var G__42881 = lambdaisland.glogi.console.colorize;
switch (G__42881) {
case "auto":
if(cljs.core.truth_(lambdaisland.glogi.console.devtools_installed_QMARK_())){
return lambdaisland.glogi.console.console_log_raw;
} else {
if(((lambdaisland.glogi.console.browser_QMARK_()) && ((!(lambdaisland.glogi.console.ie_QMARK_()))))){
return lambdaisland.glogi.console.console_log_css;
} else {
return lambdaisland.glogi.console.console_log_plain;

}
}

break;
case "raw":
return lambdaisland.glogi.console.console_log_raw;

break;
case "true":
return lambdaisland.glogi.console.console_log_css;

break;
case "false":
return lambdaisland.glogi.console.console_log_plain;

break;
default:
throw (new Error((""+"No matching clause: "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(G__42881))));

}
});
lambdaisland.glogi.console.install_BANG_ = (function lambdaisland$glogi$console$install_BANG_(){
var temp__5804__auto___42895 = goog.debug.Console.instance;
if(cljs.core.truth_(temp__5804__auto___42895)){
var instance_42896 = temp__5804__auto___42895;
instance_42896.setCapturing(false);
} else {
}

var G__42884 = lambdaisland.glogi.console.select_handler();
return (lambdaisland.glogi.add_handler_once.cljs$core$IFn$_invoke$arity$1 ? lambdaisland.glogi.add_handler_once.cljs$core$IFn$_invoke$arity$1(G__42884) : lambdaisland.glogi.add_handler_once.call(null,G__42884));
});

//# sourceMappingURL=lambdaisland.glogi.console.js.map
