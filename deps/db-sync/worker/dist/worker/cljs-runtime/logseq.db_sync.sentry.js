import "./cljs_env.js";
import "./cljs.core.js";
goog.provide('logseq.db_sync.sentry');
logseq.db_sync.sentry.not_blank = (function logseq$db_sync$sentry$not_blank(v){
if(((typeof v === 'string') && (cljs.core.seq(v)))){
return v;
} else {
return null;
}
});
logseq.db_sync.sentry.parse_sample_rate = (function logseq$db_sync$sentry$parse_sample_rate(v){
var n = parseFloat(v);
if(((cljs.core.not(isNaN(n))) && (((((0) <= n)) && ((n <= (1))))))){
return n;
} else {
return null;
}
});
logseq.db_sync.sentry.options_from_env = (function logseq$db_sync$sentry$options_from_env(env){
var dsn = logseq.db_sync.sentry.not_blank((env["SENTRY_DSN"]));
var release = logseq.db_sync.sentry.not_blank((env["SENTRY_RELEASE"]));
var environment = logseq.db_sync.sentry.not_blank((env["SENTRY_ENVIRONMENT"]));
var traces_sample_rate = logseq.db_sync.sentry.parse_sample_rate((env["SENTRY_TRACES_SAMPLE_RATE"]));
if(cljs.core.truth_(dsn)){
var G__42757 = new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"dsn","dsn",1561266567),dsn], null);
var G__42757__$1 = (cljs.core.truth_(release)?cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__42757,new cljs.core.Keyword(null,"release","release",-1534371381),release):G__42757);
var G__42757__$2 = (cljs.core.truth_(environment)?cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__42757__$1,new cljs.core.Keyword(null,"environment","environment",-666037640),environment):G__42757__$1);
if(cljs.core.truth_(traces_sample_rate)){
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__42757__$2,new cljs.core.Keyword(null,"tracesSampleRate","tracesSampleRate",446547798),traces_sample_rate);
} else {
return G__42757__$2;
}
} else {
return null;
}
});

//# sourceMappingURL=logseq.db_sync.sentry.js.map
