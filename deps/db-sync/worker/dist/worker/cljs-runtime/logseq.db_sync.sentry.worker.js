import "./cljs_env.js";
import "./cljs.core.js";
import "./shadow.esm.esm_import$$sentry$cloudflare.js";
import "./logseq.db_sync.sentry.js";
goog.provide('logseq.db_sync.sentry.worker');
logseq.db_sync.sentry.worker.wrap_handler = (function logseq$db_sync$sentry$worker$wrap_handler(handler){
return shadow.esm.esm_import$$sentry$cloudflare.withSentry((function (env){
return cljs.core.clj__GT_js((function (){var or__5142__auto__ = logseq.db_sync.sentry.options_from_env(env);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return cljs.core.PersistentArrayMap.EMPTY;
}
})());
}),handler);
});
logseq.db_sync.sentry.worker.capture_exception_BANG_ = (function logseq$db_sync$sentry$worker$capture_exception_BANG_(error){
return shadow.esm.esm_import$$sentry$cloudflare.captureException(error);
});

//# sourceMappingURL=logseq.db_sync.sentry.worker.js.map
