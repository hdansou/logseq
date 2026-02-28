import "./cljs_env.js";
import "./cljs.core.js";
import "./lambdaisland.glogi.js";
goog.provide('logseq.db_sync.worker.coerce');
logseq.db_sync.worker.coerce.invalid_coerce = new cljs.core.Keyword("logseq.db-sync.worker.coerce","invalid-coerce","logseq.db-sync.worker.coerce/invalid-coerce",577165127);
logseq.db_sync.worker.coerce.coerce = (function logseq$db_sync$worker$coerce$coerce(coercer,value,context){
try{return (coercer.cljs$core$IFn$_invoke$arity$1 ? coercer.cljs$core$IFn$_invoke$arity$1(value) : coercer.call(null,value));
}catch (e47800){var e = e47800;
if(goog.debug.LOGGING_ENABLED){
lambdaisland.glogi.log.cljs$core$IFn$_invoke$arity$4("logseq.db-sync.worker.coerce",new cljs.core.Keyword(null,"error","error",-978969032),cljs.core.identity(new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword("db-sync","malli-coerce-failed","db-sync/malli-coerce-failed",-2140286978),cljs.core.merge.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([context,new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"error","error",-978969032),e,new cljs.core.Keyword(null,"value","value",305978217),value], null)], 0)),new cljs.core.Keyword(null,"line","line",212345235),11], null)),null);
} else {
}

return logseq.db_sync.worker.coerce.invalid_coerce;
}});

//# sourceMappingURL=logseq.db_sync.worker.coerce.js.map
