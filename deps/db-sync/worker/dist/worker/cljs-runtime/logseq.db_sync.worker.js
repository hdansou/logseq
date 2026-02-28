import "./cljs_env.js";
import "./cljs.core.js";
import "./shadow.esm.esm_import$cloudflare_workers.js";
import "./lambdaisland.glogi.js";
import "./logseq.db_sync.common.js";
import "./logseq.db_sync.logging.js";
import "./logseq.db_sync.sentry.worker.js";
import "./logseq.db_sync.worker.dispatch.js";
import "./logseq.db_sync.worker.handler.sync.js";
import "./logseq.db_sync.worker.handler.ws.js";
import "./logseq.db_sync.worker.presence.js";
import "./logseq.db_sync.worker.ws.js";
import "./shadow.cljs.modern.js";
goog.provide('logseq.db_sync.worker');
logseq.db_sync.logging.install_BANG_();
logseq.db_sync.worker.worker = logseq.db_sync.sentry.worker.wrap_handler(({"fetch": (function (request,env,_ctx){
return logseq.db_sync.worker.dispatch.handle_worker_fetch(request,env);
})}));
logseq.db_sync.worker.SyncDO = class logseq$db_sync$worker$SyncDO extends shadow.esm.esm_import$cloudflare_workers.DurableObject {
  constructor(G__41213,G__41214) {
var state_41220 = G__41213;
var env_41221 = G__41214;
super(state_41220,env_41221);
var self__ = this;

(self__.state = state_41220);

(self__.env = env_41221);

(self__.sql = state_41220.storage.sql);

(self__.conn = null);

(self__.schema_ready = false);

var presence_41222 = logseq.db_sync.worker.presence.presence_STAR_(self__);
var sockets_41223 = state_41220.getWebSockets();
var seq__41215_41224 = cljs.core.seq(sockets_41223);
var chunk__41216_41225 = null;
var count__41217_41226 = (0);
var i__41218_41227 = (0);
while(true){
if((i__41218_41227 < count__41217_41226)){
var ws_41228 = chunk__41216_41225.cljs$core$IIndexed$_nth$arity$2(null,i__41218_41227);
var temp__5804__auto___41229 = ws_41228.deserializeAttachment();
if(cljs.core.truth_(temp__5804__auto___41229)){
var attachment_41230 = temp__5804__auto___41229;
cljs.core.swap_BANG_.cljs$core$IFn$_invoke$arity$4(presence_41222,cljs.core.assoc,ws_41228,cljs.core.js__GT_clj.cljs$core$IFn$_invoke$arity$variadic(attachment_41230,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"keywordize-keys","keywordize-keys",1310784252),true], 0)));
} else {
}


var G__41231 = seq__41215_41224;
var G__41232 = chunk__41216_41225;
var G__41233 = count__41217_41226;
var G__41234 = (i__41218_41227 + (1));
seq__41215_41224 = G__41231;
chunk__41216_41225 = G__41232;
count__41217_41226 = G__41233;
i__41218_41227 = G__41234;
continue;
} else {
var temp__5804__auto___41235 = cljs.core.seq(seq__41215_41224);
if(temp__5804__auto___41235){
var seq__41215_41236__$1 = temp__5804__auto___41235;
if(cljs.core.chunked_seq_QMARK_(seq__41215_41236__$1)){
var c__5673__auto___41237 = cljs.core.chunk_first(seq__41215_41236__$1);
var G__41238 = cljs.core.chunk_rest(seq__41215_41236__$1);
var G__41239 = c__5673__auto___41237;
var G__41240 = cljs.core.count(c__5673__auto___41237);
var G__41241 = (0);
seq__41215_41224 = G__41238;
chunk__41216_41225 = G__41239;
count__41217_41226 = G__41240;
i__41218_41227 = G__41241;
continue;
} else {
var ws_41242 = cljs.core.first(seq__41215_41236__$1);
var temp__5804__auto___41243__$1 = ws_41242.deserializeAttachment();
if(cljs.core.truth_(temp__5804__auto___41243__$1)){
var attachment_41244 = temp__5804__auto___41243__$1;
cljs.core.swap_BANG_.cljs$core$IFn$_invoke$arity$4(presence_41222,cljs.core.assoc,ws_41242,cljs.core.js__GT_clj.cljs$core$IFn$_invoke$arity$variadic(attachment_41244,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"keywordize-keys","keywordize-keys",1310784252),true], 0)));
} else {
}


var G__41245 = cljs.core.next(seq__41215_41236__$1);
var G__41246 = null;
var G__41247 = (0);
var G__41248 = (0);
seq__41215_41224 = G__41245;
chunk__41216_41225 = G__41246;
count__41217_41226 = G__41247;
i__41218_41227 = G__41248;
continue;
}
} else {
}
}
break;
}

state_41220.setWebSocketAutoResponse((new WebSocketRequestResponsePair("ping","pong")));
  }
};
(logseq.db_sync.worker.SyncDO.prototype.fetch = (function (request){
var self__ = this;
var this$ = this;
if(logseq.db_sync.common.upgrade_request_QMARK_(request)){
return logseq.db_sync.worker.handler.ws.handle_ws(this$,request);
} else {
return logseq.db_sync.worker.handler.sync.handle_http(this$,request);
}
}));

(logseq.db_sync.worker.SyncDO.prototype.webSocketMessage = (function (ws,message){
var self__ = this;
var this$ = this;
try{return logseq.db_sync.worker.handler.ws.handle_ws_message_BANG_(this$,ws,message);
}catch (e41219){var e = e41219;
logseq.db_sync.sentry.worker.capture_exception_BANG_(e);

if(goog.debug.LOGGING_ENABLED){
lambdaisland.glogi.log.cljs$core$IFn$_invoke$arity$4("logseq.db-sync.worker",new cljs.core.Keyword(null,"error","error",-978969032),cljs.core.identity(new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword("db-sync","ws-error","db-sync/ws-error",727129579),e,new cljs.core.Keyword(null,"line","line",212345235),50], null)),null);
} else {
}

console.error(e);

return logseq.db_sync.worker.ws.send_BANG_(ws,new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"type","type",1174270348),"error",new cljs.core.Keyword(null,"message","message",-406056002),"server error"], null));
}}));

(logseq.db_sync.worker.SyncDO.prototype.webSocketClose = (function (ws,_code,_reason){
var self__ = this;
var this$ = this;
logseq.db_sync.worker.presence.remove_presence_BANG_(this$,ws);

logseq.db_sync.worker.presence.broadcast_online_users_BANG_(this$);

if(goog.debug.LOGGING_ENABLED){
return lambdaisland.glogi.log.cljs$core$IFn$_invoke$arity$4("logseq.db-sync.worker",new cljs.core.Keyword(null,"info","info",-317069002),cljs.core.identity(new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword("db-sync","ws-closed","db-sync/ws-closed",1511656368),true,new cljs.core.Keyword(null,"line","line",212345235),56], null)),null);
} else {
return null;
}
}));

(logseq.db_sync.worker.SyncDO.prototype.webSocketError = (function (ws,error){
var self__ = this;
var this$ = this;
logseq.db_sync.worker.presence.remove_presence_BANG_(this$,ws);

logseq.db_sync.worker.presence.broadcast_online_users_BANG_(this$);

logseq.db_sync.sentry.worker.capture_exception_BANG_(error);

if(goog.debug.LOGGING_ENABLED){
return lambdaisland.glogi.log.cljs$core$IFn$_invoke$arity$4("logseq.db-sync.worker",new cljs.core.Keyword(null,"error","error",-978969032),cljs.core.identity(new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword("db-sync","ws-error","db-sync/ws-error",727129579),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"error","error",-978969032),error], null),new cljs.core.Keyword(null,"line","line",212345235),61], null)),null);
} else {
return null;
}
}));

//# sourceMappingURL=logseq.db_sync.worker.js.map
