import "./cljs_env.js";
import "./cljs.core.js";
import "./lambdaisland.glogi.js";
import "./logseq.db_sync.malli_schema.js";
import "./logseq.db_sync.protocol.js";
import "./logseq.db_sync.worker.coerce.js";
goog.provide('logseq.db_sync.worker.ws');
logseq.db_sync.worker.ws.ws_open_QMARK_ = (function logseq$db_sync$worker$ws$ws_open_QMARK_(ws){
return cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2((1),ws.readyState);
});
logseq.db_sync.worker.ws.coerce_ws_client_message = (function logseq$db_sync$worker$ws$coerce_ws_client_message(message){
if(cljs.core.truth_(message)){
var coerced = logseq.db_sync.worker.coerce.coerce(logseq.db_sync.malli_schema.ws_client_message_coercer,message,new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"schema","schema",-1582001791),new cljs.core.Keyword("ws","client","ws/client",-1323452753)], null));
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(coerced,logseq.db_sync.worker.coerce.invalid_coerce)){
return null;
} else {
return coerced;
}
} else {
return null;
}
});
logseq.db_sync.worker.ws.coerce_ws_server_message = (function logseq$db_sync$worker$ws$coerce_ws_server_message(message){
if(cljs.core.truth_(message)){
var coerced = logseq.db_sync.worker.coerce.coerce(logseq.db_sync.malli_schema.ws_server_message_coercer,message,new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"schema","schema",-1582001791),new cljs.core.Keyword("ws","server","ws/server",1499210828)], null));
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(coerced,logseq.db_sync.worker.coerce.invalid_coerce)){
return null;
} else {
return coerced;
}
} else {
return null;
}
});
logseq.db_sync.worker.ws.send_BANG_ = (function logseq$db_sync$worker$ws$send_BANG_(ws,msg){
if(logseq.db_sync.worker.ws.ws_open_QMARK_(ws)){
var temp__5802__auto__ = logseq.db_sync.worker.ws.coerce_ws_server_message(msg);
if(cljs.core.truth_(temp__5802__auto__)){
var coerced = temp__5802__auto__;
return ws.send(logseq.db_sync.protocol.encode_message(coerced));
} else {
if(goog.debug.LOGGING_ENABLED){
lambdaisland.glogi.log.cljs$core$IFn$_invoke$arity$4("logseq.db-sync.worker.ws",new cljs.core.Keyword(null,"error","error",-978969032),cljs.core.identity(new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword("db-sync","ws-response-invalid","db-sync/ws-response-invalid",1156358413),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"message","message",-406056002),msg], null),new cljs.core.Keyword(null,"line","line",212345235),27], null)),null);
} else {
}

return ws.send(logseq.db_sync.protocol.encode_message(new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"type","type",1174270348),"error",new cljs.core.Keyword(null,"message","message",-406056002),"server error"], null)));
}
} else {
return null;
}
});
logseq.db_sync.worker.ws.broadcast_BANG_ = (function logseq$db_sync$worker$ws$broadcast_BANG_(self,sender,msg){
var clients = self.state.getWebSockets();
var seq__40761 = cljs.core.seq(clients);
var chunk__40762 = null;
var count__40763 = (0);
var i__40764 = (0);
while(true){
if((i__40764 < count__40763)){
var ws = chunk__40762.cljs$core$IIndexed$_nth$arity$2(null,i__40764);
if(((cljs.core.not_EQ_.cljs$core$IFn$_invoke$arity$2(ws,sender)) && (logseq.db_sync.worker.ws.ws_open_QMARK_(ws)))){
logseq.db_sync.worker.ws.send_BANG_(ws,msg);
} else {
}


var G__40770 = seq__40761;
var G__40771 = chunk__40762;
var G__40772 = count__40763;
var G__40773 = (i__40764 + (1));
seq__40761 = G__40770;
chunk__40762 = G__40771;
count__40763 = G__40772;
i__40764 = G__40773;
continue;
} else {
var temp__5804__auto__ = cljs.core.seq(seq__40761);
if(temp__5804__auto__){
var seq__40761__$1 = temp__5804__auto__;
if(cljs.core.chunked_seq_QMARK_(seq__40761__$1)){
var c__5673__auto__ = cljs.core.chunk_first(seq__40761__$1);
var G__40774 = cljs.core.chunk_rest(seq__40761__$1);
var G__40775 = c__5673__auto__;
var G__40776 = cljs.core.count(c__5673__auto__);
var G__40777 = (0);
seq__40761 = G__40774;
chunk__40762 = G__40775;
count__40763 = G__40776;
i__40764 = G__40777;
continue;
} else {
var ws = cljs.core.first(seq__40761__$1);
if(((cljs.core.not_EQ_.cljs$core$IFn$_invoke$arity$2(ws,sender)) && (logseq.db_sync.worker.ws.ws_open_QMARK_(ws)))){
logseq.db_sync.worker.ws.send_BANG_(ws,msg);
} else {
}


var G__40778 = cljs.core.next(seq__40761__$1);
var G__40779 = null;
var G__40780 = (0);
var G__40781 = (0);
seq__40761 = G__40778;
chunk__40762 = G__40779;
count__40763 = G__40780;
i__40764 = G__40781;
continue;
}
} else {
return null;
}
}
break;
}
});

//# sourceMappingURL=logseq.db_sync.worker.ws.js.map
