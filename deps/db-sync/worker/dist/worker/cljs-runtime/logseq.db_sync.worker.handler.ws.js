import "./cljs_env.js";
import "./cljs.core.js";
import "./logseq.db_sync.protocol.js";
import "./logseq.db_sync.worker.auth.js";
import "./logseq.db_sync.worker.handler.sync.js";
import "./logseq.db_sync.worker.presence.js";
import "./logseq.db_sync.worker.ws.js";
goog.provide('logseq.db_sync.worker.handler.ws');
logseq.db_sync.worker.handler.ws.handle_ws_message_BANG_ = (function logseq$db_sync$worker$handler$ws$handle_ws_message_BANG_(self,ws,raw){
var message = logseq.db_sync.worker.ws.coerce_ws_client_message(logseq.db_sync.protocol.parse_message(raw));
if((!(cljs.core.map_QMARK_(message)))){
return logseq.db_sync.worker.ws.send_BANG_(ws,new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"type","type",1174270348),"error",new cljs.core.Keyword(null,"message","message",-406056002),"invalid request"], null));
} else {
var G__41208 = new cljs.core.Keyword(null,"type","type",1174270348).cljs$core$IFn$_invoke$arity$1(message);
switch (G__41208) {
case "hello":
return logseq.db_sync.worker.ws.send_BANG_(ws,new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"type","type",1174270348),"hello",new cljs.core.Keyword(null,"t","t",-1397832519),logseq.db_sync.worker.handler.sync.t_now(self)], null));

break;
case "ping":
return logseq.db_sync.worker.ws.send_BANG_(ws,new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"type","type",1174270348),"pong"], null));

break;
case "presence":
var editing_block_uuid = new cljs.core.Keyword(null,"editing-block-uuid","editing-block-uuid",-858399034).cljs$core$IFn$_invoke$arity$1(message);
var user = logseq.db_sync.worker.presence.get_user(self,ws);
logseq.db_sync.worker.presence.update_presence_BANG_(self,ws,new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"editing-block-uuid","editing-block-uuid",-858399034),editing_block_uuid], null));

return logseq.db_sync.worker.ws.broadcast_BANG_(self,null,new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"type","type",1174270348),"presence",new cljs.core.Keyword(null,"editing-block-uuid","editing-block-uuid",-858399034),editing_block_uuid,new cljs.core.Keyword(null,"user-id","user-id",-206822291),new cljs.core.Keyword(null,"user-id","user-id",-206822291).cljs$core$IFn$_invoke$arity$1(user)], null));

break;
case "pull":
var raw_since = new cljs.core.Keyword(null,"since","since",315379842).cljs$core$IFn$_invoke$arity$1(message);
var since = (((!((raw_since == null))))?logseq.db_sync.worker.handler.sync.parse_int(raw_since):(0));
if((((((!((raw_since == null)))) && ((!(typeof since === 'number'))))) || ((since < (0))))){
return logseq.db_sync.worker.ws.send_BANG_(ws,new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"type","type",1174270348),"error",new cljs.core.Keyword(null,"message","message",-406056002),"invalid since"], null));
} else {
return logseq.db_sync.worker.ws.send_BANG_(ws,logseq.db_sync.worker.handler.sync.pull_response(self,since));
}

break;
case "tx/batch":
var txs = new cljs.core.Keyword(null,"txs","txs",2056038378).cljs$core$IFn$_invoke$arity$1(message);
var t_before = logseq.db_sync.worker.handler.sync.parse_int(new cljs.core.Keyword(null,"t-before","t-before",-507640180).cljs$core$IFn$_invoke$arity$1(message));
if(typeof txs === 'string'){
return logseq.db_sync.worker.ws.send_BANG_(ws,logseq.db_sync.worker.handler.sync.handle_tx_batch_BANG_(self,ws,txs,t_before));
} else {
return logseq.db_sync.worker.ws.send_BANG_(ws,new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"type","type",1174270348),"tx/reject",new cljs.core.Keyword(null,"reason","reason",-2070751759),"invalid tx"], null));
}

break;
default:
return logseq.db_sync.worker.ws.send_BANG_(ws,new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"type","type",1174270348),"error",new cljs.core.Keyword(null,"message","message",-406056002),"unknown type"], null));

}
}
});
logseq.db_sync.worker.handler.ws.handle_ws = (function logseq$db_sync$worker$handler$ws$handle_ws(self,request){
var pair = (new WebSocketPair());
var client = (pair[(0)]);
var server = (pair[(1)]);
var state = self.state;
state.acceptWebSocket(server);

var token_41210 = logseq.db_sync.worker.auth.token_from_request(request);
var claims_41211 = logseq.db_sync.worker.auth.unsafe_jwt_claims(token_41210);
var user_41212 = logseq.db_sync.worker.presence.claims__GT_user(claims_41211);
if(cljs.core.truth_(user_41212)){
logseq.db_sync.worker.presence.add_presence_BANG_(self,server,user_41212);
} else {
}

logseq.db_sync.worker.presence.broadcast_online_users_BANG_(self);

return (new Response(null,({"status": (101), "webSocket": client})));
});

//# sourceMappingURL=logseq.db_sync.worker.handler.ws.js.map
