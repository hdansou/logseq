import "./cljs_env.js";
import "./cljs.core.js";
import "./clojure.string.js";
import "./logseq.db_sync.worker.ws.js";
goog.provide('logseq.db_sync.worker.presence');
logseq.db_sync.worker.presence.claims__GT_user = (function logseq$db_sync$worker$presence$claims__GT_user(claims){
if(cljs.core.truth_(claims)){
var user_id = (claims["sub"]);
var email = (claims["email"]);
var username = (function (){var or__5142__auto__ = (claims["preferred_username"]);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
var or__5142__auto____$1 = (claims["cognito:username"]);
if(cljs.core.truth_(or__5142__auto____$1)){
return or__5142__auto____$1;
} else {
return (claims["username"]);
}
}
})();
var name = (claims["name"]);
if(typeof user_id === 'string'){
var G__41005 = new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"user-id","user-id",-206822291),user_id], null);
var G__41005__$1 = ((typeof email === 'string')?cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__41005,new cljs.core.Keyword(null,"email","email",1415816706),email):G__41005);
var G__41005__$2 = ((typeof username === 'string')?cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__41005__$1,new cljs.core.Keyword(null,"username","username",1605666410),username):G__41005__$1);
if(typeof name === 'string'){
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__41005__$2,new cljs.core.Keyword(null,"name","name",1843675177),name);
} else {
return G__41005__$2;
}
} else {
return null;
}
} else {
return null;
}
});
logseq.db_sync.worker.presence.presence_STAR_ = (function logseq$db_sync$worker$presence$presence_STAR_(self){
var or__5142__auto__ = self.presence;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return (self.presence = cljs.core.atom.cljs$core$IFn$_invoke$arity$1(cljs.core.PersistentArrayMap.EMPTY));
}
});
logseq.db_sync.worker.presence.online_users = (function logseq$db_sync$worker$presence$online_users(self){
return cljs.core.vec(cljs.core.distinct.cljs$core$IFn$_invoke$arity$1(cljs.core.vals(cljs.core.deref(logseq.db_sync.worker.presence.presence_STAR_(self)))));
});
logseq.db_sync.worker.presence.broadcast_online_users_BANG_ = (function logseq$db_sync$worker$presence$broadcast_online_users_BANG_(self){
return logseq.db_sync.worker.ws.broadcast_BANG_(self,null,new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"type","type",1174270348),"online-users",new cljs.core.Keyword(null,"online-users","online-users",-747563810),logseq.db_sync.worker.presence.online_users(self)], null));
});
logseq.db_sync.worker.presence.add_presence_BANG_ = (function logseq$db_sync$worker$presence$add_presence_BANG_(self,ws,user){
cljs.core.swap_BANG_.cljs$core$IFn$_invoke$arity$4(logseq.db_sync.worker.presence.presence_STAR_(self),cljs.core.assoc,ws,user);

return ws.serializeAttachment(cljs.core.clj__GT_js(user));
});
logseq.db_sync.worker.presence.update_presence_BANG_ = (function logseq$db_sync$worker$presence$update_presence_BANG_(self,ws,p__41011){
var map__41012 = p__41011;
var map__41012__$1 = cljs.core.__destructure_map(map__41012);
var updates = map__41012__$1;
var editing_block_uuid = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__41012__$1,new cljs.core.Keyword(null,"editing-block-uuid","editing-block-uuid",-858399034));
return cljs.core.swap_BANG_.cljs$core$IFn$_invoke$arity$2(logseq.db_sync.worker.presence.presence_STAR_(self),(function (presence){
var temp__5802__auto__ = cljs.core.get.cljs$core$IFn$_invoke$arity$2(presence,ws);
if(cljs.core.truth_(temp__5802__auto__)){
var user = temp__5802__auto__;
var user_SINGLEQUOTE_ = ((cljs.core.contains_QMARK_(updates,new cljs.core.Keyword(null,"editing-block-uuid","editing-block-uuid",-858399034)))?((((typeof editing_block_uuid === 'string') && ((!(clojure.string.blank_QMARK_(editing_block_uuid))))))?cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(user,new cljs.core.Keyword(null,"editing-block-uuid","editing-block-uuid",-858399034),editing_block_uuid):cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(user,new cljs.core.Keyword(null,"editing-block-uuid","editing-block-uuid",-858399034))):user);
ws.serializeAttachment(cljs.core.clj__GT_js(user_SINGLEQUOTE_));

return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(presence,ws,user_SINGLEQUOTE_);
} else {
return presence;
}
}));
});
logseq.db_sync.worker.presence.get_user = (function logseq$db_sync$worker$presence$get_user(self,ws){
return cljs.core.get.cljs$core$IFn$_invoke$arity$2(cljs.core.deref(logseq.db_sync.worker.presence.presence_STAR_(self)),ws);
});
logseq.db_sync.worker.presence.remove_presence_BANG_ = (function logseq$db_sync$worker$presence$remove_presence_BANG_(self,ws){
return cljs.core.swap_BANG_.cljs$core$IFn$_invoke$arity$3(logseq.db_sync.worker.presence.presence_STAR_(self),cljs.core.dissoc,ws);
});

//# sourceMappingURL=logseq.db_sync.worker.presence.js.map
