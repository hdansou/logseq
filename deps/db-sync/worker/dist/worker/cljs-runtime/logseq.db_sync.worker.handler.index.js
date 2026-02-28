import "./cljs_env.js";
import "./cljs.core.js";
import "./lambdaisland.glogi.js";
import "./logseq.db_sync.common.js";
import "./logseq.db_sync.index.js";
import "./logseq.db_sync.worker.auth.js";
import "./logseq.db_sync.worker.http.js";
import "./logseq.db_sync.worker.routes.index.js";
import "./promesa.core.js";
goog.provide('logseq.db_sync.worker.handler.index');
logseq.db_sync.worker.handler.index.index_db = (function logseq$db_sync$worker$handler$index$index_db(self){
var db = self.d1;
if(cljs.core.truth_(db)){
} else {
if(goog.debug.LOGGING_ENABLED){
lambdaisland.glogi.log.cljs$core$IFn$_invoke$arity$4("logseq.db-sync.worker.handler.index",new cljs.core.Keyword(null,"error","error",-978969032),cljs.core.identity(new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword("db-sync","index-db-missing","db-sync/index-db-missing",-20062686),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"binding","binding",539932593),"DB"], null),new cljs.core.Keyword(null,"line","line",212345235),13], null)),null);
} else {
}
}

return db;
});
logseq.db_sync.worker.handler.index.handle = (function logseq$db_sync$worker$handler$index$handle(p__40697){
var map__40698 = p__40697;
var map__40698__$1 = cljs.core.__destructure_map(map__40698);
var db = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40698__$1,new cljs.core.Keyword(null,"db","db",993250759));
var env = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40698__$1,new cljs.core.Keyword(null,"env","env",-1815813235));
var request = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40698__$1,new cljs.core.Keyword(null,"request","request",1772954723));
var url = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40698__$1,new cljs.core.Keyword(null,"url","url",276297046));
var claims = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40698__$1,new cljs.core.Keyword(null,"claims","claims",1454900969));
var route = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40698__$1,new cljs.core.Keyword(null,"route","route",329891309));
var path_params = new cljs.core.Keyword(null,"path-params","path-params",-48130597).cljs$core$IFn$_invoke$arity$1(route);
var graph_id = new cljs.core.Keyword(null,"graph-id","graph-id",-205404489).cljs$core$IFn$_invoke$arity$1(path_params);
var member_id = new cljs.core.Keyword(null,"member-id","member-id",606659713).cljs$core$IFn$_invoke$arity$1(path_params);
var user_id = (claims["sub"]);
var G__40700 = new cljs.core.Keyword(null,"handler","handler",-195596612).cljs$core$IFn$_invoke$arity$1(route);
var G__40700__$1 = (((G__40700 instanceof cljs.core.Keyword))?G__40700.fqn:null);
switch (G__40700__$1) {
case "graphs/list":
if(typeof user_id === 'string'){
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_index_list(db,user_id)),(function (graphs){
return promesa.protocols._promise(logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("graphs","list","graphs/list",1868883776),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"graphs","graphs",-1584479112),graphs], null)));
}));
}));
} else {
return logseq.db_sync.worker.http.unauthorized();
}

break;
case "graphs/create":
return logseq.db_sync.common.read_json(request).then((function (result){
if((result == null)){
return logseq.db_sync.worker.http.bad_request("missing body");
} else {
var body = cljs.core.js__GT_clj.cljs$core$IFn$_invoke$arity$variadic(result,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"keywordize-keys","keywordize-keys",1310784252),true], 0));
var body__$1 = logseq.db_sync.worker.http.coerce_http_request(new cljs.core.Keyword("graphs","create","graphs/create",1622477539),body);
var graph_id__$1 = (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.random_uuid()));
if((!(typeof user_id === 'string'))){
return logseq.db_sync.worker.http.unauthorized();
} else {
if((body__$1 == null)){
return logseq.db_sync.worker.http.bad_request("invalid body");
} else {
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(body__$1),(function (p__40701){
var map__40702 = p__40701;
var map__40702__$1 = cljs.core.__destructure_map(map__40702);
var graph_name = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40702__$1,new cljs.core.Keyword(null,"graph-name","graph-name",416773857));
var schema_version = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40702__$1,new cljs.core.Keyword(null,"schema-version","schema-version",1117939594));
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_graph_name_exists_QMARK_(db,graph_name,user_id)),(function (name_exists_QMARK_){
return promesa.protocols._promise((cljs.core.truth_(name_exists_QMARK_)?logseq.db_sync.worker.http.bad_request("duplicate graph name"):promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$1){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_index_upsert_BANG_(db,graph_id__$1,graph_name,user_id,schema_version)),(function (_){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_graph_member_upsert_BANG_(db,graph_id__$1,user_id,"manager",user_id)),(function (___$1){
return promesa.protocols._promise(logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("graphs","create","graphs/create",1622477539),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"graph-id","graph-id",-205404489),graph_id__$1], null)));
}));
}));
}))));
}));
}));
}));

}
}
}
}));

break;
case "graphs/access":
if((!(typeof user_id === 'string'))){
return logseq.db_sync.worker.http.unauthorized();
} else {
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_user_has_access_to_graph_QMARK_(db,graph_id,user_id)),(function (owns_QMARK_){
return promesa.protocols._promise((cljs.core.truth_(owns_QMARK_)?logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("graphs","access","graphs/access",788024307),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"ok","ok",967785236),true], null)):logseq.db_sync.worker.http.forbidden()));
}));
}));

}

break;
case "graph-members/list":
if((!(typeof user_id === 'string'))){
return logseq.db_sync.worker.http.unauthorized();
} else {
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_user_has_access_to_graph_QMARK_(db,graph_id,user_id)),(function (can_access_QMARK_){
return promesa.protocols._promise(((cljs.core.not(can_access_QMARK_))?logseq.db_sync.worker.http.forbidden():promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$1){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_graph_members_list(db,graph_id)),(function (members){
return promesa.protocols._promise(logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("graph-members","list","graph-members/list",-1059934571),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"members","members",159001018),members], null)));
}));
}))));
}));
}));

}

break;
case "graph-members/create":
if((!(typeof user_id === 'string'))){
return logseq.db_sync.worker.http.unauthorized();
} else {
return logseq.db_sync.common.read_json(request).then((function (result){
if((result == null)){
return logseq.db_sync.worker.http.bad_request("missing body");
} else {
var body = cljs.core.js__GT_clj.cljs$core$IFn$_invoke$arity$variadic(result,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"keywordize-keys","keywordize-keys",1310784252),true], 0));
var body__$1 = logseq.db_sync.worker.http.coerce_http_request(new cljs.core.Keyword("graph-members","create","graph-members/create",1356984878),body);
var member_id__$1 = new cljs.core.Keyword(null,"user-id","user-id",-206822291).cljs$core$IFn$_invoke$arity$1(body__$1);
var email = new cljs.core.Keyword(null,"email","email",1415816706).cljs$core$IFn$_invoke$arity$1(body__$1);
var role = (function (){var or__5142__auto__ = new cljs.core.Keyword(null,"role","role",-736691072).cljs$core$IFn$_invoke$arity$1(body__$1);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return "member";
}
})();
if((body__$1 == null)){
return logseq.db_sync.worker.http.bad_request("invalid body");
} else {
if((((!(typeof member_id__$1 === 'string'))) && ((!(typeof email === 'string'))))){
return logseq.db_sync.worker.http.bad_request("invalid user");
} else {
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_user_is_manager_QMARK_(db,graph_id,user_id)),(function (manager_QMARK_){
return promesa.protocols._mcat(promesa.protocols._promise(((typeof member_id__$1 === 'string')?promesa.core.resolved(member_id__$1):logseq.db_sync.index._LT_user_id_by_email(db,email))),(function (resolved_id){
return promesa.protocols._promise(((cljs.core.not(manager_QMARK_))?logseq.db_sync.worker.http.forbidden():(((!(typeof resolved_id === 'string')))?logseq.db_sync.worker.http.bad_request("user not found"):promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$1){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_graph_member_upsert_BANG_(db,graph_id,resolved_id,role,user_id)),(function (_){
return promesa.protocols._promise(logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("graph-members","create","graph-members/create",1356984878),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"ok","ok",967785236),true], null)));
}));
})))));
}));
}));
}));

}
}
}
}));

}

break;
case "graph-members/update":
if((!(typeof user_id === 'string'))){
return logseq.db_sync.worker.http.unauthorized();
} else {
if((!(typeof member_id === 'string'))){
return logseq.db_sync.worker.http.bad_request("invalid user id");
} else {
return logseq.db_sync.common.read_json(request).then((function (result){
if((result == null)){
return logseq.db_sync.worker.http.bad_request("missing body");
} else {
var body = cljs.core.js__GT_clj.cljs$core$IFn$_invoke$arity$variadic(result,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"keywordize-keys","keywordize-keys",1310784252),true], 0));
var body__$1 = logseq.db_sync.worker.http.coerce_http_request(new cljs.core.Keyword("graph-members","update","graph-members/update",-639206622),body);
var role = new cljs.core.Keyword(null,"role","role",-736691072).cljs$core$IFn$_invoke$arity$1(body__$1);
if((body__$1 == null)){
return logseq.db_sync.worker.http.bad_request("invalid body");
} else {
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_user_is_manager_QMARK_(db,graph_id,user_id)),(function (manager_QMARK_){
return promesa.protocols._promise(((cljs.core.not(manager_QMARK_))?logseq.db_sync.worker.http.forbidden():promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$1){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_graph_member_update_role_BANG_(db,graph_id,member_id,role)),(function (_){
return promesa.protocols._promise(logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("graph-members","update","graph-members/update",-639206622),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"ok","ok",967785236),true], null)));
}));
}))));
}));
}));

}
}
}));

}
}

break;
case "graph-members/delete":
if((!(typeof user_id === 'string'))){
return logseq.db_sync.worker.http.unauthorized();
} else {
if((!(typeof member_id === 'string'))){
return logseq.db_sync.worker.http.bad_request("invalid user id");
} else {
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_user_is_manager_QMARK_(db,graph_id,user_id)),(function (manager_QMARK_){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_graph_member_role(db,graph_id,member_id)),(function (target_role){
return promesa.protocols._mcat(promesa.protocols._promise(((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(user_id,member_id)) && (cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2("member",target_role)))),(function (self_leave_QMARK_){
return promesa.protocols._promise((cljs.core.truth_((function (){var and__5140__auto__ = manager_QMARK_;
if(cljs.core.truth_(and__5140__auto__)){
return cljs.core.not_EQ_.cljs$core$IFn$_invoke$arity$2("manager",target_role);
} else {
return and__5140__auto__;
}
})())?promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$1){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_graph_member_delete_BANG_(db,graph_id,member_id)),(function (_){
return promesa.protocols._promise(logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("graph-members","delete","graph-members/delete",-249361854),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"ok","ok",967785236),true], null)));
}));
})):(cljs.core.truth_(self_leave_QMARK_)?promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$1){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_graph_member_delete_BANG_(db,graph_id,member_id)),(function (_){
return promesa.protocols._promise(logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("graph-members","delete","graph-members/delete",-249361854),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"ok","ok",967785236),true], null)));
}));
})):logseq.db_sync.worker.http.forbidden()
)));
}));
}));
}));
}));

}
}

break;
case "graphs/delete":
if(cljs.core.not(cljs.core.seq(graph_id))){
return logseq.db_sync.worker.http.bad_request("missing graph id");
} else {
if((!(typeof user_id === 'string'))){
return logseq.db_sync.worker.http.unauthorized();
} else {
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_user_has_access_to_graph_QMARK_(db,graph_id,user_id)),(function (owns_QMARK_){
return promesa.protocols._promise(((cljs.core.not(owns_QMARK_))?logseq.db_sync.worker.http.forbidden():promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$1){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_index_delete_BANG_(db,graph_id)),(function (_){
return promesa.protocols._mcat(promesa.protocols._promise((function (){var namespace = env.LOGSEQ_SYNC_DO;
var do_id = namespace.idFromName(graph_id);
var stub = namespace.get(do_id);
var reset_url = (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(url.origin)+"/admin/reset");
return stub.fetch((new Request(reset_url,({"method": "DELETE"}))));
})()),(function (___35454__auto__){
return promesa.protocols._promise(logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("graphs","delete","graphs/delete",1077756941),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"graph-id","graph-id",-205404489),graph_id,new cljs.core.Keyword(null,"deleted","deleted",-510100639),true], null)));
}));
}));
}))));
}));
}));

}
}

break;
case "e2ee/user-keys-get":
if(typeof user_id === 'string'){
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_user_rsa_key_pair(db,user_id)),(function (pair){
return promesa.protocols._promise(logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("e2ee","user-keys","e2ee/user-keys",13227013),(function (){var or__5142__auto__ = pair;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return cljs.core.PersistentArrayMap.EMPTY;
}
})()));
}));
}));
} else {
return logseq.db_sync.worker.http.unauthorized();
}

break;
case "e2ee/user-keys-post":
return logseq.db_sync.common.read_json(request).then((function (result){
if((result == null)){
return logseq.db_sync.worker.http.bad_request("missing body");
} else {
var body = cljs.core.js__GT_clj.cljs$core$IFn$_invoke$arity$variadic(result,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"keywordize-keys","keywordize-keys",1310784252),true], 0));
var body__$1 = logseq.db_sync.worker.http.coerce_http_request(new cljs.core.Keyword("e2ee","user-keys","e2ee/user-keys",13227013),body);
if((!(typeof user_id === 'string'))){
return logseq.db_sync.worker.http.unauthorized();
} else {
if((body__$1 == null)){
return logseq.db_sync.worker.http.bad_request("invalid body");
} else {
var map__40769 = body__$1;
var map__40769__$1 = cljs.core.__destructure_map(map__40769);
var public_key = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40769__$1,new cljs.core.Keyword(null,"public-key","public-key",-2106850051));
var encrypted_private_key = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40769__$1,new cljs.core.Keyword(null,"encrypted-private-key","encrypted-private-key",1069899594));
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_user_rsa_key_pair_upsert_BANG_(db,user_id,public_key,encrypted_private_key)),(function (_){
return promesa.protocols._promise(logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("e2ee","user-keys","e2ee/user-keys",13227013),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"public-key","public-key",-2106850051),public_key,new cljs.core.Keyword(null,"encrypted-private-key","encrypted-private-key",1069899594),encrypted_private_key], null)));
}));
}));

}
}
}
}));

break;
case "e2ee/user-public-key-get":
var email = url.searchParams.get("email");
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_user_rsa_public_key_by_email(db,email)),(function (public_key){
return promesa.protocols._promise(logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("e2ee","user-public-key","e2ee/user-public-key",-1282741690),(function (){var G__40785 = cljs.core.PersistentArrayMap.EMPTY;
if((!((public_key == null)))){
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__40785,new cljs.core.Keyword(null,"public-key","public-key",-2106850051),public_key);
} else {
return G__40785;
}
})()));
}));
}));

break;
case "e2ee/graph-aes-key-get":
if((!(typeof user_id === 'string'))){
return logseq.db_sync.worker.http.unauthorized();
} else {
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_user_has_access_to_graph_QMARK_(db,graph_id,user_id)),(function (access_QMARK_){
return promesa.protocols._promise(((cljs.core.not(access_QMARK_))?logseq.db_sync.worker.http.forbidden():promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$1){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_graph_encrypted_aes_key(db,graph_id,user_id)),(function (encrypted_aes_key){
return promesa.protocols._promise(logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("e2ee","graph-aes-key","e2ee/graph-aes-key",-307279709),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"encrypted-aes-key","encrypted-aes-key",-1115865399),encrypted_aes_key], null)));
}));
}))));
}));
}));

}

break;
case "e2ee/graph-aes-key-post":
if((!(typeof user_id === 'string'))){
return logseq.db_sync.worker.http.unauthorized();
} else {
return logseq.db_sync.common.read_json(request).then((function (result){
if((result == null)){
return logseq.db_sync.worker.http.bad_request("missing body");
} else {
var body = cljs.core.js__GT_clj.cljs$core$IFn$_invoke$arity$variadic(result,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"keywordize-keys","keywordize-keys",1310784252),true], 0));
var body__$1 = logseq.db_sync.worker.http.coerce_http_request(new cljs.core.Keyword("e2ee","graph-aes-key","e2ee/graph-aes-key",-307279709),body);
if((body__$1 == null)){
return logseq.db_sync.worker.http.bad_request("invalid body");
} else {
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_user_has_access_to_graph_QMARK_(db,graph_id,user_id)),(function (access_QMARK_){
return promesa.protocols._promise(((cljs.core.not(access_QMARK_))?logseq.db_sync.worker.http.forbidden():(function (){var map__40792 = body__$1;
var map__40792__$1 = cljs.core.__destructure_map(map__40792);
var encrypted_aes_key = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40792__$1,new cljs.core.Keyword(null,"encrypted-aes-key","encrypted-aes-key",-1115865399));
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$1){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_graph_encrypted_aes_key_upsert_BANG_(db,graph_id,user_id,encrypted_aes_key)),(function (_){
return promesa.protocols._promise(logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("e2ee","graph-aes-key","e2ee/graph-aes-key",-307279709),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"encrypted-aes-key","encrypted-aes-key",-1115865399),encrypted_aes_key], null)));
}));
}));
})()));
}));
}));
}
}
}));

}

break;
case "e2ee/grant-access":
if((!(typeof user_id === 'string'))){
return logseq.db_sync.worker.http.unauthorized();
} else {
return logseq.db_sync.common.read_json(request).then((function (result){
if((result == null)){
return logseq.db_sync.worker.http.bad_request("missing body");
} else {
var body = cljs.core.js__GT_clj.cljs$core$IFn$_invoke$arity$variadic(result,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"keywordize-keys","keywordize-keys",1310784252),true], 0));
var body__$1 = logseq.db_sync.worker.http.coerce_http_request(new cljs.core.Keyword("e2ee","grant-access","e2ee/grant-access",-971940261),body);
if((body__$1 == null)){
return logseq.db_sync.worker.http.bad_request("invalid body");
} else {
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_user_is_manager_QMARK_(db,graph_id,user_id)),(function (manager_QMARK_){
return promesa.protocols._promise(((cljs.core.not(manager_QMARK_))?logseq.db_sync.worker.http.forbidden():(function (){var entries = new cljs.core.Keyword(null,"target-user-email+encrypted-aes-key-coll","target-user-email+encrypted-aes-key-coll",-393868366).cljs$core$IFn$_invoke$arity$1(body__$1);
var missing = cljs.core.atom.cljs$core$IFn$_invoke$arity$1(cljs.core.PersistentVector.EMPTY);
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$1){
return promesa.protocols._mcat(promesa.protocols._promise(promesa.core.all(cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (entry){
var email = new cljs.core.Keyword(null,"email","email",1415816706).cljs$core$IFn$_invoke$arity$1(entry);
var encrypted_aes_key = new cljs.core.Keyword(null,"encrypted-aes-key","encrypted-aes-key",-1115865399).cljs$core$IFn$_invoke$arity$1(entry);
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$2){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.index._LT_user_id_by_email(db,email)),(function (target_user_id){
return promesa.protocols._mcat(promesa.protocols._promise((function (){var and__5140__auto__ = target_user_id;
if(cljs.core.truth_(and__5140__auto__)){
return logseq.db_sync.index._LT_user_has_access_to_graph_QMARK_(db,graph_id,target_user_id);
} else {
return and__5140__auto__;
}
})()),(function (access_QMARK_){
return promesa.protocols._promise((cljs.core.truth_((function (){var and__5140__auto__ = target_user_id;
if(cljs.core.truth_(and__5140__auto__)){
return access_QMARK_;
} else {
return and__5140__auto__;
}
})())?logseq.db_sync.index._LT_graph_encrypted_aes_key_upsert_BANG_(db,graph_id,target_user_id,encrypted_aes_key):cljs.core.swap_BANG_.cljs$core$IFn$_invoke$arity$3(missing,cljs.core.conj,email)));
}));
}));
}));
}),entries))),(function (_){
return promesa.protocols._promise(logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("e2ee","grant-access","e2ee/grant-access",-971940261),(function (){var G__40802 = new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"ok","ok",967785236),true], null);
if(cljs.core.seq(cljs.core.deref(missing))){
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__40802,new cljs.core.Keyword(null,"missing-users","missing-users",523787750),cljs.core.deref(missing));
} else {
return G__40802;
}
})()));
}));
}));
})()));
}));
}));
}
}
}));
}

break;
default:
return logseq.db_sync.worker.http.not_found();

}
});
logseq.db_sync.worker.handler.index.handle_fetch = (function logseq$db_sync$worker$handler$index$handle_fetch(self,request){
var db = logseq.db_sync.worker.handler.index.index_db(self);
var env = self.env;
var url = (new URL(request.url));
var path = url.pathname;
var method = request.method;
try{if(cljs.core.contains_QMARK_(new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 2, ["OPTIONS",null,"HEAD",null], null), null),method)){
return logseq.db_sync.common.options_response();
} else {
if((db == null)){
return logseq.db_sync.worker.http.error_response("server error",(500));
} else {
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.worker.auth.auth_claims(request,env)),(function (claims){
return promesa.protocols._mcat(promesa.protocols._promise((cljs.core.truth_(claims)?logseq.db_sync.index._LT_user_upsert_BANG_(db,claims):null)),(function (_){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.worker.routes.index.match_route(method,path)),(function (route){
return promesa.protocols._mcat(promesa.protocols._promise((((claims == null))?logseq.db_sync.worker.http.unauthorized():(cljs.core.truth_(route)?logseq.db_sync.worker.handler.index.handle(new cljs.core.PersistentArrayMap(null, 6, [new cljs.core.Keyword(null,"db","db",993250759),db,new cljs.core.Keyword(null,"env","env",-1815813235),env,new cljs.core.Keyword(null,"request","request",1772954723),request,new cljs.core.Keyword(null,"url","url",276297046),url,new cljs.core.Keyword(null,"claims","claims",1454900969),claims,new cljs.core.Keyword(null,"route","route",329891309),route], null)):logseq.db_sync.worker.http.not_found()
))),(function (response){
return promesa.protocols._promise(response);
}));
}));
}));
}));
}));

}
}
}catch (e40807){var error = e40807;
if(goog.debug.LOGGING_ENABLED){
lambdaisland.glogi.log.cljs$core$IFn$_invoke$arity$4("logseq.db-sync.worker.handler.index",new cljs.core.Keyword(null,"error","error",-978969032),cljs.core.identity(new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword("db-sync","index-error","db-sync/index-error",-1180121082),error,new cljs.core.Keyword(null,"line","line",212345235),318], null)),null);
} else {
}

return logseq.db_sync.worker.http.error_response("server error",(500));
}});
logseq.db_sync.worker.handler.index.graph_access_response = (function logseq$db_sync$worker$handler$index$graph_access_response(request,env,graph_id){
var token = logseq.db_sync.worker.auth.token_from_request(request);
var url = (new URL(request.url));
var access_url = (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(url.origin)+"/graphs/"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(graph_id)+"/access");
var headers = (new Headers(request.headers));
var index_self = ({"env": env, "d1": (env["DB"])});
if(typeof token === 'string'){
headers.set("authorization",(""+"Bearer "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(token)));
} else {
}

return logseq.db_sync.worker.handler.index.handle_fetch(index_self,(new Request(access_url,({"method": "GET", "headers": headers}))));
});

//# sourceMappingURL=logseq.db_sync.worker.handler.index.js.map
