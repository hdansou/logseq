import "./cljs_env.js";
import "./cljs.core.js";
import "./clojure.string.js";
import "./logseq.common.authorization.js";
goog.provide('logseq.db_sync.worker.auth');
if((typeof logseq !== 'undefined') && (typeof logseq.db_sync !== 'undefined') && (typeof logseq.db_sync.worker !== 'undefined') && (typeof logseq.db_sync.worker.auth !== 'undefined') && (typeof logseq.db_sync.worker.auth.auth_cache !== 'undefined')){
} else {
logseq.db_sync.worker.auth.auth_cache = (new Map());
}
if((typeof logseq !== 'undefined') && (typeof logseq.db_sync !== 'undefined') && (typeof logseq.db_sync.worker !== 'undefined') && (typeof logseq.db_sync.worker.auth !== 'undefined') && (typeof logseq.db_sync.worker.auth.auth_cache_max_entries !== 'undefined')){
} else {
logseq.db_sync.worker.auth.auth_cache_max_entries = (512);
}
logseq.db_sync.worker.auth.auth_exp_skew_seconds = (5);
logseq.db_sync.worker.auth.now_seconds = (function logseq$db_sync$worker$auth$now_seconds(){
return Math.floor((Date.now() / (1000)));
});
logseq.db_sync.worker.auth.evict_auth_cache_BANG_ = (function logseq$db_sync$worker$auth$evict_auth_cache_BANG_(){
if((logseq.db_sync.worker.auth.auth_cache.size > logseq.db_sync.worker.auth.auth_cache_max_entries)){
var keys_iter = logseq.db_sync.worker.auth.auth_cache.keys();
var first_key = keys_iter.next();
if(cljs.core.truth_(first_key.done)){
return null;
} else {
return logseq.db_sync.worker.auth.auth_cache.delete(first_key.value);
}
} else {
return null;
}
});
logseq.db_sync.worker.auth.cached_claims = (function logseq$db_sync$worker$auth$cached_claims(token){
var entry = logseq.db_sync.worker.auth.auth_cache.get(token);
if(cljs.core.truth_(entry)){
var exp = (entry["exp"]);
var now = logseq.db_sync.worker.auth.now_seconds();
if(((typeof exp === 'number') && ((exp <= (now + logseq.db_sync.worker.auth.auth_exp_skew_seconds))))){
logseq.db_sync.worker.auth.auth_cache.delete(token);

return null;
} else {
return (entry["claims"]);
}
} else {
return null;
}
});
logseq.db_sync.worker.auth.cache_claims_BANG_ = (function logseq$db_sync$worker$auth$cache_claims_BANG_(token,claims){
var exp = (claims["exp"]);
if(((typeof token === 'string') && (typeof exp === 'number'))){
logseq.db_sync.worker.auth.auth_cache.set(token,({"claims": claims, "exp": exp}));

return logseq.db_sync.worker.auth.evict_auth_cache_BANG_();
} else {
return null;
}
});
logseq.db_sync.worker.auth.bearer_token = (function logseq$db_sync$worker$auth$bearer_token(auth_header){
if(((typeof auth_header === 'string') && (clojure.string.starts_with_QMARK_(auth_header,"Bearer ")))){
return cljs.core.subs.cljs$core$IFn$_invoke$arity$2(auth_header,(7));
} else {
return null;
}
});
logseq.db_sync.worker.auth.token_from_request = (function logseq$db_sync$worker$auth$token_from_request(request){
var or__5142__auto__ = logseq.db_sync.worker.auth.bearer_token(request.headers.get("authorization"));
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
var url = (new URL(request.url));
return url.searchParams.get("token");
}
});
logseq.db_sync.worker.auth.static_claims = (function logseq$db_sync$worker$auth$static_claims(env,token){
var expected = (env["DB_SYNC_AUTH_TOKEN"]);
var user_id = (function (){var or__5142__auto__ = (env["DB_SYNC_STATIC_USER_ID"]);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return "user";
}
})();
var email = (env["DB_SYNC_STATIC_EMAIL"]);
var username = (env["DB_SYNC_STATIC_USERNAME"]);
if(((typeof expected === 'string') && (((typeof token === 'string') && (cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(expected,token)))))){
var claims = ({"sub": user_id});
if(typeof email === 'string'){
(claims["email"] = email);
} else {
}

if(typeof username === 'string'){
(claims["username"] = username);
} else {
}

return claims;
} else {
return null;
}
});
logseq.db_sync.worker.auth.none_claims = (function logseq$db_sync$worker$auth$none_claims(env){
var user_id = (function (){var or__5142__auto__ = (env["DB_SYNC_STATIC_USER_ID"]);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return "user";
}
})();
var email = (env["DB_SYNC_STATIC_EMAIL"]);
var username = (env["DB_SYNC_STATIC_USERNAME"]);
var claims = ({"sub": user_id});
if(typeof email === 'string'){
(claims["email"] = email);
} else {
}

if(typeof username === 'string'){
(claims["username"] = username);
} else {
}

return claims;
});
logseq.db_sync.worker.auth.decode_jwt_part = (function logseq$db_sync$worker$auth$decode_jwt_part(part){
var pad = (((cljs.core.mod(cljs.core.count(part),(4)) > (0)))?cljs.core.apply.cljs$core$IFn$_invoke$arity$2(cljs.core.str,cljs.core.repeat.cljs$core$IFn$_invoke$arity$2(((4) - cljs.core.mod(cljs.core.count(part),(4))),"=")):"");
var base64 = clojure.string.replace(clojure.string.replace((""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(part)+cljs.core.str.cljs$core$IFn$_invoke$arity$1(pad)),"-","+"),"_","/");
var raw = atob(base64);
return JSON.parse(raw);
});
logseq.db_sync.worker.auth.unsafe_jwt_claims = (function logseq$db_sync$worker$auth$unsafe_jwt_claims(token){
try{if(typeof token === 'string'){
var parts = clojure.string.split.cljs$core$IFn$_invoke$arity$2(token,/\./);
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2((3),cljs.core.count(parts))){
return logseq.db_sync.worker.auth.decode_jwt_part(cljs.core.nth.cljs$core$IFn$_invoke$arity$2(parts,(1)));
} else {
return null;
}
} else {
return null;
}
}catch (e47884){var _ = e47884;
return null;
}});
logseq.db_sync.worker.auth.auth_claims = (function logseq$db_sync$worker$auth$auth_claims(request,env){
var token = logseq.db_sync.worker.auth.token_from_request(request);
var driver = (function (){var G__47886 = (env["DB_SYNC_AUTH_DRIVER"]);
if((G__47886 == null)){
return null;
} else {
return clojure.string.lower_case(G__47886);
}
})();
var G__47887 = driver;
switch (G__47887) {
case "static":
return Promise.resolve(logseq.db_sync.worker.auth.static_claims(env,token));

break;
case "none":
return Promise.resolve(logseq.db_sync.worker.auth.none_claims(env));

break;
default:
if(typeof token === 'string'){
var temp__5802__auto__ = logseq.db_sync.worker.auth.cached_claims(token);
if(cljs.core.truth_(temp__5802__auto__)){
var claims = temp__5802__auto__;
return Promise.resolve(claims);
} else {
return logseq.common.authorization.verify_jwt(token,env).then((function (claims){
if(cljs.core.truth_(claims)){
logseq.db_sync.worker.auth.cache_claims_BANG_(token,claims);
} else {
}

return claims;
})).catch((function (_){
return null;
}));
}
} else {
return Promise.resolve(null);
}

}
});

//# sourceMappingURL=logseq.db_sync.worker.auth.js.map
