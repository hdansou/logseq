import "./cljs_env.js";
import "./cljs.core.js";
import "./clojure.string.js";
import "./promesa.core.js";
goog.provide('logseq.common.authorization');
logseq.common.authorization.text_decoder = (new TextDecoder());
logseq.common.authorization.text_encoder = (new TextEncoder());
logseq.common.authorization.jwks_ttl_ms = ((((6) * (60)) * (60)) * (1000));
logseq.common.authorization.token_ttl_ms = (((60) * (60)) * (1000));
logseq.common.authorization.token_capacity = (200);
if((typeof logseq !== 'undefined') && (typeof logseq.common !== 'undefined') && (typeof logseq.common.authorization !== 'undefined') && (typeof logseq.common.authorization._STAR_jwks_cache !== 'undefined')){
} else {
logseq.common.authorization._STAR_jwks_cache = cljs.core.atom.cljs$core$IFn$_invoke$arity$1(new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"url","url",276297046),null,new cljs.core.Keyword(null,"keys","keys",1068423698),null,new cljs.core.Keyword(null,"fetched-at","fetched-at",1252416344),(0)], null));
}
if((typeof logseq !== 'undefined') && (typeof logseq.common !== 'undefined') && (typeof logseq.common.authorization !== 'undefined') && (typeof logseq.common.authorization._STAR_token_cache !== 'undefined')){
} else {
logseq.common.authorization._STAR_token_cache = cljs.core.atom.cljs$core$IFn$_invoke$arity$1(cljs.core.PersistentArrayMap.EMPTY);
}
logseq.common.authorization.get_now_ms = (function logseq$common$authorization$get_now_ms(){
return Date.now();
});
logseq.common.authorization.cached_token = (function logseq$common$authorization$cached_token(token,now_s,now_ms){
var temp__5804__auto__ = cljs.core.get.cljs$core$IFn$_invoke$arity$2(cljs.core.deref(logseq.common.authorization._STAR_token_cache),token);
if(cljs.core.truth_(temp__5804__auto__)){
var map__47843 = temp__5804__auto__;
var map__47843__$1 = cljs.core.__destructure_map(map__47843);
var payload = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__47843__$1,new cljs.core.Keyword(null,"payload","payload",-383036092));
var exp = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__47843__$1,new cljs.core.Keyword(null,"exp","exp",-261706262));
var cached_at = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__47843__$1,new cljs.core.Keyword(null,"cached-at","cached-at",2133976632));
if(((typeof exp === 'number') && ((((exp > now_s)) && (((now_ms - cached_at) < logseq.common.authorization.token_ttl_ms)))))){
return payload;
} else {
return null;
}
} else {
return null;
}
});
logseq.common.authorization.cache_token_BANG_ = (function logseq$common$authorization$cache_token_BANG_(token,payload,now_ms){
var exp = (payload["exp"]);
if(typeof exp === 'number'){
cljs.core.swap_BANG_.cljs$core$IFn$_invoke$arity$4(logseq.common.authorization._STAR_token_cache,cljs.core.assoc,token,new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"payload","payload",-383036092),payload,new cljs.core.Keyword(null,"exp","exp",-261706262),exp,new cljs.core.Keyword(null,"cached-at","cached-at",2133976632),now_ms], null));
} else {
}

if((cljs.core.count(cljs.core.deref(logseq.common.authorization._STAR_token_cache)) > logseq.common.authorization.token_capacity)){
return cljs.core.swap_BANG_.cljs$core$IFn$_invoke$arity$2(logseq.common.authorization._STAR_token_cache,(function (cache){
return cljs.core.into.cljs$core$IFn$_invoke$arity$3(cljs.core.PersistentArrayMap.EMPTY,cljs.core.remove.cljs$core$IFn$_invoke$arity$1((function (p__47844){
var vec__47845 = p__47844;
var _ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__47845,(0),null);
var map__47848 = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__47845,(1),null);
var map__47848__$1 = cljs.core.__destructure_map(map__47848);
var exp__$1 = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__47848__$1,new cljs.core.Keyword(null,"exp","exp",-261706262));
var cached_at = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__47848__$1,new cljs.core.Keyword(null,"cached-at","cached-at",2133976632));
return (((!(typeof exp__$1 === 'number'))) || ((((exp__$1 <= Math.floor((now_ms / (1000))))) || (((now_ms - cached_at) >= logseq.common.authorization.token_ttl_ms)))));
})),cache);
}));
} else {
return null;
}
});
logseq.common.authorization.get_jwks_keys = (function logseq$common$authorization$get_jwks_keys(var_args){
var args__5882__auto__ = [];
var len__5876__auto___47864 = arguments.length;
var i__5877__auto___47865 = (0);
while(true){
if((i__5877__auto___47865 < len__5876__auto___47864)){
args__5882__auto__.push((arguments[i__5877__auto___47865]));

var G__47866 = (i__5877__auto___47865 + (1));
i__5877__auto___47865 = G__47866;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((1) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((1)),(0),null)):null);
return logseq.common.authorization.get_jwks_keys.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),argseq__5883__auto__);
});

(logseq.common.authorization.get_jwks_keys.cljs$core$IFn$_invoke$arity$variadic = (function (url,p__47851){
var map__47852 = p__47851;
var map__47852__$1 = cljs.core.__destructure_map(map__47852);
var force_QMARK_ = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__47852__$1,new cljs.core.Keyword(null,"force?","force?",1839038675));
var now = logseq.common.authorization.get_now_ms();
var map__47853 = new cljs.core.PersistentArrayMap(null, 4, [new cljs.core.Keyword(null,"cached-url","cached-url",-687594183),new cljs.core.Keyword(null,"url","url",276297046).cljs$core$IFn$_invoke$arity$1(cljs.core.deref(logseq.common.authorization._STAR_jwks_cache)),new cljs.core.Keyword(null,"url","url",276297046),url,new cljs.core.Keyword(null,"jwks-keys","jwks-keys",1233126000),new cljs.core.Keyword(null,"keys","keys",1068423698).cljs$core$IFn$_invoke$arity$1(cljs.core.deref(logseq.common.authorization._STAR_jwks_cache)),new cljs.core.Keyword(null,"fetched-at","fetched-at",1252416344),new cljs.core.Keyword(null,"fetched-at","fetched-at",1252416344).cljs$core$IFn$_invoke$arity$1(cljs.core.deref(logseq.common.authorization._STAR_jwks_cache))], null);
var map__47853__$1 = cljs.core.__destructure_map(map__47853);
var url__$1 = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__47853__$1,new cljs.core.Keyword(null,"url","url",276297046));
var cached_url = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__47853__$1,new cljs.core.Keyword(null,"cached-url","cached-url",-687594183));
var jwks_keys = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__47853__$1,new cljs.core.Keyword(null,"jwks-keys","jwks-keys",1233126000));
var fetched_at = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__47853__$1,new cljs.core.Keyword(null,"fetched-at","fetched-at",1252416344));
var fresh_QMARK_ = (function (){var and__5140__auto__ = cljs.core.not(force_QMARK_);
if(and__5140__auto__){
var and__5140__auto____$1 = cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(cached_url,url__$1);
if(and__5140__auto____$1){
var and__5140__auto____$2 = jwks_keys;
if(cljs.core.truth_(and__5140__auto____$2)){
return ((now - fetched_at) < logseq.common.authorization.jwks_ttl_ms);
} else {
return and__5140__auto____$2;
}
} else {
return and__5140__auto____$1;
}
} else {
return and__5140__auto__;
}
})();
if(cljs.core.truth_(fresh_QMARK_)){
return promesa.core.resolved(jwks_keys);
} else {
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___42540__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(fetch(url__$1)),(function (jwks_resp){
return promesa.protocols._mcat(promesa.protocols._promise((cljs.core.truth_(jwks_resp.ok)?null:(function(){throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2("jwks",cljs.core.PersistentArrayMap.EMPTY)})())),(function (_){
return promesa.protocols._mcat(promesa.protocols._promise(jwks_resp.json()),(function (jwks){
return promesa.protocols._mcat(promesa.protocols._promise((function (){var or__5142__auto__ = (jwks["keys"]);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return [];
}
})()),(function (jwks_keys__$1){
return promesa.protocols._mcat(promesa.protocols._promise(cljs.core.reset_BANG_(logseq.common.authorization._STAR_jwks_cache,new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"url","url",276297046),url__$1,new cljs.core.Keyword(null,"keys","keys",1068423698),jwks_keys__$1,new cljs.core.Keyword(null,"fetched-at","fetched-at",1252416344),now], null))),(function (___42508__auto__){
return promesa.protocols._promise(jwks_keys__$1);
}));
}));
}));
}));
}));
}));
}
}));

(logseq.common.authorization.get_jwks_keys.cljs$lang$maxFixedArity = (1));

/** @this {Function} */
(logseq.common.authorization.get_jwks_keys.cljs$lang$applyTo = (function (seq47849){
var G__47850 = cljs.core.first(seq47849);
var seq47849__$1 = cljs.core.next(seq47849);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__47850,seq47849__$1);
}));

logseq.common.authorization.base64url__GT_uint8array = (function logseq$common$authorization$base64url__GT_uint8array(input){
var pad = (((cljs.core.mod(cljs.core.count(input),(4)) > (0)))?cljs.core.apply.cljs$core$IFn$_invoke$arity$2(cljs.core.str,cljs.core.repeat.cljs$core$IFn$_invoke$arity$2(((4) - cljs.core.mod(cljs.core.count(input),(4))),"=")):"");
var base64 = clojure.string.replace(clojure.string.replace((""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(input)+cljs.core.str.cljs$core$IFn$_invoke$arity$1(pad)),"-","+"),"_","/");
var raw = atob(base64);
var data = (new Uint8Array(raw.length));
var n__5741__auto___47871 = raw.length;
var i_47872 = (0);
while(true){
if((i_47872 < n__5741__auto___47871)){
(data[i_47872] = raw.charCodeAt(i_47872));

var G__47873 = (i_47872 + (1));
i_47872 = G__47873;
continue;
} else {
}
break;
}

return data;
});
logseq.common.authorization.decode_jwt_part = (function logseq$common$authorization$decode_jwt_part(part){
var data = logseq.common.authorization.base64url__GT_uint8array(part);
return JSON.parse(logseq.common.authorization.text_decoder.decode(data));
});
logseq.common.authorization.import_rsa_key = (function logseq$common$authorization$import_rsa_key(jwk){
return crypto.subtle.importKey("jwk",jwk,({"name": "RSASSA-PKCS1-v1_5", "hash": "SHA-256"}),false,["verify"]);
});
logseq.common.authorization.verify_jwt = (function logseq$common$authorization$verify_jwt(token,env){
var parts = clojure.string.split.cljs$core$IFn$_invoke$arity$2(token,/\./);
if(cljs.core.not_EQ_.cljs$core$IFn$_invoke$arity$2((3),cljs.core.count(parts))){
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2("invalid",cljs.core.PersistentArrayMap.EMPTY);
} else {
}

var header_part = cljs.core.nth.cljs$core$IFn$_invoke$arity$2(parts,(0));
var payload_part = cljs.core.nth.cljs$core$IFn$_invoke$arity$2(parts,(1));
var signature_part = cljs.core.nth.cljs$core$IFn$_invoke$arity$2(parts,(2));
var now_ms = logseq.common.authorization.get_now_ms();
var now_s = Math.floor((now_ms / (1000)));
var temp__5802__auto__ = logseq.common.authorization.cached_token(token,now_s,now_ms);
if(cljs.core.truth_(temp__5802__auto__)){
var cached = temp__5802__auto__;
return promesa.core.resolved(cached);
} else {
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___42540__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.common.authorization.decode_jwt_part(header_part)),(function (header){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.common.authorization.decode_jwt_part(payload_part)),(function (payload){
return promesa.protocols._mcat(promesa.protocols._promise((env["COGNITO_ISSUER"])),(function (issuer){
return promesa.protocols._mcat(promesa.protocols._promise((env["COGNITO_CLIENT_ID"])),(function (client_id){
return promesa.protocols._mcat(promesa.protocols._promise(((cljs.core.not_EQ_.cljs$core$IFn$_invoke$arity$2((payload["iss"]),issuer))?(function(){throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2("iss not found",cljs.core.PersistentArrayMap.EMPTY)})():null)),(function (_){
return promesa.protocols._mcat(promesa.protocols._promise(((cljs.core.not_EQ_.cljs$core$IFn$_invoke$arity$2((payload["aud"]),client_id))?(function(){throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2("aud not found",cljs.core.PersistentArrayMap.EMPTY)})():null)),(function (___$1){
return promesa.protocols._mcat(promesa.protocols._promise((cljs.core.truth_((function (){var and__5140__auto__ = (payload["exp"]);
if(cljs.core.truth_(and__5140__auto__)){
return ((payload["exp"]) < now_s);
} else {
return and__5140__auto__;
}
})())?(function(){throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2("exp",cljs.core.PersistentArrayMap.EMPTY)})():null)),(function (___$2){
return promesa.protocols._mcat(promesa.protocols._promise((env["COGNITO_JWKS_URL"])),(function (jwks_url){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.common.authorization.get_jwks_keys(jwks_url)),(function (jwks_keys){
return promesa.protocols._mcat(promesa.protocols._promise(jwks_keys.find((function (k){
return cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2((k["kid"]),(header["kid"]));
}))),(function (matching_key){
return promesa.protocols._mcat(promesa.protocols._promise((cljs.core.truth_(matching_key)?matching_key:promesa.protocols._mcat(promesa.protocols._promise(null),(function (___42540__auto____$1){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.common.authorization.get_jwks_keys.cljs$core$IFn$_invoke$arity$variadic(jwks_url,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"force?","force?",1839038675),true], 0))),(function (jwks_keys__$1){
return promesa.protocols._mcat(promesa.protocols._promise(jwks_keys__$1.find((function (k){
return cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2((k["kid"]),(header["kid"]));
}))),(function (matching_key__$1){
return promesa.protocols._promise(matching_key__$1);
}));
}));
})))),(function (matching_key__$1){
return promesa.protocols._mcat(promesa.protocols._promise((cljs.core.truth_(matching_key__$1)?null:(function(){throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2("kid",cljs.core.PersistentArrayMap.EMPTY)})())),(function (___$3){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.common.authorization.import_rsa_key(matching_key__$1)),(function (crypto_key){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.common.authorization.text_encoder.encode((""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(header_part)+"."+cljs.core.str.cljs$core$IFn$_invoke$arity$1(payload_part)))),(function (data){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.common.authorization.base64url__GT_uint8array(signature_part)),(function (signature){
return promesa.protocols._mcat(promesa.protocols._promise(crypto.subtle.verify("RSASSA-PKCS1-v1_5",crypto_key,signature,data)),(function (ok){
return promesa.protocols._promise((cljs.core.truth_(ok)?(function (){
logseq.common.authorization.cache_token_BANG_(token,payload,now_ms);

return payload;
})()
:null));
}));
}));
}));
}));
}));
}));
}));
}));
}));
}));
}));
}));
}));
}));
}));
}));
}));
}
});

//# sourceMappingURL=logseq.common.authorization.js.map
