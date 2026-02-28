import "./cljs_env.js";
import "./cljs.core.js";
import "./clojure.string.js";
import "./datascript.core.js";
goog.provide('logseq.db.frontend.db_ident');
/**
 * Ensures the given db-ident is unique. If a db-ident conflicts, it is made
 *   unique by adding a suffix with a unique number e.g. :db-ident-1 :db-ident-2
 */
logseq.db.frontend.db_ident.ensure_unique_db_ident = (function logseq$db$frontend$db_ident$ensure_unique_db_ident(db,db_ident){
if(cljs.core.truth_((datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,db_ident) : datascript.core.entity.call(null,db,db_ident)))){
var existing_idents = (function (){var G__47796 = new cljs.core.PersistentVector(null, 9, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"find","find",496279456),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?ident","?ident",1230589912,null),new cljs.core.Symbol(null,"...","...",-1926939749,null)], null),new cljs.core.Keyword(null,"in","in",-1531184865),new cljs.core.Symbol(null,"$","$",-1580747756,null),new cljs.core.Symbol(null,"?ident-name","?ident-name",-1596982243,null),new cljs.core.Keyword(null,"where","where",-2044795965),new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?b","?b",1575118075,null),new cljs.core.Keyword("db","ident","db/ident",-737096),new cljs.core.Symbol(null,"?ident","?ident",1230589912,null)], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [cljs.core.list(new cljs.core.Symbol(null,"str","str",-1564826950,null),new cljs.core.Symbol(null,"?ident","?ident",1230589912,null)),new cljs.core.Symbol(null,"?str-ident","?str-ident",2051298487,null)], null),new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [cljs.core.list(new cljs.core.Symbol("clojure.string","starts-with?","clojure.string/starts-with?",656256322,null),new cljs.core.Symbol(null,"?str-ident","?str-ident",2051298487,null),new cljs.core.Symbol(null,"?ident-name","?ident-name",-1596982243,null))], null)], null);
var G__47797 = db;
var G__47798 = (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(db_ident)+"-");
return (datascript.core.q.cljs$core$IFn$_invoke$arity$3 ? datascript.core.q.cljs$core$IFn$_invoke$arity$3(G__47796,G__47797,G__47798) : datascript.core.q.call(null,G__47796,G__47797,G__47798));
})();
var new_ident = (function (){var temp__5802__auto__ = cljs.core.apply.cljs$core$IFn$_invoke$arity$2(cljs.core.max,cljs.core.keep.cljs$core$IFn$_invoke$arity$2((function (p1__47794_SHARP_){
return cljs.core.parse_long(clojure.string.replace_first((""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(p1__47794_SHARP_)),(""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(db_ident)+"-"),""));
}),existing_idents));
if(cljs.core.truth_(temp__5802__auto__)){
var max_num = temp__5802__auto__;
return cljs.core.keyword.cljs$core$IFn$_invoke$arity$2(cljs.core.namespace(db_ident),(""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.name(db_ident))+"-"+cljs.core.str.cljs$core$IFn$_invoke$arity$1((max_num + (1)))));
} else {
return cljs.core.keyword.cljs$core$IFn$_invoke$arity$2(cljs.core.namespace(db_ident),(""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.name(db_ident))+"-1"));
}
})();
return new_ident;
} else {
return db_ident;
}
});
logseq.db.frontend.db_ident.non_int_char_range = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
logseq.db.frontend.db_ident.alphabet = cljs.core.mapv.cljs$core$IFn$_invoke$arity$2(cljs.core.str,"_-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");
logseq.db.frontend.db_ident.random_bytes = (function logseq$db$frontend$db_ident$random_bytes(size){
var seed = (new Uint8Array(size));
crypto.getRandomValues(seed);

return cljs.core.array_seq.cljs$core$IFn$_invoke$arity$1(seed);
});
/**
 * Random id generator
 */
logseq.db.frontend.db_ident.nano_id = (function logseq$db$frontend$db_ident$nano_id(var_args){
var G__47809 = arguments.length;
switch (G__47809) {
case 0:
return logseq.db.frontend.db_ident.nano_id.cljs$core$IFn$_invoke$arity$0();

break;
case 1:
return logseq.db.frontend.db_ident.nano_id.cljs$core$IFn$_invoke$arity$1((arguments[(0)]));

break;
default:
throw (new Error(["Invalid arity: ",arguments.length].join("")));

}
});

(logseq.db.frontend.db_ident.nano_id.cljs$core$IFn$_invoke$arity$0 = (function (){
return logseq.db.frontend.db_ident.nano_id.cljs$core$IFn$_invoke$arity$1((21));
}));

(logseq.db.frontend.db_ident.nano_id.cljs$core$IFn$_invoke$arity$1 = (function (size){
var mask_SINGLEQUOTE_ = (63);
var bs = logseq.db.frontend.db_ident.random_bytes(size);
var id = "";
while(true){
if(cljs.core.truth_(bs)){
var G__47829 = cljs.core.next(bs);
var G__47830 = (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(id)+cljs.core.str.cljs$core$IFn$_invoke$arity$1((function (){var G__47814 = (mask_SINGLEQUOTE_ & cljs.core.first(bs));
return (logseq.db.frontend.db_ident.alphabet.cljs$core$IFn$_invoke$arity$1 ? logseq.db.frontend.db_ident.alphabet.cljs$core$IFn$_invoke$arity$1(G__47814) : logseq.db.frontend.db_ident.alphabet.call(null,G__47814));
})()));
bs = G__47829;
id = G__47830;
continue;
} else {
return id;
}
break;
}
}));

(logseq.db.frontend.db_ident.nano_id.cljs$lang$maxFixedArity = 1);

logseq.db.frontend.db_ident.normalize_ident_name_part = (function logseq$db$frontend$db_ident$normalize_ident_name_part(name_string){
return cljs.core.apply.cljs$core$IFn$_invoke$arity$2(cljs.core.str,cljs.core.filter.cljs$core$IFn$_invoke$arity$2((function (p1__47816_SHARP_){
return cljs.core.re_find(/[0-9a-zA-Z*+!_'?<>=-]{1}/,p1__47816_SHARP_);
}),clojure.string.replace_first(name_string,/^(\d)/,"NUM-$1")));
});
/**
 * Creates a :db/ident for a class or property by sanitizing the given name.
 *   The created ident must obey clojure's rules for keywords i.e.
 *   be a valid symbol per https://clojure.org/reference/reader#_symbols
 * 
 * NOTE: Only use this when creating a db-ident for a new class/property. Using
 * this in read-only contexts like querying can result in db-ident conflicts
 */
logseq.db.frontend.db_ident.create_db_ident_from_name = (function logseq$db$frontend$db_ident$create_db_ident_from_name(user_namespace,name_string){
if((((user_namespace instanceof cljs.core.Keyword)) || (typeof user_namespace === 'string'))){
} else {
throw (new Error("Assert failed: (or (keyword? user-namespace) (string? user-namespace))"));
}

if(typeof name_string === 'string'){
} else {
throw (new Error("Assert failed: (string? name-string)"));
}

if(cljs.core.not(cljs.core.re_find(/^(logseq|block)(\.|$)/,cljs.core.name(user_namespace)))){
} else {
throw (new Error((""+"Assert failed: "+"New ident is not allowed to use an internal namespace"+"\n"+"(not (re-find #\"^(logseq|block)(\\.|$)\" (name user-namespace)))")));
}

if((typeof process !== 'undefined')){
return cljs.core.keyword.cljs$core$IFn$_invoke$arity$2(user_namespace,logseq.db.frontend.db_ident.normalize_ident_name_part(name_string));
} else {
var plugin_QMARK_ = clojure.string.starts_with_QMARK_(user_namespace,"plugin.class.");
var suffix = (""+"-"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.rand_nth(logseq.db.frontend.db_ident.non_int_char_range))+cljs.core.str.cljs$core$IFn$_invoke$arity$1(logseq.db.frontend.db_ident.nano_id.cljs$core$IFn$_invoke$arity$1((7))));
return cljs.core.keyword.cljs$core$IFn$_invoke$arity$2(user_namespace,(""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(logseq.db.frontend.db_ident.normalize_ident_name_part(name_string))+cljs.core.str.cljs$core$IFn$_invoke$arity$1(((plugin_QMARK_)?null:suffix))));
}
});
logseq.db.frontend.db_ident.replace_db_ident_random_suffix = (function logseq$db$frontend$db_ident$replace_db_ident_random_suffix(db_ident_kw,new_suffix){
if((db_ident_kw instanceof cljs.core.Keyword)){
} else {
throw (new Error("Assert failed: (keyword? db-ident-kw)"));
}

if(((typeof new_suffix === 'string') && (cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2((8),cljs.core.count(new_suffix))))){
} else {
throw (new Error("Assert failed: (and (string? new-suffix) (= 8 (count new-suffix)))"));
}

return cljs.core.keyword.cljs$core$IFn$_invoke$arity$2(cljs.core.namespace(db_ident_kw),clojure.string.replace_first(cljs.core.name(db_ident_kw),/-.{8}$/,(""+"-"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(new_suffix))));
});

//# sourceMappingURL=logseq.db.frontend.db_ident.js.map
