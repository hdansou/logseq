import "./cljs_env.js";
import "./cljs.core.js";
import "./clojure.string.js";
goog.provide('logseq.common.plural');
if((typeof logseq !== 'undefined') && (typeof logseq.common !== 'undefined') && (typeof logseq.common.plural !== 'undefined') && (typeof logseq.common.plural.plural_rules !== 'undefined')){
} else {
logseq.common.plural.plural_rules = cljs.core.atom.cljs$core$IFn$_invoke$arity$1(cljs.core.PersistentVector.EMPTY);
}
if((typeof logseq !== 'undefined') && (typeof logseq.common !== 'undefined') && (typeof logseq.common.plural !== 'undefined') && (typeof logseq.common.plural.singular_rules !== 'undefined')){
} else {
logseq.common.plural.singular_rules = cljs.core.atom.cljs$core$IFn$_invoke$arity$1(cljs.core.PersistentVector.EMPTY);
}
if((typeof logseq !== 'undefined') && (typeof logseq.common !== 'undefined') && (typeof logseq.common.plural !== 'undefined') && (typeof logseq.common.plural.uncountables !== 'undefined')){
} else {
logseq.common.plural.uncountables = cljs.core.atom.cljs$core$IFn$_invoke$arity$1(cljs.core.PersistentArrayMap.EMPTY);
}
if((typeof logseq !== 'undefined') && (typeof logseq.common !== 'undefined') && (typeof logseq.common.plural !== 'undefined') && (typeof logseq.common.plural.irregular_plurals !== 'undefined')){
} else {
logseq.common.plural.irregular_plurals = cljs.core.atom.cljs$core$IFn$_invoke$arity$1(cljs.core.PersistentArrayMap.EMPTY);
}
if((typeof logseq !== 'undefined') && (typeof logseq.common !== 'undefined') && (typeof logseq.common.plural !== 'undefined') && (typeof logseq.common.plural.irregular_singles !== 'undefined')){
} else {
logseq.common.plural.irregular_singles = cljs.core.atom.cljs$core$IFn$_invoke$arity$1(cljs.core.PersistentArrayMap.EMPTY);
}
/**
 * If rule is a string, compile to case-insensitive regexp that matches the whole string.
 * Else keep it (assumed to be js/RegExp).
 */
logseq.common.plural.sanitize_rule = (function logseq$common$plural$sanitize_rule(rule){
if(typeof rule === 'string'){
return (new RegExp((""+"^"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(rule)+"$"),"i"));
} else {
return rule;
}
});
/**
 * Replicate casing of `word` onto `token`.
 */
logseq.common.plural.restore_case = (function logseq$common$plural$restore_case(word,token){
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(word,token)){
return token;
} else {
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(word,clojure.string.lower_case(word))){
return clojure.string.lower_case(token);
} else {
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(word,clojure.string.upper_case(word))){
return clojure.string.upper_case(token);
} else {
if(((cljs.core.seq(word)) && (cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(cljs.core.subs.cljs$core$IFn$_invoke$arity$3(word,(0),(1)),clojure.string.upper_case(cljs.core.subs.cljs$core$IFn$_invoke$arity$3(word,(0),(1))))))){
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(clojure.string.upper_case(cljs.core.subs.cljs$core$IFn$_invoke$arity$3(token,(0),(1))))+cljs.core.str.cljs$core$IFn$_invoke$arity$1(clojure.string.lower_case(cljs.core.subs.cljs$core$IFn$_invoke$arity$2(token,(1)))));
} else {
return clojure.string.lower_case(token);

}
}
}
}
});
/**
 * Replace $1..$12 etc in `s` using JS replace args (match, g1, g2 ...).
 */
logseq.common.plural.interpolate = (function logseq$common$plural$interpolate(s,js_args){
return s.replace((new RegExp("\\$(\\d{1,2})","g")),(function (_,idx){
var i = parseInt(idx,(10));
var v = (js_args[i]);
var or__5142__auto__ = v;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return "";
}
}));
});
/**
 * Apply a [re repl] rule to word with casing restoration (matches JS behavior).
 */
logseq.common.plural.replace_with_rule = (function logseq$common$plural$replace_with_rule(word,p__48150){
var vec__48151 = p__48150;
var re = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__48151,(0),null);
var repl = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__48151,(1),null);
return word.replace(re,(function() { 
var G__48287__delegate = function (args){
var match = cljs.core.nth.cljs$core$IFn$_invoke$arity$2(args,(0));
var offset = cljs.core.nth.cljs$core$IFn$_invoke$arity$2(args,(cljs.core.count(args) - (2)));
var js_args = cljs.core.to_array(args);
var result = logseq.common.plural.interpolate(repl,js_args);
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(match,"")){
return logseq.common.plural.restore_case(cljs.core.subs.cljs$core$IFn$_invoke$arity$3(word,(offset - (1)),offset),result);
} else {
return logseq.common.plural.restore_case(match,result);
}
};
var G__48287 = function (var_args){
var args = null;
if (arguments.length > 0) {
var G__48290__i = 0, G__48290__a = new Array(arguments.length -  0);
while (G__48290__i < G__48290__a.length) {G__48290__a[G__48290__i] = arguments[G__48290__i + 0]; ++G__48290__i;}
  args = new cljs.core.IndexedSeq(G__48290__a,0,null);
} 
return G__48287__delegate.call(this,args);};
G__48287.cljs$lang$maxFixedArity = 0;
G__48287.cljs$lang$applyTo = (function (arglist__48291){
var args = cljs.core.seq(arglist__48291);
return G__48287__delegate(args);
});
G__48287.cljs$core$IFn$_invoke$arity$variadic = G__48287__delegate;
return G__48287;
})()
);
});
/**
 * Return sanitized `word` based on `token` and `rules`.
 */
logseq.common.plural.sanitize_word = (function logseq$common$plural$sanitize_word(token,word,rules){
if((((cljs.core.count(token) === (0))) || (cljs.core.contains_QMARK_(cljs.core.deref(logseq.common.plural.uncountables),token)))){
return word;
} else {
var rs = rules;
var n = cljs.core.count(rs);
var i = (n - (1));
while(true){
if((i < (0))){
return word;
} else {
var vec__48160 = cljs.core.nth.cljs$core$IFn$_invoke$arity$2(rs,i);
var re = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__48160,(0),null);
var _ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__48160,(1),null);
var rule = vec__48160;
if(cljs.core.truth_(re.test(word))){
return logseq.common.plural.replace_with_rule(word,rule);
} else {
var G__48298 = (i - (1));
i = G__48298;
continue;
}
}
break;
}

}
});
/**
 * Build a word transformer (plural or singular).
 */
logseq.common.plural.replace_word_fn = (function logseq$common$plural$replace_word_fn(replace_map_atom,keep_map_atom,rules_atom){
return (function (word){
var token = clojure.string.lower_case(word);
var keep_map = cljs.core.deref(keep_map_atom);
var replace_map = cljs.core.deref(replace_map_atom);
var rules = cljs.core.deref(rules_atom);
if(cljs.core.contains_QMARK_(keep_map,token)){
return logseq.common.plural.restore_case(word,token);
} else {
if(cljs.core.contains_QMARK_(replace_map,token)){
return logseq.common.plural.restore_case(word,cljs.core.get.cljs$core$IFn$_invoke$arity$2(replace_map,token));
} else {
return logseq.common.plural.sanitize_word(token,word,rules);

}
}
});
});
/**
 * Build a predicate for whether word is plural/singular (mirrors JS `checkWord`).
 */
logseq.common.plural.check_word_fn = (function logseq$common$plural$check_word_fn(replace_map_atom,keep_map_atom,rules_atom){
return (function (word){
var token = clojure.string.lower_case(word);
var keep_map = cljs.core.deref(keep_map_atom);
var replace_map = cljs.core.deref(replace_map_atom);
var rules = cljs.core.deref(rules_atom);
if(cljs.core.contains_QMARK_(keep_map,token)){
return true;
} else {
if(cljs.core.contains_QMARK_(replace_map,token)){
return false;
} else {
return cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(logseq.common.plural.sanitize_word(token,token,rules),token);

}
}
});
});
logseq.common.plural.plural = logseq.common.plural.replace_word_fn(logseq.common.plural.irregular_singles,logseq.common.plural.irregular_plurals,logseq.common.plural.plural_rules);
logseq.common.plural.singular = logseq.common.plural.replace_word_fn(logseq.common.plural.irregular_plurals,logseq.common.plural.irregular_singles,logseq.common.plural.singular_rules);
logseq.common.plural.is_plural_QMARK_ = logseq.common.plural.check_word_fn(logseq.common.plural.irregular_singles,logseq.common.plural.irregular_plurals,logseq.common.plural.plural_rules);
logseq.common.plural.is_singular_QMARK_ = logseq.common.plural.check_word_fn(logseq.common.plural.irregular_plurals,logseq.common.plural.irregular_singles,logseq.common.plural.singular_rules);
/**
 * Pluralize or singularize based on count. If inclusive, prefix with count.
 */
logseq.common.plural.pluralize = (function logseq$common$plural$pluralize(var_args){
var G__48185 = arguments.length;
switch (G__48185) {
case 2:
return logseq.common.plural.pluralize.cljs$core$IFn$_invoke$arity$2((arguments[(0)]),(arguments[(1)]));

break;
case 3:
return logseq.common.plural.pluralize.cljs$core$IFn$_invoke$arity$3((arguments[(0)]),(arguments[(1)]),(arguments[(2)]));

break;
default:
throw (new Error(["Invalid arity: ",arguments.length].join("")));

}
});

(logseq.common.plural.pluralize.cljs$core$IFn$_invoke$arity$2 = (function (word,item_count){
return logseq.common.plural.pluralize.cljs$core$IFn$_invoke$arity$3(word,item_count,false);
}));

(logseq.common.plural.pluralize.cljs$core$IFn$_invoke$arity$3 = (function (word,item_count,inclusive){
var pluralized = ((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(item_count,(1)))?logseq.common.plural.singular(word):logseq.common.plural.plural(word));
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1((cljs.core.truth_(inclusive)?(""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(item_count)+" "):null))+cljs.core.str.cljs$core$IFn$_invoke$arity$1(pluralized));
}));

(logseq.common.plural.pluralize.cljs$lang$maxFixedArity = 3);

logseq.common.plural.add_plural_rule_BANG_ = (function logseq$common$plural$add_plural_rule_BANG_(rule,replacement){
return cljs.core.swap_BANG_.cljs$core$IFn$_invoke$arity$3(logseq.common.plural.plural_rules,cljs.core.conj,new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [logseq.common.plural.sanitize_rule(rule),replacement], null));
});
logseq.common.plural.add_singular_rule_BANG_ = (function logseq$common$plural$add_singular_rule_BANG_(rule,replacement){
return cljs.core.swap_BANG_.cljs$core$IFn$_invoke$arity$3(logseq.common.plural.singular_rules,cljs.core.conj,new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [logseq.common.plural.sanitize_rule(rule),replacement], null));
});
/**
 * If word is string => mark as uncountable.
 * If regexp => add plural+singular passthrough rules ($0).
 */
logseq.common.plural.add_uncountable_rule_BANG_ = (function logseq$common$plural$add_uncountable_rule_BANG_(word){
if(typeof word === 'string'){
return cljs.core.swap_BANG_.cljs$core$IFn$_invoke$arity$4(logseq.common.plural.uncountables,cljs.core.assoc,clojure.string.lower_case(word),true);
} else {
logseq.common.plural.add_plural_rule_BANG_(word,"$0");

return logseq.common.plural.add_singular_rule_BANG_(word,"$0");
}
});
logseq.common.plural.add_irregular_rule_BANG_ = (function logseq$common$plural$add_irregular_rule_BANG_(single,plural_word){
var p = clojure.string.lower_case(plural_word);
var s = clojure.string.lower_case(single);
cljs.core.swap_BANG_.cljs$core$IFn$_invoke$arity$4(logseq.common.plural.irregular_singles,cljs.core.assoc,s,p);

return cljs.core.swap_BANG_.cljs$core$IFn$_invoke$arity$4(logseq.common.plural.irregular_plurals,cljs.core.assoc,p,s);
});
logseq.common.plural.init_irregulars_BANG_ = (function logseq$common$plural$init_irregulars_BANG_(){
var seq__48192 = cljs.core.seq(cljs.core.PersistentVector.fromArray([new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["I","we"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["me","us"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["he","they"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["she","they"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["them","them"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["myself","ourselves"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["yourself","yourselves"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["itself","themselves"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["herself","themselves"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["himself","themselves"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["themself","themselves"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["is","are"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["was","were"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["has","have"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["this","these"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["that","those"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["my","our"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["its","their"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["his","their"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["her","their"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["echo","echoes"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["dingo","dingoes"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["volcano","volcanoes"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["tornado","tornadoes"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["torpedo","torpedoes"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["genus","genera"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["viscus","viscera"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["stigma","stigmata"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["stoma","stomata"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["dogma","dogmata"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["lemma","lemmata"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["schema","schemata"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["anathema","anathemata"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["ox","oxen"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["axe","axes"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["die","dice"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["yes","yeses"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["foot","feet"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["eave","eaves"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["goose","geese"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["tooth","teeth"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["quiz","quizzes"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["human","humans"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["proof","proofs"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["carve","carves"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["valve","valves"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["looey","looies"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["thief","thieves"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["groove","grooves"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["pickaxe","pickaxes"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["passerby","passersby"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["canvas","canvases"], null)], true));
var chunk__48193 = null;
var count__48194 = (0);
var i__48195 = (0);
while(true){
if((i__48195 < count__48194)){
var vec__48213 = chunk__48193.cljs$core$IIndexed$_nth$arity$2(null,i__48195);
var s = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__48213,(0),null);
var p = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__48213,(1),null);
logseq.common.plural.add_irregular_rule_BANG_(s,p);


var G__48317 = seq__48192;
var G__48318 = chunk__48193;
var G__48319 = count__48194;
var G__48320 = (i__48195 + (1));
seq__48192 = G__48317;
chunk__48193 = G__48318;
count__48194 = G__48319;
i__48195 = G__48320;
continue;
} else {
var temp__5804__auto__ = cljs.core.seq(seq__48192);
if(temp__5804__auto__){
var seq__48192__$1 = temp__5804__auto__;
if(cljs.core.chunked_seq_QMARK_(seq__48192__$1)){
var c__5673__auto__ = cljs.core.chunk_first(seq__48192__$1);
var G__48321 = cljs.core.chunk_rest(seq__48192__$1);
var G__48322 = c__5673__auto__;
var G__48323 = cljs.core.count(c__5673__auto__);
var G__48324 = (0);
seq__48192 = G__48321;
chunk__48193 = G__48322;
count__48194 = G__48323;
i__48195 = G__48324;
continue;
} else {
var vec__48216 = cljs.core.first(seq__48192__$1);
var s = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__48216,(0),null);
var p = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__48216,(1),null);
logseq.common.plural.add_irregular_rule_BANG_(s,p);


var G__48325 = cljs.core.next(seq__48192__$1);
var G__48326 = null;
var G__48327 = (0);
var G__48328 = (0);
seq__48192 = G__48325;
chunk__48193 = G__48326;
count__48194 = G__48327;
i__48195 = G__48328;
continue;
}
} else {
return null;
}
}
break;
}
});
logseq.common.plural.init_plural_rules_BANG_ = (function logseq$common$plural$init_plural_rules_BANG_(){
var seq__48223 = cljs.core.seq(new cljs.core.PersistentVector(null, 25, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("s?$","i")),"s"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("[^\\u0000-\\u007F]$","i")),"$0"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("([^aeiou]ese)$","i")),"$1"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(ax|test)is$","i")),"$1es"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(alias|[^aou]us|t[lm]as|gas|ris)$","i")),"$1es"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(e[mn]u)s?$","i")),"$1s"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("([^l]ias|[aeiou]las|[ejzr]as|[iu]am)$","i")),"$1"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(alumn|syllab|vir|radi|nucle|fung|cact|stimul|termin|bacill|foc|uter|loc|strat)(?:us|i)$","i")),"$1i"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(alumn|alg|vertebr)(?:a|ae)$","i")),"$1ae"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(seraph|cherub)(?:im)?$","i")),"$1im"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(her|at|gr)o$","i")),"$1oes"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(agend|addend|millenni|dat|extrem|bacteri|desiderat|strat|candelabr|errat|ov|symposi|curricul|automat|quor)(?:a|um)$","i")),"$1a"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(apheli|hyperbat|periheli|asyndet|noumen|phenomen|criteri|organ|prolegomen|hedr|automat)(?:a|on)$","i")),"$1a"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("sis$","i")),"ses"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(?:(kni|wi|li)fe|(ar|l|ea|eo|oa|hoo)f)$","i")),"$1$2ves"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("([^aeiouy]|qu)y$","i")),"$1ies"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("([^ch][ieo][ln])ey$","i")),"$1ies"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(x|ch|ss|sh|zz)$","i")),"$1es"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(matr|cod|mur|sil|vert|ind|append)(?:ix|ex)$","i")),"$1ices"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("\\b((?:tit)?m|l)(?:ice|ouse)$","i")),"$1ice"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(pe)(?:rson|ople)$","i")),"$1ople"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(child)(?:ren)?$","i")),"$1ren"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("eaux$","i")),"$0"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("m[ae]n$","i")),"men"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["thou","you"], null)], null));
var chunk__48224 = null;
var count__48225 = (0);
var i__48226 = (0);
while(true){
if((i__48226 < count__48225)){
var vec__48237 = chunk__48224.cljs$core$IIndexed$_nth$arity$2(null,i__48226);
var rule = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__48237,(0),null);
var repl = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__48237,(1),null);
logseq.common.plural.add_plural_rule_BANG_(rule,repl);


var G__48332 = seq__48223;
var G__48333 = chunk__48224;
var G__48334 = count__48225;
var G__48335 = (i__48226 + (1));
seq__48223 = G__48332;
chunk__48224 = G__48333;
count__48225 = G__48334;
i__48226 = G__48335;
continue;
} else {
var temp__5804__auto__ = cljs.core.seq(seq__48223);
if(temp__5804__auto__){
var seq__48223__$1 = temp__5804__auto__;
if(cljs.core.chunked_seq_QMARK_(seq__48223__$1)){
var c__5673__auto__ = cljs.core.chunk_first(seq__48223__$1);
var G__48336 = cljs.core.chunk_rest(seq__48223__$1);
var G__48337 = c__5673__auto__;
var G__48338 = cljs.core.count(c__5673__auto__);
var G__48339 = (0);
seq__48223 = G__48336;
chunk__48224 = G__48337;
count__48225 = G__48338;
i__48226 = G__48339;
continue;
} else {
var vec__48240 = cljs.core.first(seq__48223__$1);
var rule = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__48240,(0),null);
var repl = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__48240,(1),null);
logseq.common.plural.add_plural_rule_BANG_(rule,repl);


var G__48340 = cljs.core.next(seq__48223__$1);
var G__48341 = null;
var G__48342 = (0);
var G__48343 = (0);
seq__48223 = G__48340;
chunk__48224 = G__48341;
count__48225 = G__48342;
i__48226 = G__48343;
continue;
}
} else {
return null;
}
}
break;
}
});
logseq.common.plural.init_singular_rules_BANG_ = (function logseq$common$plural$init_singular_rules_BANG_(){
var seq__48243 = cljs.core.seq(new cljs.core.PersistentVector(null, 24, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("s$","i")),""], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(ss)$","i")),"$1"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(wi|kni|(?:after|half|high|low|mid|non|night|[^\\w]|^)li)ves$","i")),"$1fe"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(ar|(?:wo|[ae])l|[eo][ao])ves$","i")),"$1f"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("ies$","i")),"y"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(dg|ss|ois|lk|ok|wn|mb|th|ch|ec|oal|is|ck|ix|sser|ts|wb)ies$","i")),"$1ie"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("\\b(l|(?:neck|cross|hog|aun)?t|coll|faer|food|gen|goon|group|hipp|junk|vegg|(?:pork)?p|charl|calor|cut)ies$","i")),"$1ie"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("\\b(mon|smil)ies$","i")),"$1ey"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("\\b((?:tit)?m|l)ice$","i")),"$1ouse"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(seraph|cherub)im$","i")),"$1"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(x|ch|ss|sh|zz|tto|go|cho|alias|[^aou]us|t[lm]as|gas|(?:her|at|gr)o|[aeiou]ris)(?:es)?$","i")),"$1"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(analy|diagno|parenthe|progno|synop|the|empha|cri|ne)(?:sis|ses)$","i")),"$1sis"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(movie|twelve|abuse|e[mn]u)s$","i")),"$1"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(test)(?:is|es)$","i")),"$1is"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(alumn|syllab|vir|radi|nucle|fung|cact|stimul|termin|bacill|foc|uter|loc|strat)(?:us|i)$","i")),"$1us"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(agend|addend|millenni|dat|extrem|bacteri|desiderat|strat|candelabr|errat|ov|symposi|curricul|quor)a$","i")),"$1um"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(apheli|hyperbat|periheli|asyndet|noumen|phenomen|criteri|organ|prolegomen|hedr|automat)a$","i")),"$1on"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(alumn|alg|vertebr)ae$","i")),"$1a"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(cod|mur|sil|vert|ind)ices$","i")),"$1ex"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(matr|append)ices$","i")),"$1ix"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(pe)(rson|ople)$","i")),"$1rson"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(child)ren$","i")),"$1"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("(eau)x?$","i")),"$1"], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("men$","i")),"man"], null)], null));
var chunk__48244 = null;
var count__48245 = (0);
var i__48246 = (0);
while(true){
if((i__48246 < count__48245)){
var vec__48260 = chunk__48244.cljs$core$IIndexed$_nth$arity$2(null,i__48246);
var rule = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__48260,(0),null);
var repl = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__48260,(1),null);
logseq.common.plural.add_singular_rule_BANG_(rule,repl);


var G__48360 = seq__48243;
var G__48361 = chunk__48244;
var G__48362 = count__48245;
var G__48363 = (i__48246 + (1));
seq__48243 = G__48360;
chunk__48244 = G__48361;
count__48245 = G__48362;
i__48246 = G__48363;
continue;
} else {
var temp__5804__auto__ = cljs.core.seq(seq__48243);
if(temp__5804__auto__){
var seq__48243__$1 = temp__5804__auto__;
if(cljs.core.chunked_seq_QMARK_(seq__48243__$1)){
var c__5673__auto__ = cljs.core.chunk_first(seq__48243__$1);
var G__48373 = cljs.core.chunk_rest(seq__48243__$1);
var G__48374 = c__5673__auto__;
var G__48375 = cljs.core.count(c__5673__auto__);
var G__48376 = (0);
seq__48243 = G__48373;
chunk__48244 = G__48374;
count__48245 = G__48375;
i__48246 = G__48376;
continue;
} else {
var vec__48264 = cljs.core.first(seq__48243__$1);
var rule = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__48264,(0),null);
var repl = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__48264,(1),null);
logseq.common.plural.add_singular_rule_BANG_(rule,repl);


var G__48377 = cljs.core.next(seq__48243__$1);
var G__48378 = null;
var G__48379 = (0);
var G__48380 = (0);
seq__48243 = G__48377;
chunk__48244 = G__48378;
count__48245 = G__48379;
i__48246 = G__48380;
continue;
}
} else {
return null;
}
}
break;
}
});
logseq.common.plural.init_uncountables_BANG_ = (function logseq$common$plural$init_uncountables_BANG_(){
var seq__48268_48381 = cljs.core.seq(cljs.core.PersistentVector.fromArray(["adulthood","advice","agenda","aid","aircraft","alcohol","ammo","analytics","anime","athletics","audio","bison","blood","bream","buffalo","butter","carp","cash","chassis","chess","clothing","cod","commerce","cooperation","corps","debris","diabetes","digestion","elk","energy","equipment","excretion","expertise","firmware","flounder","fun","gallows","garbage","graffiti","hardware","headquarters","health","herpes","highjinks","homework","housework","information","jeans","justice","kudos","labour","literature","machinery","mackerel","mail","media","mews","moose","music","mud","manga","news","only","personnel","pike","plankton","pliers","police","pollution","premises","rain","research","rice","salmon","scissors","series","sewage","shambles","shrimp","software","staff","swine","tennis","traffic","transportation","trout","tuna","wealth","welfare","whiting","wildebeest","wildlife","you"], true));
var chunk__48269_48382 = null;
var count__48270_48383 = (0);
var i__48271_48384 = (0);
while(true){
if((i__48271_48384 < count__48270_48383)){
var w_48385 = chunk__48269_48382.cljs$core$IIndexed$_nth$arity$2(null,i__48271_48384);
logseq.common.plural.add_uncountable_rule_BANG_(w_48385);


var G__48386 = seq__48268_48381;
var G__48387 = chunk__48269_48382;
var G__48388 = count__48270_48383;
var G__48389 = (i__48271_48384 + (1));
seq__48268_48381 = G__48386;
chunk__48269_48382 = G__48387;
count__48270_48383 = G__48388;
i__48271_48384 = G__48389;
continue;
} else {
var temp__5804__auto___48390 = cljs.core.seq(seq__48268_48381);
if(temp__5804__auto___48390){
var seq__48268_48391__$1 = temp__5804__auto___48390;
if(cljs.core.chunked_seq_QMARK_(seq__48268_48391__$1)){
var c__5673__auto___48392 = cljs.core.chunk_first(seq__48268_48391__$1);
var G__48393 = cljs.core.chunk_rest(seq__48268_48391__$1);
var G__48394 = c__5673__auto___48392;
var G__48395 = cljs.core.count(c__5673__auto___48392);
var G__48396 = (0);
seq__48268_48381 = G__48393;
chunk__48269_48382 = G__48394;
count__48270_48383 = G__48395;
i__48271_48384 = G__48396;
continue;
} else {
var w_48397 = cljs.core.first(seq__48268_48391__$1);
logseq.common.plural.add_uncountable_rule_BANG_(w_48397);


var G__48398 = cljs.core.next(seq__48268_48391__$1);
var G__48399 = null;
var G__48400 = (0);
var G__48401 = (0);
seq__48268_48381 = G__48398;
chunk__48269_48382 = G__48399;
count__48270_48383 = G__48400;
i__48271_48384 = G__48401;
continue;
}
} else {
}
}
break;
}

var seq__48276 = cljs.core.seq(new cljs.core.PersistentVector(null, 8, 5, cljs.core.PersistentVector.EMPTY_NODE, [(new RegExp("pok[e\u00E9]mon$","i")),(new RegExp("[^aeiou]ese$","i")),(new RegExp("deer$","i")),(new RegExp("fish$","i")),(new RegExp("measles$","i")),(new RegExp("o[iu]s$","i")),(new RegExp("pox$","i")),(new RegExp("sheep$","i"))], null));
var chunk__48277 = null;
var count__48278 = (0);
var i__48279 = (0);
while(true){
if((i__48279 < count__48278)){
var re = chunk__48277.cljs$core$IIndexed$_nth$arity$2(null,i__48279);
logseq.common.plural.add_uncountable_rule_BANG_(re);


var G__48405 = seq__48276;
var G__48406 = chunk__48277;
var G__48407 = count__48278;
var G__48408 = (i__48279 + (1));
seq__48276 = G__48405;
chunk__48277 = G__48406;
count__48278 = G__48407;
i__48279 = G__48408;
continue;
} else {
var temp__5804__auto__ = cljs.core.seq(seq__48276);
if(temp__5804__auto__){
var seq__48276__$1 = temp__5804__auto__;
if(cljs.core.chunked_seq_QMARK_(seq__48276__$1)){
var c__5673__auto__ = cljs.core.chunk_first(seq__48276__$1);
var G__48409 = cljs.core.chunk_rest(seq__48276__$1);
var G__48410 = c__5673__auto__;
var G__48411 = cljs.core.count(c__5673__auto__);
var G__48412 = (0);
seq__48276 = G__48409;
chunk__48277 = G__48410;
count__48278 = G__48411;
i__48279 = G__48412;
continue;
} else {
var re = cljs.core.first(seq__48276__$1);
logseq.common.plural.add_uncountable_rule_BANG_(re);


var G__48413 = cljs.core.next(seq__48276__$1);
var G__48414 = null;
var G__48415 = (0);
var G__48416 = (0);
seq__48276 = G__48413;
chunk__48277 = G__48414;
count__48278 = G__48415;
i__48279 = G__48416;
continue;
}
} else {
return null;
}
}
break;
}
});
logseq.common.plural.init_irregulars_BANG_();
logseq.common.plural.init_plural_rules_BANG_();
logseq.common.plural.init_singular_rules_BANG_();
logseq.common.plural.init_uncountables_BANG_();

//# sourceMappingURL=logseq.common.plural.js.map
