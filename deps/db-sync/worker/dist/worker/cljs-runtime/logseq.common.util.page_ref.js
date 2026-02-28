import "./cljs_env.js";
import "./cljs.core.js";
import "./shadow.esm.esm_import$path.js";
import "./clojure.string.js";
goog.provide('logseq.common.util.page_ref');
/**
 * Opening characters for page-ref
 */
logseq.common.util.page_ref.left_brackets = "[[";
/**
 * Closing characters for page-ref
 */
logseq.common.util.page_ref.right_brackets = "]]";
/**
 * Opening and closing characters for page-ref
 */
logseq.common.util.page_ref.left_and_right_brackets = (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(logseq.common.util.page_ref.left_brackets)+cljs.core.str.cljs$core$IFn$_invoke$arity$1(logseq.common.util.page_ref.right_brackets));
/**
 * Inner capture and doesn't match nested brackets
 */
logseq.common.util.page_ref.page_ref_re = /\[\[(.*?)\]\]/;
/**
 * Matches most inner nested brackets
 */
logseq.common.util.page_ref.page_ref_without_nested_re = /\[\[([^\[\]]+)\]\]/;
/**
 * Inner capture that matches anything between brackets
 */
logseq.common.util.page_ref.page_ref_any_re = /\[\[(.*)\]\]/;
logseq.common.util.page_ref.markdown_page_ref_re = /\[(.*)\]\(file:.*\)/;
/**
 * Returns the basename of a file path. e.g. /a/b/c.md -> c.md
 */
logseq.common.util.page_ref.get_file_basename = (function logseq$common$util$page_ref$get_file_basename(path){
if(clojure.string.blank_QMARK_(path)){
return null;
} else {
return shadow.esm.esm_import$path.parse(clojure.string.replace(path,"+","/")).base;
}
});
/**
 * Determines if string is page-ref. Avoid using with format-specific page-refs e.g. org
 */
logseq.common.util.page_ref.page_ref_QMARK_ = (function logseq$common$util$page_ref$page_ref_QMARK_(s){
return ((clojure.string.starts_with_QMARK_(s,logseq.common.util.page_ref.left_brackets)) && (clojure.string.ends_with_QMARK_(s,logseq.common.util.page_ref.right_brackets)));
});
/**
 * Create a page ref given a page name
 */
logseq.common.util.page_ref.__GT_page_ref = (function logseq$common$util$page_ref$__GT_page_ref(page_name){
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(logseq.common.util.page_ref.left_brackets)+cljs.core.str.cljs$core$IFn$_invoke$arity$1(page_name)+cljs.core.str.cljs$core$IFn$_invoke$arity$1(logseq.common.util.page_ref.right_brackets));
});
/**
 * Extracts page names from md page-refs or logseq page-refs. Only call in
 *   contexts where md page-refs are used
 */
logseq.common.util.page_ref.get_page_name = (function logseq$common$util$page_ref$get_page_name(s){
var and__5140__auto__ = typeof s === 'string';
if(and__5140__auto__){
var or__5142__auto__ = (function (){var temp__5804__auto__ = cljs.core.re_matches(logseq.common.util.page_ref.markdown_page_ref_re,s);
if(cljs.core.truth_(temp__5804__auto__)){
var vec__48588 = temp__5804__auto__;
var _ = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__48588,(0),null);
var label = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__48588,(1),null);
var _path = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__48588,(2),null);
return clojure.string.trim(label);
} else {
return null;
}
})();
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return cljs.core.second(cljs.core.re_matches(logseq.common.util.page_ref.page_ref_any_re,s));
}
} else {
return and__5140__auto__;
}
});
/**
 * Extracts page-name from page-ref and fall back to arg. Useful for when user
 *   input may (not) be a page-ref
 */
logseq.common.util.page_ref.get_page_name_BANG_ = (function logseq$common$util$page_ref$get_page_name_BANG_(s){
var or__5142__auto__ = logseq.common.util.page_ref.get_page_name(s);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return s;
}
});
logseq.common.util.page_ref.page_ref_un_brackets_BANG_ = (function logseq$common$util$page_ref$page_ref_un_brackets_BANG_(s){
var or__5142__auto__ = logseq.common.util.page_ref.get_page_name(s);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return s;
}
});

//# sourceMappingURL=logseq.common.util.page_ref.js.map
