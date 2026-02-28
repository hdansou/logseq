import "./cljs_env.js";
import "./cljs.core.js";
import "./clojure.string.js";
goog.provide('logseq.common.config');
/**
 * @define {boolean}
 * @type {boolean}
 */
logseq.common.config.PUBLISHING = goog.define("logseq.common.config.PUBLISHING",false);
logseq.common.config.hidden_QMARK_ = (function logseq$common$config$hidden_QMARK_(path,patterns){
var path__$1 = ((((typeof path === 'string') && (cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2("/",cljs.core.first(path)))))?cljs.core.subs.cljs$core$IFn$_invoke$arity$2(path,(1)):path);
return cljs.core.some((function (pattern){
var pattern__$1 = ((((typeof pattern === 'string') && (cljs.core.not_EQ_.cljs$core$IFn$_invoke$arity$2("/",cljs.core.first(pattern)))))?(""+"/"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(pattern)):pattern);
return clojure.string.starts_with_QMARK_((""+"/"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(path__$1)),pattern__$1);
}),patterns);
});
/**
 * Removes files that match a pattern specified by :hidden config
 */
logseq.common.config.remove_hidden_files = (function logseq$common$config$remove_hidden_files(files,config,get_path_fn){
var temp__5802__auto__ = cljs.core.seq(new cljs.core.Keyword(null,"hidden","hidden",-312506092).cljs$core$IFn$_invoke$arity$1(config));
if(temp__5802__auto__){
var patterns = temp__5802__auto__;
return cljs.core.remove.cljs$core$IFn$_invoke$arity$2((function (file){
var path = (get_path_fn.cljs$core$IFn$_invoke$arity$1 ? get_path_fn.cljs$core$IFn$_invoke$arity$1(file) : get_path_fn.call(null,file));
return logseq.common.config.hidden_QMARK_(path,patterns);
}),files);
} else {
return files;
}
});
logseq.common.config.app_name = "logseq";
if((typeof logseq !== 'undefined') && (typeof logseq.common !== 'undefined') && (typeof logseq.common.config !== 'undefined') && (typeof logseq.common.config.asset_protocol !== 'undefined')){
} else {
logseq.common.config.asset_protocol = "assets://";
}
if((typeof logseq !== 'undefined') && (typeof logseq.common !== 'undefined') && (typeof logseq.common.config !== 'undefined') && (typeof logseq.common.config.db_version_prefix !== 'undefined')){
} else {
logseq.common.config.db_version_prefix = "logseq_db_";
}
if((typeof logseq !== 'undefined') && (typeof logseq.common !== 'undefined') && (typeof logseq.common.config !== 'undefined') && (typeof logseq.common.config.file_version_prefix !== 'undefined')){
} else {
logseq.common.config.file_version_prefix = "logseq_local_";
}
if((typeof logseq !== 'undefined') && (typeof logseq.common !== 'undefined') && (typeof logseq.common.config !== 'undefined') && (typeof logseq.common.config.local_assets_dir !== 'undefined')){
} else {
logseq.common.config.local_assets_dir = "assets";
}
if((typeof logseq !== 'undefined') && (typeof logseq.common !== 'undefined') && (typeof logseq.common.config !== 'undefined') && (typeof logseq.common.config.unlinked_graphs_dir !== 'undefined')){
} else {
logseq.common.config.unlinked_graphs_dir = "Unlinked graphs";
}
if((typeof logseq !== 'undefined') && (typeof logseq.common !== 'undefined') && (typeof logseq.common.config !== 'undefined') && (typeof logseq.common.config.favorites_page_name !== 'undefined')){
} else {
logseq.common.config.favorites_page_name = "$$$favorites";
}
if((typeof logseq !== 'undefined') && (typeof logseq.common !== 'undefined') && (typeof logseq.common.config !== 'undefined') && (typeof logseq.common.config.views_page_name !== 'undefined')){
} else {
logseq.common.config.views_page_name = "$$$views";
}
if((typeof logseq !== 'undefined') && (typeof logseq.common !== 'undefined') && (typeof logseq.common.config !== 'undefined') && (typeof logseq.common.config.library_page_name !== 'undefined')){
} else {
logseq.common.config.library_page_name = "Library";
}
if((typeof logseq !== 'undefined') && (typeof logseq.common !== 'undefined') && (typeof logseq.common.config !== 'undefined') && (typeof logseq.common.config.quick_add_page_name !== 'undefined')){
} else {
logseq.common.config.quick_add_page_name = "Quick add";
}
logseq.common.config.local_relative_asset_QMARK_ = (function logseq$common$config$local_relative_asset_QMARK_(s){
var and__5140__auto__ = typeof s === 'string';
if(and__5140__auto__){
return cljs.core.re_find(cljs.core.re_pattern((""+"^[./]*"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(logseq.common.config.local_assets_dir))),s);
} else {
return and__5140__auto__;
}
});
logseq.common.config.local_protocol_asset_QMARK_ = (function logseq$common$config$local_protocol_asset_QMARK_(s){
if(typeof s === 'string'){
return clojure.string.starts_with_QMARK_(s,logseq.common.config.asset_protocol);
} else {
return null;
}
});
logseq.common.config.protocol_path_QMARK_ = (function logseq$common$config$protocol_path_QMARK_(s){
try{var url = (new URL(s));
return (!((url.protocol == null)));
}catch (e36276){var _ = e36276;
return false;
}});
logseq.common.config.remove_asset_protocol = (function logseq$common$config$remove_asset_protocol(s){
if(cljs.core.truth_(logseq.common.config.local_protocol_asset_QMARK_(s))){
return clojure.string.replace_first(s,logseq.common.config.asset_protocol,"file://");
} else {
return s;
}
});
logseq.common.config.text_formats = (function logseq$common$config$text_formats(){
return new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 25, [new cljs.core.Keyword(null,"dat","dat",683898592),null,new cljs.core.Keyword(null,"markdown","markdown",1227225089),null,new cljs.core.Keyword(null,"js","js",1768080579),null,new cljs.core.Keyword(null,"tldr","tldr",1945790343),null,new cljs.core.Keyword(null,"txt","txt",626843688),null,new cljs.core.Keyword(null,"yml","yml",1135976041),null,new cljs.core.Keyword(null,"erl","erl",-1257006295),null,new cljs.core.Keyword(null,"css","css",1135045163),null,new cljs.core.Keyword(null,"asciidoc","asciidoc",1736965296),null,new cljs.core.Keyword(null,"ts","ts",1617209904),null,new cljs.core.Keyword(null,"rb","rb",1673817808),null,new cljs.core.Keyword(null,"ml","ml",1909675057),null,new cljs.core.Keyword(null,"java","java",1958249105),null,new cljs.core.Keyword(null,"c","c",-1763192079),null,new cljs.core.Keyword(null,"org","org",1495985),null,new cljs.core.Keyword(null,"ex","ex",-1413771341),null,new cljs.core.Keyword(null,"edn","edn",1317840885),null,new cljs.core.Keyword(null,"php","php",-97199496),null,new cljs.core.Keyword(null,"rst","rst",-824162183),null,new cljs.core.Keyword(null,"sh","sh",-682444007),null,new cljs.core.Keyword(null,"json","json",1279968570),null,new cljs.core.Keyword(null,"clj","clj",-660495428),null,new cljs.core.Keyword(null,"adoc","adoc",-1288345346),null,new cljs.core.Keyword(null,"html","html",-998796897),null,new cljs.core.Keyword(null,"md","md",707286655),null], null), null);
});
logseq.common.config.img_formats = (function logseq$common$config$img_formats(){
return new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 8, [new cljs.core.Keyword(null,"bmp","bmp",1866754050),null,new cljs.core.Keyword(null,"png","png",551930691),null,new cljs.core.Keyword(null,"gif","gif",1261828260),null,new cljs.core.Keyword(null,"webp","webp",1501869900),null,new cljs.core.Keyword(null,"svg","svg",856789142),null,new cljs.core.Keyword(null,"jpeg","jpeg",-646816934),null,new cljs.core.Keyword(null,"ico","ico",1994407291),null,new cljs.core.Keyword(null,"jpg","jpg",-1835942949),null], null), null);
});
if((typeof logseq !== 'undefined') && (typeof logseq.common !== 'undefined') && (typeof logseq.common.config !== 'undefined') && (typeof logseq.common.config.block_pattern !== 'undefined')){
} else {
logseq.common.config.block_pattern = "-";
}
/**
 * File only config keys that are deprecated in DB graphs along with
 *   descriptions for their deprecation.
 */
logseq.common.config.file_only_config = cljs.core.merge.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([cljs.core.zipmap(new cljs.core.PersistentVector(null, 17, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("file","name-format","file/name-format",1975432459),new cljs.core.Keyword("file-sync","ignore-files","file-sync/ignore-files",-646131209),new cljs.core.Keyword(null,"hidden","hidden",-312506092),new cljs.core.Keyword(null,"ignored-page-references-keywords","ignored-page-references-keywords",44006978),new cljs.core.Keyword("journal","file-name-format","journal/file-name-format",-18110349),new cljs.core.Keyword("journal","page-title-format","journal/page-title-format",2033061997),new cljs.core.Keyword(null,"journals-directory","journals-directory",1373812460),new cljs.core.Keyword("logbook","settings","logbook/settings",824968896),new cljs.core.Keyword("org-mode","insert-file-link?","org-mode/insert-file-link?",-1472433842),new cljs.core.Keyword(null,"pages-directory","pages-directory",-1705912407),new cljs.core.Keyword(null,"preferred-workflow","preferred-workflow",-1794663444),new cljs.core.Keyword("property","separated-by-commas","property/separated-by-commas",1105223737),new cljs.core.Keyword("property-pages","excludelist","property-pages/excludelist",1710831097),new cljs.core.Keyword("srs","learning-fraction","srs/learning-fraction",-869447179),new cljs.core.Keyword("srs","initial-interval","srs/initial-interval",-1802131142),new cljs.core.Keyword(null,"whiteboards-directory","whiteboards-directory",1994949079),new cljs.core.Keyword("feature","enable-whiteboards?","feature/enable-whiteboards?",-52089888)], null),cljs.core.repeat.cljs$core$IFn$_invoke$arity$1("is not used in DB graphs")),new cljs.core.PersistentArrayMap(null, 6, [new cljs.core.Keyword(null,"preferred-format","preferred-format",-1784393121),"is not used in DB graphs as there is only markdown mode.",new cljs.core.Keyword("property-pages","enabled?","property-pages/enabled?",-48336645),"is not used in DB graphs as all properties have pages",new cljs.core.Keyword(null,"block-hidden-properties","block-hidden-properties",-155956857),"is not used in DB graphs as hiding a property is done in its configuration",new cljs.core.Keyword("feature","enable-block-timestamps?","feature/enable-block-timestamps?",155290768),"is not used in DB graphs as it is always enabled",new cljs.core.Keyword(null,"favorites","favorites",1740773480),"is not stored in config for DB graphs",new cljs.core.Keyword(null,"default-templates","default-templates",1374700421),"is replaced by #Template and the `Apply template to tags` property"], null)], 0));

//# sourceMappingURL=logseq.common.config.js.map
