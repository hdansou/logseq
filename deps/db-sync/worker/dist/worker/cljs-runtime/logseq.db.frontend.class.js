import "./cljs_env.js";
import "./cljs.core.js";
import "./clojure.set.js";
import "./clojure.string.js";
import "./datascript.core.js";
import "./datascript.impl.entity.js";
import "./flatland.ordered.map.js";
import "./logseq.common.defkeywords.js";
import "./logseq.db.frontend.db_ident.js";
import "./logseq.db.frontend.entity_util.js";
import "./logseq.db.frontend.rules.js";
import "./logseq.db.sqlite.util.js";
goog.provide('logseq.db.frontend.class$');
/**
 * Map of built-in classes for db graphs with their :db/ident as keys
 */
logseq.db.frontend.class$.built_in_classes = cljs.core.apply.cljs$core$IFn$_invoke$arity$2(flatland.ordered.map.ordered_map,cljs.core.PersistentVector.fromArray([new cljs.core.Keyword("logseq.class","Root","logseq.class/Root",273783827),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"title","title",636505583),"Root Tag"], null),new cljs.core.Keyword("logseq.class","Tag","logseq.class/Tag",-538902083),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"title","title",636505583),"Tag"], null),new cljs.core.Keyword("logseq.class","Property","logseq.class/Property",1038767048),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"title","title",636505583),"Property"], null),new cljs.core.Keyword("logseq.class","Page","logseq.class/Page",1484340329),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"title","title",636505583),"Page"], null),new cljs.core.Keyword("logseq.class","Journal","logseq.class/Journal",1979741081),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"title","title",636505583),"Journal",new cljs.core.Keyword(null,"properties","properties",685819552),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword("logseq.property.class","extends","logseq.property.class/extends",747593803),new cljs.core.Keyword("logseq.class","Page","logseq.class/Page",1484340329),new cljs.core.Keyword("logseq.property.journal","title-format","logseq.property.journal/title-format",-1536497954),"MMM do, yyyy"], null)], null),new cljs.core.Keyword("logseq.class","Whiteboard","logseq.class/Whiteboard",1013698452),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"title","title",636505583),"Whiteboard",new cljs.core.Keyword(null,"properties","properties",685819552),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword("logseq.property.class","extends","logseq.property.class/extends",747593803),new cljs.core.Keyword("logseq.class","Page","logseq.class/Page",1484340329)], null)], null),new cljs.core.Keyword("logseq.class","Task","logseq.class/Task",-1282181457),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"title","title",636505583),"Task",new cljs.core.Keyword(null,"schema","schema",-1582001791),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"properties","properties",685819552),new cljs.core.PersistentVector(null, 4, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("logseq.property","status","logseq.property/status",-907216853),new cljs.core.Keyword("logseq.property","priority","logseq.property/priority",239228411),new cljs.core.Keyword("logseq.property","deadline","logseq.property/deadline",1685901604),new cljs.core.Keyword("logseq.property","scheduled","logseq.property/scheduled",1644520943)], null)], null)], null),new cljs.core.Keyword("logseq.class","Query","logseq.class/Query",-232480166),new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"title","title",636505583),"Query",new cljs.core.Keyword(null,"properties","properties",685819552),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword("logseq.property","icon","logseq.property/icon",589123285),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"type","type",1174270348),new cljs.core.Keyword(null,"tabler-icon","tabler-icon",945975358),new cljs.core.Keyword(null,"id","id",-1388402092),"search"], null)], null),new cljs.core.Keyword(null,"schema","schema",-1582001791),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"properties","properties",685819552),new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("logseq.property","query","logseq.property/query",-97414126)], null)], null)], null),new cljs.core.Keyword("logseq.class","Card","logseq.class/Card",-1358281109),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"title","title",636505583),"Card",new cljs.core.Keyword(null,"schema","schema",-1582001791),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"properties","properties",685819552),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("logseq.property.fsrs","state","logseq.property.fsrs/state",-1165165087),new cljs.core.Keyword("logseq.property.fsrs","due","logseq.property.fsrs/due",-1089080549)], null)], null)], null),new cljs.core.Keyword("logseq.class","Cards","logseq.class/Cards",-1284265167),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"title","title",636505583),"Cards",new cljs.core.Keyword(null,"properties","properties",685819552),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword("logseq.property","icon","logseq.property/icon",589123285),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"type","type",1174270348),new cljs.core.Keyword(null,"tabler-icon","tabler-icon",945975358),new cljs.core.Keyword(null,"id","id",-1388402092),"search"], null),new cljs.core.Keyword("logseq.property.class","extends","logseq.property.class/extends",747593803),new cljs.core.Keyword("logseq.class","Query","logseq.class/Query",-232480166)], null)], null),new cljs.core.Keyword("logseq.class","Asset","logseq.class/Asset",-797502970),new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"title","title",636505583),"Asset",new cljs.core.Keyword(null,"properties","properties",685819552),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword("logseq.property.class","hide-from-node","logseq.property.class/hide-from-node",-26103727),true,new cljs.core.Keyword("logseq.property.view","type","logseq.property.view/type",218237607),new cljs.core.Keyword("logseq.property.view","type.gallery","logseq.property.view/type.gallery",150605112)], null),new cljs.core.Keyword(null,"schema","schema",-1582001791),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"properties","properties",685819552),new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("logseq.property.asset","type","logseq.property.asset/type",-1142083098),new cljs.core.Keyword("logseq.property.asset","size","logseq.property.asset/size",-116786219),new cljs.core.Keyword("logseq.property.asset","checksum","logseq.property.asset/checksum",-1011416979)], null),new cljs.core.Keyword(null,"required-properties","required-properties",1219426728),new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("logseq.property.asset","type","logseq.property.asset/type",-1142083098),new cljs.core.Keyword("logseq.property.asset","size","logseq.property.asset/size",-116786219),new cljs.core.Keyword("logseq.property.asset","checksum","logseq.property.asset/checksum",-1011416979)], null)], null)], null),new cljs.core.Keyword("logseq.class","Code-block","logseq.class/Code-block",1454986641),new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"title","title",636505583),"Code",new cljs.core.Keyword(null,"properties","properties",685819552),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword("logseq.property.class","hide-from-node","logseq.property.class/hide-from-node",-26103727),true], null),new cljs.core.Keyword(null,"schema","schema",-1582001791),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"properties","properties",685819552),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("logseq.property.node","display-type","logseq.property.node/display-type",442446189),new cljs.core.Keyword("logseq.property.code","lang","logseq.property.code/lang",-857897165)], null)], null)], null),new cljs.core.Keyword("logseq.class","Quote-block","logseq.class/Quote-block",-1176166617),new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"title","title",636505583),"Quote",new cljs.core.Keyword(null,"properties","properties",685819552),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword("logseq.property.class","hide-from-node","logseq.property.class/hide-from-node",-26103727),true], null),new cljs.core.Keyword(null,"schema","schema",-1582001791),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"properties","properties",685819552),new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("logseq.property.node","display-type","logseq.property.node/display-type",442446189)], null)], null)], null),new cljs.core.Keyword("logseq.class","Math-block","logseq.class/Math-block",-2038963121),new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"title","title",636505583),"Math",new cljs.core.Keyword(null,"properties","properties",685819552),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword("logseq.property.class","hide-from-node","logseq.property.class/hide-from-node",-26103727),true], null),new cljs.core.Keyword(null,"schema","schema",-1582001791),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"properties","properties",685819552),new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("logseq.property.node","display-type","logseq.property.node/display-type",442446189)], null)], null)], null),new cljs.core.Keyword("logseq.class","Pdf-annotation","logseq.class/Pdf-annotation",-504959620),new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"title","title",636505583),"PDF Annotation",new cljs.core.Keyword(null,"properties","properties",685819552),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword("logseq.property.class","hide-from-node","logseq.property.class/hide-from-node",-26103727),true], null),new cljs.core.Keyword(null,"schema","schema",-1582001791),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"properties","properties",685819552),new cljs.core.PersistentVector(null, 7, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("logseq.property","ls-type","logseq.property/ls-type",326979345),new cljs.core.Keyword("logseq.property.pdf","hl-color","logseq.property.pdf/hl-color",-674793887),new cljs.core.Keyword("logseq.property","asset","logseq.property/asset",876856790),new cljs.core.Keyword("logseq.property.pdf","hl-page","logseq.property.pdf/hl-page",-753284596),new cljs.core.Keyword("logseq.property.pdf","hl-value","logseq.property.pdf/hl-value",545829402),new cljs.core.Keyword("logseq.property.pdf","hl-type","logseq.property.pdf/hl-type",-998437832),new cljs.core.Keyword("logseq.property.pdf","hl-image","logseq.property.pdf/hl-image",137767009)], null),new cljs.core.Keyword(null,"required-properties","required-properties",1219426728),new cljs.core.PersistentVector(null, 5, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("logseq.property","ls-type","logseq.property/ls-type",326979345),new cljs.core.Keyword("logseq.property.pdf","hl-color","logseq.property.pdf/hl-color",-674793887),new cljs.core.Keyword("logseq.property","asset","logseq.property/asset",876856790),new cljs.core.Keyword("logseq.property.pdf","hl-page","logseq.property.pdf/hl-page",-753284596),new cljs.core.Keyword("logseq.property.pdf","hl-value","logseq.property.pdf/hl-value",545829402)], null)], null)], null),new cljs.core.Keyword("logseq.class","Template","logseq.class/Template",1720854846),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"title","title",636505583),"Template",new cljs.core.Keyword(null,"schema","schema",-1582001791),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"properties","properties",685819552),new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("logseq.property","template-applied-to","logseq.property/template-applied-to",-429124322)], null)], null)], null)],true));
/**
 * Children of :logseq.class/Page
 */
logseq.db.frontend.class$.page_children_classes = cljs.core.set(cljs.core.keep.cljs$core$IFn$_invoke$arity$2((function (p__40502){
var vec__40503 = p__40502;
var class_ident = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40503,(0),null);
var m = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__40503,(1),null);
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(cljs.core.get_in.cljs$core$IFn$_invoke$arity$2(m,new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"properties","properties",685819552),new cljs.core.Keyword("logseq.property.class","extends","logseq.property.class/extends",747593803)], null)),new cljs.core.Keyword("logseq.class","Page","logseq.class/Page",1484340329))){
return class_ident;
} else {
return null;
}
}),logseq.db.frontend.class$.built_in_classes));
/**
 * Built-in classes that behave like a page. Classes should match entity-util/page?
 */
logseq.db.frontend.class$.page_classes = cljs.core.into.cljs$core$IFn$_invoke$arity$2(new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword("logseq.class","Property","logseq.class/Property",1038767048),null,new cljs.core.Keyword("logseq.class","Page","logseq.class/Page",1484340329),null,new cljs.core.Keyword("logseq.class","Tag","logseq.class/Tag",-538902083),null], null), null),logseq.db.frontend.class$.page_children_classes);
/**
 * Built-in classes that are hidden on a node and all pages view
 */
logseq.db.frontend.class$.internal_tags = new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 5, [new cljs.core.Keyword("logseq.class","Asset","logseq.class/Asset",-797502970),null,new cljs.core.Keyword("logseq.class","Property","logseq.class/Property",1038767048),null,new cljs.core.Keyword("logseq.class","Page","logseq.class/Page",1484340329),null,new cljs.core.Keyword("logseq.class","Root","logseq.class/Root",273783827),null,new cljs.core.Keyword("logseq.class","Tag","logseq.class/Tag",-538902083),null], null), null);
/**
 * Built-in classes that are private and should not be used by a user directly.
 */
logseq.db.frontend.class$.private_tags = clojure.set.union.cljs$core$IFn$_invoke$arity$2(cljs.core.disj.cljs$core$IFn$_invoke$arity$2(logseq.db.frontend.class$.internal_tags,new cljs.core.Keyword("logseq.class","Root","logseq.class/Root",273783827)),new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword("logseq.class","Whiteboard","logseq.class/Whiteboard",1013698452),null,new cljs.core.Keyword("logseq.class","Journal","logseq.class/Journal",1979741081),null,new cljs.core.Keyword("logseq.class","Pdf-annotation","logseq.class/Pdf-annotation",-504959620),null], null), null));
logseq.db.frontend.class$.block_kind_tags = new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 7, [new cljs.core.Keyword("logseq.class","Quote-block","logseq.class/Quote-block",-1176166617),null,new cljs.core.Keyword("logseq.class","Math-block","logseq.class/Math-block",-2038963121),null,new cljs.core.Keyword("logseq.class","Code-block","logseq.class/Code-block",1454986641),null,new cljs.core.Keyword("logseq.class","Cards","logseq.class/Cards",-1284265167),null,new cljs.core.Keyword("logseq.class","Query","logseq.class/Query",-232480166),null,new cljs.core.Keyword("logseq.class","Pdf-annotation","logseq.class/Pdf-annotation",-504959620),null,new cljs.core.Keyword("logseq.class","Template","logseq.class/Template",1720854846),null], null), null);
/**
 * Classes that should be removed from inline tags
 */
logseq.db.frontend.class$.disallowed_inline_tags = clojure.set.union.cljs$core$IFn$_invoke$arity$variadic(logseq.db.frontend.class$.page_classes,logseq.db.frontend.class$.private_tags,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([logseq.db.frontend.class$.block_kind_tags], 0));
/**
 * Built-in classes that are hidden when choosing extends
 */
logseq.db.frontend.class$.extends_hidden_tags = clojure.set.union.cljs$core$IFn$_invoke$arity$2(logseq.db.frontend.class$.private_tags,logseq.db.frontend.class$.block_kind_tags);
/**
 * Built-in classes that are hidden in a few contexts like property values
 */
logseq.db.frontend.class$.hidden_tags = new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword("logseq.class","Asset","logseq.class/Asset",-797502970),null,new cljs.core.Keyword("logseq.class","Page","logseq.class/Page",1484340329),null,new cljs.core.Keyword("logseq.class","Root","logseq.class/Root",273783827),null], null), null);
/**
 * Returns all children of a class
 */
logseq.db.frontend.class$.get_structured_children = (function logseq$db$frontend$class$get_structured_children(db,eid){
return cljs.core.remove.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentHashSet.createAsIfByAssoc([eid]),(function (){var G__40512 = new cljs.core.PersistentVector(null, 8, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"find","find",496279456),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Symbol(null,"?c","?c",870679775,null),new cljs.core.Symbol(null,"...","...",-1926939749,null)], null),new cljs.core.Keyword(null,"in","in",-1531184865),new cljs.core.Symbol(null,"$","$",-1580747756,null),new cljs.core.Symbol(null,"?p","?p",-10896580,null),new cljs.core.Symbol(null,"%","%",-950237169,null),new cljs.core.Keyword(null,"where","where",-2044795965),cljs.core.list(new cljs.core.Symbol(null,"class-extends","class-extends",-1824144665,null),new cljs.core.Symbol(null,"?p","?p",-10896580,null),new cljs.core.Symbol(null,"?c","?c",870679775,null))], null);
var G__40513 = db;
var G__40514 = eid;
var G__40515 = new cljs.core.Keyword(null,"class-extends","class-extends",830291104).cljs$core$IFn$_invoke$arity$1(logseq.db.frontend.rules.rules);
return (datascript.core.q.cljs$core$IFn$_invoke$arity$4 ? datascript.core.q.cljs$core$IFn$_invoke$arity$4(G__40512,G__40513,G__40514,G__40515) : datascript.core.q.call(null,G__40512,G__40513,G__40514,G__40515));
})());
});
/**
 * Returns all extends of a class
 */
logseq.db.frontend.class$.get_class_extends = (function logseq$db$frontend$class$get_class_extends(class$){
if(datascript.impl.entity.entity_QMARK_(class$)){
} else {
throw (new Error((""+"Assert failed: "+"get-class-extends `class` should be an entity"+"\n"+"(de/entity? class)")));
}

var extends$ = new cljs.core.Keyword("logseq.property.class","extends","logseq.property.class/extends",747593803).cljs$core$IFn$_invoke$arity$1(class$);
var result = cljs.core.PersistentVector.EMPTY;
while(true){
if(cljs.core.seq(extends$)){
var G__40543 = cljs.core.mapcat.cljs$core$IFn$_invoke$arity$variadic(new cljs.core.Keyword("logseq.property.class","extends","logseq.property.class/extends",747593803),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([extends$], 0));
var G__40544 = cljs.core.into.cljs$core$IFn$_invoke$arity$2(result,extends$);
extends$ = G__40543;
result = G__40544;
continue;
} else {
return cljs.core.reverse(cljs.core.distinct.cljs$core$IFn$_invoke$arity$1(result));
}
break;
}
});
/**
 * Creates a class :db/ident for a default user namespace.
 * NOTE: Only use this when creating a db-ident for a new class.
 */
logseq.db.frontend.class$.create_user_class_ident_from_name = (function logseq$db$frontend$class$create_user_class_ident_from_name(var_args){
var args__5882__auto__ = [];
var len__5876__auto___40545 = arguments.length;
var i__5877__auto___40546 = (0);
while(true){
if((i__5877__auto___40546 < len__5876__auto___40545)){
args__5882__auto__.push((arguments[i__5877__auto___40546]));

var G__40547 = (i__5877__auto___40546 + (1));
i__5877__auto___40546 = G__40547;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((2) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((2)),(0),null)):null);
return logseq.db.frontend.class$.create_user_class_ident_from_name.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),(arguments[(1)]),argseq__5883__auto__);
});

(logseq.db.frontend.class$.create_user_class_ident_from_name.cljs$core$IFn$_invoke$arity$variadic = (function (db,class_name,p__40525){
var map__40526 = p__40525;
var map__40526__$1 = cljs.core.__destructure_map(map__40526);
var ident_namespace = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__40526__$1,new cljs.core.Keyword(null,"ident-namespace","ident-namespace",2103796218));
var ident_ns = (function (){var or__5142__auto__ = ident_namespace;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return "user.class";
}
})();
var db_ident = logseq.db.frontend.db_ident.create_db_ident_from_name(ident_ns,class_name);
if(cljs.core.truth_(db)){
return logseq.db.frontend.db_ident.ensure_unique_db_ident(db,db_ident);
} else {
return db_ident;
}
}));

(logseq.db.frontend.class$.create_user_class_ident_from_name.cljs$lang$maxFixedArity = (2));

/** @this {Function} */
(logseq.db.frontend.class$.create_user_class_ident_from_name.cljs$lang$applyTo = (function (seq40522){
var G__40523 = cljs.core.first(seq40522);
var seq40522__$1 = cljs.core.next(seq40522);
var G__40524 = cljs.core.first(seq40522__$1);
var seq40522__$2 = cljs.core.next(seq40522__$1);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__40523,G__40524,seq40522__$2);
}));

/**
 * Builds a new class with a unique :db/ident. Also throws exception for user
 *   facing messages when name is invalid
 */
logseq.db.frontend.class$.build_new_class = (function logseq$db$frontend$class$build_new_class(var_args){
var args__5882__auto__ = [];
var len__5876__auto___40548 = arguments.length;
var i__5877__auto___40549 = (0);
while(true){
if((i__5877__auto___40549 < len__5876__auto___40548)){
args__5882__auto__.push((arguments[i__5877__auto___40549]));

var G__40551 = (i__5877__auto___40549 + (1));
i__5877__auto___40549 = G__40551;
continue;
} else {
}
break;
}

var argseq__5883__auto__ = ((((2) < args__5882__auto__.length))?(new cljs.core.IndexedSeq(args__5882__auto__.slice((2)),(0),null)):null);
return logseq.db.frontend.class$.build_new_class.cljs$core$IFn$_invoke$arity$variadic((arguments[(0)]),(arguments[(1)]),argseq__5883__auto__);
});

(logseq.db.frontend.class$.build_new_class.cljs$core$IFn$_invoke$arity$variadic = (function (db,page_m,p__40530){
var map__40531 = p__40530;
var map__40531__$1 = cljs.core.__destructure_map(map__40531);
var option = map__40531__$1;
if(typeof new cljs.core.Keyword("block","title","block/title",710445684).cljs$core$IFn$_invoke$arity$1(page_m) === 'string'){
} else {
throw (new Error("Assert failed: (string? (:block/title page-m))"));
}

var db_ident = logseq.db.frontend.class$.create_user_class_ident_from_name.cljs$core$IFn$_invoke$arity$variadic(db,new cljs.core.Keyword("block","title","block/title",710445684).cljs$core$IFn$_invoke$arity$1(page_m),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([option], 0));
return logseq.db.sqlite.util.build_new_class(cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(page_m,new cljs.core.Keyword("db","ident","db/ident",-737096),db_ident));
}));

(logseq.db.frontend.class$.build_new_class.cljs$lang$maxFixedArity = (2));

/** @this {Function} */
(logseq.db.frontend.class$.build_new_class.cljs$lang$applyTo = (function (seq40527){
var G__40528 = cljs.core.first(seq40527);
var seq40527__$1 = cljs.core.next(seq40527);
var G__40529 = cljs.core.first(seq40527__$1);
var seq40527__$2 = cljs.core.next(seq40527__$1);
var self__5861__auto__ = this;
return self__5861__auto__.cljs$core$IFn$_invoke$arity$variadic(G__40528,G__40529,seq40527__$2);
}));

if((typeof logseq !== 'undefined') && (typeof logseq.db !== 'undefined') && (typeof logseq.db.frontend !== 'undefined') && (typeof logseq.db.frontend.class$ !== 'undefined') && (typeof logseq.db.frontend.class$.logseq_class !== 'undefined')){
} else {
logseq.db.frontend.class$.logseq_class = "logseq.class";
}
/**
 * Determines if keyword is a logseq class
 */
logseq.db.frontend.class$.logseq_class_QMARK_ = (function logseq$db$frontend$class$logseq_class_QMARK_(kw){
return cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(logseq.db.frontend.class$.logseq_class,cljs.core.namespace(kw));
});
/**
 * Determines if namespace string is a user class
 */
logseq.db.frontend.class$.user_class_namespace_QMARK_ = (function logseq$db$frontend$class$user_class_namespace_QMARK_(s){
return clojure.string.includes_QMARK_(s,".class");
});
/**
 * Get class objects including children classes'
 */
logseq.db.frontend.class$.get_class_objects = (function logseq$db$frontend$class$get_class_objects(db,class_id){
var class_children = logseq.db.frontend.class$.get_structured_children(db,class_id);
var class_ids = cljs.core.distinct.cljs$core$IFn$_invoke$arity$1(cljs.core.conj.cljs$core$IFn$_invoke$arity$2(class_children,class_id));
var datoms = cljs.core.mapcat.cljs$core$IFn$_invoke$arity$variadic((function (id){
return datascript.core.datoms.cljs$core$IFn$_invoke$arity$4(db,new cljs.core.Keyword(null,"avet","avet",1383857032),new cljs.core.Keyword("block","tags","block/tags",1814948340),id);
}),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([class_ids], 0));
var non_hidden_e = (function (id){
var e = (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(db,id) : datascript.core.entity.call(null,db,id));
if(logseq.db.frontend.entity_util.hidden_QMARK_(e)){
return null;
} else {
return e;
}
});
return cljs.core.keep.cljs$core$IFn$_invoke$arity$2((function (d){
return non_hidden_e(new cljs.core.Keyword(null,"e","e",1381269198).cljs$core$IFn$_invoke$arity$1(d));
}),datoms);
});

//# sourceMappingURL=logseq.db.frontend.class.js.map
