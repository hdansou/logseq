import "./cljs_env.js";
import "./cljs.core.js";
import "./clojure.set.js";
import "./clojure.string.js";
import "./datascript.core.js";
import "./logseq.common.config.js";
import "./logseq.common.util.namespace.js";
import "./logseq.common.util.page_ref.js";
import "./logseq.common.uuid.js";
import "./logseq.db.frontend.class.js";
import "./logseq.db.frontend.entity_util.js";
import "./logseq.db.frontend.property.js";
goog.provide('logseq.db.frontend.db');
/**
 * Whether property a built-in property for the specific class
 */
logseq.db.frontend.db.built_in_class_property_QMARK_ = (function logseq$db$frontend$db$built_in_class_property_QMARK_(class_entity,property_entity){
var and__5140__auto__ = logseq.db.frontend.entity_util.built_in_QMARK_(class_entity);
if(cljs.core.truth_(and__5140__auto__)){
var and__5140__auto____$1 = logseq.db.frontend.entity_util.class_QMARK_(class_entity);
if(cljs.core.truth_(and__5140__auto____$1)){
var and__5140__auto____$2 = logseq.db.frontend.entity_util.built_in_QMARK_(property_entity);
if(cljs.core.truth_(and__5140__auto____$2)){
return cljs.core.contains_QMARK_(cljs.core.set(cljs.core.get_in.cljs$core$IFn$_invoke$arity$2((function (){var G__40550 = new cljs.core.Keyword("db","ident","db/ident",-737096).cljs$core$IFn$_invoke$arity$1(class_entity);
return (logseq.db.frontend.class$.built_in_classes.cljs$core$IFn$_invoke$arity$1 ? logseq.db.frontend.class$.built_in_classes.cljs$core$IFn$_invoke$arity$1(G__40550) : logseq.db.frontend.class$.built_in_classes.call(null,G__40550));
})(),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"schema","schema",-1582001791),new cljs.core.Keyword(null,"properties","properties",685819552)], null))),new cljs.core.Keyword("db","ident","db/ident",-737096).cljs$core$IFn$_invoke$arity$1(property_entity));
} else {
return and__5140__auto____$2;
}
} else {
return and__5140__auto____$1;
}
} else {
return and__5140__auto__;
}
});
/**
 * Private built-in pages should not be navigable or searchable by users. Later it
 * could be useful to use this for the All Pages view
 */
logseq.db.frontend.db.private_built_in_page_QMARK_ = (function logseq$db$frontend$db$private_built_in_page_QMARK_(page){
if(cljs.core.truth_(logseq.db.frontend.entity_util.property_QMARK_(page))){
return cljs.core.not(logseq.db.frontend.property.public_built_in_property_QMARK_(page));
} else {
if(cljs.core.truth_((function (){var or__5142__auto__ = logseq.db.frontend.entity_util.class_QMARK_(page);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return logseq.db.frontend.entity_util.internal_page_QMARK_(page);
}
})())){
return false;
} else {
return true;

}
}
});
/**
 * Builds tx for a favorite block in favorite page
 */
logseq.db.frontend.db.build_favorite_tx = (function logseq$db$frontend$db$build_favorite_tx(favorite_uuid){
return new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword("block","link","block/link",-1872399993),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("block","uuid","block/uuid",-1991494552),favorite_uuid], null),new cljs.core.Keyword("block","title","block/title",710445684),""], null);
});
logseq.db.frontend.db.get_all_properties = (function logseq$db$frontend$db$get_all_properties(db){
return cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (d){
var G__40552 = db;
var G__40553 = new cljs.core.Keyword(null,"e","e",1381269198).cljs$core$IFn$_invoke$arity$1(d);
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(G__40552,G__40553) : datascript.core.entity.call(null,G__40552,G__40553));
}),datascript.core.datoms.cljs$core$IFn$_invoke$arity$4(db,new cljs.core.Keyword(null,"avet","avet",1383857032),new cljs.core.Keyword("block","tags","block/tags",1814948340),new cljs.core.Keyword("logseq.class","Property","logseq.class/Property",1038767048)));
});
logseq.db.frontend.db.get_page_parents = (function logseq$db$frontend$db$get_page_parents(node){
var temp__5804__auto__ = new cljs.core.Keyword("block","parent","block/parent",-918309064).cljs$core$IFn$_invoke$arity$1(node);
if(cljs.core.truth_(temp__5804__auto__)){
var parent = temp__5804__auto__;
var current_parent = parent;
var parents_SINGLEQUOTE_ = cljs.core.PersistentVector.EMPTY;
while(true){
if(cljs.core.truth_((function (){var and__5140__auto__ = current_parent;
if(cljs.core.truth_(and__5140__auto__)){
return (!(cljs.core.contains_QMARK_(parents_SINGLEQUOTE_,current_parent)));
} else {
return and__5140__auto__;
}
})())){
var G__40607 = new cljs.core.Keyword("block","parent","block/parent",-918309064).cljs$core$IFn$_invoke$arity$1(current_parent);
var G__40608 = cljs.core.conj.cljs$core$IFn$_invoke$arity$2(parents_SINGLEQUOTE_,current_parent);
current_parent = G__40607;
parents_SINGLEQUOTE_ = G__40608;
continue;
} else {
return cljs.core.vec(cljs.core.reverse(parents_SINGLEQUOTE_));
}
break;
}
} else {
return null;
}
});
logseq.db.frontend.db.get_class_title_with_extends = (function logseq$db$frontend$db$get_class_title_with_extends(entity){
var extends$ = (function (){var G__40566 = new cljs.core.Keyword("logseq.property.class","extends","logseq.property.class/extends",747593803).cljs$core$IFn$_invoke$arity$1(entity);
var G__40566__$1 = (((G__40566 == null))?null:cljs.core.remove.cljs$core$IFn$_invoke$arity$2((function (extend){
var or__5142__auto__ = new cljs.core.Keyword("logseq.property","built-in?","logseq.property/built-in?",-1125958160).cljs$core$IFn$_invoke$arity$1(extend);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("block","title","block/title",710445684).cljs$core$IFn$_invoke$arity$1(entity),new cljs.core.Keyword("block","title","block/title",710445684).cljs$core$IFn$_invoke$arity$1(extend));
}
}),G__40566));
if((G__40566__$1 == null)){
return null;
} else {
return cljs.core.vec(G__40566__$1);
}
})();
if(cljs.core.seq(extends$)){
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2((1),cljs.core.count(extends$)))?new cljs.core.Keyword("block","title","block/title",710445684).cljs$core$IFn$_invoke$arity$1(cljs.core.first(extends$)):clojure.string.join.cljs$core$IFn$_invoke$arity$2(" | ",cljs.core.map.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("block","title","block/title",710445684),cljs.core.take.cljs$core$IFn$_invoke$arity$2((2),extends$)))))+cljs.core.str.cljs$core$IFn$_invoke$arity$1(logseq.common.util.namespace.parent_char)+cljs.core.str.cljs$core$IFn$_invoke$arity$1(new cljs.core.Keyword("block","title","block/title",710445684).cljs$core$IFn$_invoke$arity$1(entity)));
} else {
return new cljs.core.Keyword("block","title","block/title",710445684).cljs$core$IFn$_invoke$arity$1(entity);
}
});
logseq.db.frontend.db.get_title_with_parents = (function logseq$db$frontend$db$get_title_with_parents(entity){
if(cljs.core.truth_(logseq.db.frontend.entity_util.class_QMARK_(entity))){
return logseq.db.frontend.db.get_class_title_with_extends(entity);
} else {
if(cljs.core.truth_(logseq.db.frontend.entity_util.page_QMARK_(entity))){
var parents_SINGLEQUOTE_ = cljs.core.remove.cljs$core$IFn$_invoke$arity$2((function (e){
var and__5140__auto__ = new cljs.core.Keyword("logseq.property","built-in?","logseq.property/built-in?",-1125958160).cljs$core$IFn$_invoke$arity$1(e);
if(cljs.core.truth_(and__5140__auto__)){
return cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(logseq.common.config.library_page_name,new cljs.core.Keyword("block","title","block/title",710445684).cljs$core$IFn$_invoke$arity$1(e));
} else {
return and__5140__auto__;
}
}),logseq.db.frontend.db.get_page_parents(entity));
return clojure.string.join.cljs$core$IFn$_invoke$arity$2(logseq.common.util.namespace.parent_char,cljs.core.map.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("block","title","block/title",710445684),cljs.core.conj.cljs$core$IFn$_invoke$arity$2(cljs.core.vec(parents_SINGLEQUOTE_),entity)));
} else {
return new cljs.core.Keyword("block","title","block/title",710445684).cljs$core$IFn$_invoke$arity$1(entity);

}
}
});
/**
 * Returns all parents of all classes. Like get-class-extends but for multiple classes
 */
logseq.db.frontend.db.get_classes_parents = (function logseq$db$frontend$db$get_classes_parents(tags){
var tags_SINGLEQUOTE_ = cljs.core.filter.cljs$core$IFn$_invoke$arity$2(logseq.db.frontend.entity_util.class_QMARK_,tags);
var result = cljs.core.mapcat.cljs$core$IFn$_invoke$arity$variadic(logseq.db.frontend.class$.get_class_extends,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([tags_SINGLEQUOTE_], 0));
return cljs.core.set(result);
});
/**
 * Whether `object` is an instance of `class`
 */
logseq.db.frontend.db.class_instance_QMARK_ = (function logseq$db$frontend$db$class_instance_QMARK_(class$,object){
var tags = new cljs.core.Keyword("block","tags","block/tags",1814948340).cljs$core$IFn$_invoke$arity$1(object);
var tags_ids = cljs.core.set(cljs.core.map.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db","id","db/id",-1388397098),tags));
var or__5142__auto__ = cljs.core.contains_QMARK_(tags_ids,new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(class$));
if(or__5142__auto__){
return or__5142__auto__;
} else {
var class_parent_ids = cljs.core.set(cljs.core.map.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db","id","db/id",-1388397098),logseq.db.frontend.db.get_classes_parents(tags)));
return cljs.core.contains_QMARK_(clojure.set.union.cljs$core$IFn$_invoke$arity$2(class_parent_ids,tags_ids),new cljs.core.Keyword("db","id","db/id",-1388397098).cljs$core$IFn$_invoke$arity$1(class$));
}
});
logseq.db.frontend.db.inline_tag_QMARK_ = (function logseq$db$frontend$db$inline_tag_QMARK_(block_raw_title,tag){
if(typeof block_raw_title === 'string'){
} else {
throw (new Error((""+"Assert failed: "+"block-raw-title should be a string"+"\n"+"(string? block-raw-title)")));
}

return clojure.string.includes_QMARK_(block_raw_title,(""+"#"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(logseq.common.util.page_ref.__GT_page_ref(new cljs.core.Keyword("block","uuid","block/uuid",-1991494552).cljs$core$IFn$_invoke$arity$1(tag)))));
});
if((typeof logseq !== 'undefined') && (typeof logseq.db !== 'undefined') && (typeof logseq.db.frontend !== 'undefined') && (typeof logseq.db.frontend.db !== 'undefined') && (typeof logseq.db.frontend.db.node_display_type_classes !== 'undefined')){
} else {
logseq.db.frontend.db.node_display_type_classes = new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword("logseq.class","Quote-block","logseq.class/Quote-block",-1176166617),null,new cljs.core.Keyword("logseq.class","Math-block","logseq.class/Math-block",-2038963121),null,new cljs.core.Keyword("logseq.class","Code-block","logseq.class/Code-block",1454986641),null], null), null);
}
logseq.db.frontend.db.get_class_ident_by_display_type = (function logseq$db$frontend$db$get_class_ident_by_display_type(display_type){
var G__40592 = display_type;
var G__40592__$1 = (((G__40592 instanceof cljs.core.Keyword))?G__40592.fqn:null);
switch (G__40592__$1) {
case "code":
return new cljs.core.Keyword("logseq.class","Code-block","logseq.class/Code-block",1454986641);

break;
case "math":
return new cljs.core.Keyword("logseq.class","Math-block","logseq.class/Math-block",-2038963121);

break;
case "quote":
return new cljs.core.Keyword("logseq.class","Quote-block","logseq.class/Quote-block",-1176166617);

break;
default:
return null;

}
});
logseq.db.frontend.db.get_display_type_by_class_ident = (function logseq$db$frontend$db$get_display_type_by_class_ident(class_ident){
var G__40593 = class_ident;
var G__40593__$1 = (((G__40593 instanceof cljs.core.Keyword))?G__40593.fqn:null);
switch (G__40593__$1) {
case "logseq.class/Code-block":
return new cljs.core.Keyword(null,"code","code",1586293142);

break;
case "logseq.class/Math-block":
return new cljs.core.Keyword(null,"math","math",-2026912803);

break;
case "logseq.class/Quote-block":
return new cljs.core.Keyword(null,"quote","quote",-262615245);

break;
default:
return null;

}
});
logseq.db.frontend.db.get_built_in_page = (function logseq$db$frontend$db$get_built_in_page(db,title){
if(cljs.core.truth_(db)){
var id = logseq.common.uuid.gen_uuid.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"builtin-block-uuid","builtin-block-uuid",-652923992),title);
var G__40597 = db;
var G__40598 = new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword("block","uuid","block/uuid",-1991494552),id], null);
return (datascript.core.entity.cljs$core$IFn$_invoke$arity$2 ? datascript.core.entity.cljs$core$IFn$_invoke$arity$2(G__40597,G__40598) : datascript.core.entity.call(null,G__40597,G__40598));
} else {
return null;
}
});
logseq.db.frontend.db.library_QMARK_ = (function logseq$db$frontend$db$library_QMARK_(page){
var and__5140__auto__ = logseq.db.frontend.entity_util.built_in_QMARK_(page);
if(cljs.core.truth_(and__5140__auto__)){
return cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(logseq.common.config.library_page_name,new cljs.core.Keyword("block","title","block/title",710445684).cljs$core$IFn$_invoke$arity$1(page));
} else {
return and__5140__auto__;
}
});

//# sourceMappingURL=logseq.db.frontend.db.js.map
