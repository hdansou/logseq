import "./cljs_env.js";
import "./cljs.core.js";
import "./clojure.set.js";
import "./clojure.string.js";
goog.provide('logseq.db.frontend.schema');
logseq.db.frontend.schema.schema_version_QMARK_ = cljs.core.every_pred.cljs$core$IFn$_invoke$arity$2(cljs.core.map_QMARK_,new cljs.core.Keyword(null,"major","major",-27376078));
/**
 * Return schema-version({:major <num> :minor <num>}).
 *   supported input: 10, "10.1", [10 1]
 */
logseq.db.frontend.schema.parse_schema_version = (function logseq$db$frontend$schema$parse_schema_version(string_or_compatible_number){
if(cljs.core.truth_(logseq.db.frontend.schema.schema_version_QMARK_(string_or_compatible_number))){
return string_or_compatible_number;
} else {
if(cljs.core.truth_((function (){var and__5140__auto__ = cljs.core.sequential_QMARK_(string_or_compatible_number);
if(and__5140__auto__){
return cljs.core.first(string_or_compatible_number);
} else {
return and__5140__auto__;
}
})())){
return new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"major","major",-27376078),cljs.core.first(string_or_compatible_number),new cljs.core.Keyword(null,"minor","minor",-608536071),cljs.core.second(string_or_compatible_number)], null);
} else {
if(cljs.core.int_QMARK_(string_or_compatible_number)){
return new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"major","major",-27376078),string_or_compatible_number,new cljs.core.Keyword(null,"minor","minor",-608536071),null], null);
} else {
if(typeof string_or_compatible_number === 'string'){
var vec__50481 = cljs.core.map.cljs$core$IFn$_invoke$arity$2(cljs.core.parse_long,clojure.string.split.cljs$core$IFn$_invoke$arity$2(string_or_compatible_number,/\./));
var major = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50481,(0),null);
var minor = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50481,(1),null);
if((!((major == null)))){
} else {
throw (new Error("Assert failed: (some? major)"));
}

return new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"major","major",-27376078),major,new cljs.core.Keyword(null,"minor","minor",-608536071),minor], null);
} else {
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2((""+"Bad schema version: "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(string_or_compatible_number)),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"data","data",-232669377),string_or_compatible_number], null));

}
}
}
}
});
logseq.db.frontend.schema.compare_schema_version = (function logseq$db$frontend$schema$compare_schema_version(x,y){
return cljs.core.apply.cljs$core$IFn$_invoke$arity$2(cljs.core.compare,cljs.core.map.cljs$core$IFn$_invoke$arity$2(cljs.core.juxt.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"major","major",-27376078),new cljs.core.Keyword(null,"minor","minor",-608536071)),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [logseq.db.frontend.schema.parse_schema_version(x),logseq.db.frontend.schema.parse_schema_version(y)], null)));
});
logseq.db.frontend.schema.version = logseq.db.frontend.schema.parse_schema_version("65.22");
/**
 * Return a number.
 *   Compatible with current schema-version number.
 *   schema-version-old: 10, a number
 *   schema-version-new: "12.34", string, <major-num>.<minor-num>
 */
logseq.db.frontend.schema.major_version = (function logseq$db$frontend$schema$major_version(schema_version){
if(cljs.core.truth_(logseq.db.frontend.schema.schema_version_QMARK_(schema_version))){
return new cljs.core.Keyword(null,"major","major",-27376078).cljs$core$IFn$_invoke$arity$1(schema_version);
} else {
return new cljs.core.Keyword(null,"major","major",-27376078).cljs$core$IFn$_invoke$arity$1(logseq.db.frontend.schema.parse_schema_version(schema_version));
}
});
logseq.db.frontend.schema.schema_version__GT_string = (function logseq$db$frontend$schema$schema_version__GT_string(schema_version){
if(typeof schema_version === 'string'){
return schema_version;
} else {
if(cljs.core.int_QMARK_(schema_version)){
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(schema_version));
} else {
if(cljs.core.truth_(logseq.db.frontend.schema.schema_version_QMARK_(schema_version))){
var temp__5802__auto__ = new cljs.core.Keyword(null,"minor","minor",-608536071).cljs$core$IFn$_invoke$arity$1(schema_version);
if(cljs.core.truth_(temp__5802__auto__)){
var minor = temp__5802__auto__;
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(new cljs.core.Keyword(null,"major","major",-27376078).cljs$core$IFn$_invoke$arity$1(schema_version))+"."+cljs.core.str.cljs$core$IFn$_invoke$arity$1(minor));
} else {
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(new cljs.core.Keyword(null,"major","major",-27376078).cljs$core$IFn$_invoke$arity$1(schema_version)));
}
} else {
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2("Not a schema-version",new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"data","data",-232669377),schema_version], null));

}
}
}
});
/**
 * Schema for DB graphs
 */
logseq.db.frontend.schema.schema = cljs.core.PersistentHashMap.fromArrays([new cljs.core.Keyword("file","created-at","file/created-at",-92397056),new cljs.core.Keyword("block","tx-id","block/tx-id",547556161),new cljs.core.Keyword("file","content","file/content",12680964),new cljs.core.Keyword("block","alias","block/alias",-2112644699),new cljs.core.Keyword("kv","value","kv/value",305981670),new cljs.core.Keyword("block","link","block/link",-1872399993),new cljs.core.Keyword("block","uuid","block/uuid",-1991494552),new cljs.core.Keyword("block","updated-at","block/updated-at",-1516550551),new cljs.core.Keyword("file","size","file/size",1053598731),new cljs.core.Keyword("block","refs","block/refs",-1214495349),new cljs.core.Keyword("block","closed-value-property","block/closed-value-property",1157792813),new cljs.core.Keyword("file","last-modified-at","file/last-modified-at",473527310),new cljs.core.Keyword("block","created-at","block/created-at",1440015),new cljs.core.Keyword("block","collapsed?","block/collapsed?",2140210991),new cljs.core.Keyword("block","journal-day","block/journal-day",-145748366),new cljs.core.Keyword("block","tags","block/tags",1814948340),new cljs.core.Keyword("block","title","block/title",710445684),new cljs.core.Keyword("db","ident","db/ident",-737096),new cljs.core.Keyword("block","parent","block/parent",-918309064),new cljs.core.Keyword("block","order","block/order",-1429282437),new cljs.core.Keyword("block","page","block/page",822314108),new cljs.core.Keyword("block","name","block/name",1619760316),new cljs.core.Keyword("file","path","file/path",-191335748)],[cljs.core.PersistentArrayMap.EMPTY,cljs.core.PersistentArrayMap.EMPTY,cljs.core.PersistentArrayMap.EMPTY,new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword("db","valueType","db/valueType",1827971944),new cljs.core.Keyword("db.type","ref","db.type/ref",-1728373079),new cljs.core.Keyword("db","cardinality","db/cardinality",-104975659),new cljs.core.Keyword("db.cardinality","many","db.cardinality/many",772806234),new cljs.core.Keyword("db","index","db/index",-1531680669),true], null),cljs.core.PersistentArrayMap.EMPTY,new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword("db","valueType","db/valueType",1827971944),new cljs.core.Keyword("db.type","ref","db.type/ref",-1728373079),new cljs.core.Keyword("db","index","db/index",-1531680669),true], null),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword("db","unique","db/unique",329396388),new cljs.core.Keyword("db.unique","identity","db.unique/identity",1675950722)], null),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword("db","index","db/index",-1531680669),true], null),cljs.core.PersistentArrayMap.EMPTY,new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword("db","valueType","db/valueType",1827971944),new cljs.core.Keyword("db.type","ref","db.type/ref",-1728373079),new cljs.core.Keyword("db","cardinality","db/cardinality",-104975659),new cljs.core.Keyword("db.cardinality","many","db.cardinality/many",772806234)], null),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword("db","valueType","db/valueType",1827971944),new cljs.core.Keyword("db.type","ref","db.type/ref",-1728373079),new cljs.core.Keyword("db","cardinality","db/cardinality",-104975659),new cljs.core.Keyword("db.cardinality","many","db.cardinality/many",772806234)], null),cljs.core.PersistentArrayMap.EMPTY,new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword("db","index","db/index",-1531680669),true], null),cljs.core.PersistentArrayMap.EMPTY,new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword("db","index","db/index",-1531680669),true], null),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword("db","valueType","db/valueType",1827971944),new cljs.core.Keyword("db.type","ref","db.type/ref",-1728373079),new cljs.core.Keyword("db","cardinality","db/cardinality",-104975659),new cljs.core.Keyword("db.cardinality","many","db.cardinality/many",772806234)], null),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword("db","index","db/index",-1531680669),true], null),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword("db","unique","db/unique",329396388),new cljs.core.Keyword("db.unique","identity","db.unique/identity",1675950722)], null),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword("db","valueType","db/valueType",1827971944),new cljs.core.Keyword("db.type","ref","db.type/ref",-1728373079),new cljs.core.Keyword("db","index","db/index",-1531680669),true], null),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword("db","index","db/index",-1531680669),true], null),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword("db","valueType","db/valueType",1827971944),new cljs.core.Keyword("db.type","ref","db.type/ref",-1728373079),new cljs.core.Keyword("db","index","db/index",-1531680669),true], null),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword("db","index","db/index",-1531680669),true], null),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword("db","unique","db/unique",329396388),new cljs.core.Keyword("db.unique","identity","db.unique/identity",1675950722)], null)]);
/**
 * Retract attributes for DB graphs
 */
logseq.db.frontend.schema.retract_attributes = new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword("block","warning","block/warning",2131709542),null], null), null);
logseq.db.frontend.schema.ref_type_attributes = cljs.core.into.cljs$core$IFn$_invoke$arity$3(cljs.core.PersistentHashSet.EMPTY,cljs.core.keep.cljs$core$IFn$_invoke$arity$1((function (p__50498){
var vec__50499 = p__50498;
var attr_name = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50499,(0),null);
var attr_body_map = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50499,(1),null);
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db.type","ref","db.type/ref",-1728373079),new cljs.core.Keyword("db","valueType","db/valueType",1827971944).cljs$core$IFn$_invoke$arity$1(attr_body_map))){
return attr_name;
} else {
return null;
}
})),logseq.db.frontend.schema.schema);
logseq.db.frontend.schema.card_many_attributes = cljs.core.into.cljs$core$IFn$_invoke$arity$3(cljs.core.PersistentHashSet.EMPTY,cljs.core.keep.cljs$core$IFn$_invoke$arity$1((function (p__50503){
var vec__50504 = p__50503;
var attr_name = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50504,(0),null);
var attr_body_map = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50504,(1),null);
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("db.cardinality","many","db.cardinality/many",772806234),new cljs.core.Keyword("db","cardinality","db/cardinality",-104975659).cljs$core$IFn$_invoke$arity$1(attr_body_map))){
return attr_name;
} else {
return null;
}
})),logseq.db.frontend.schema.schema);
logseq.db.frontend.schema.card_many_ref_type_attributes = clojure.set.intersection.cljs$core$IFn$_invoke$arity$2(logseq.db.frontend.schema.card_many_attributes,logseq.db.frontend.schema.ref_type_attributes);
logseq.db.frontend.schema.card_one_ref_type_attributes = clojure.set.difference.cljs$core$IFn$_invoke$arity$2(logseq.db.frontend.schema.ref_type_attributes,logseq.db.frontend.schema.card_many_attributes);
logseq.db.frontend.schema.db_non_ref_attributes = cljs.core.set(cljs.core.keep.cljs$core$IFn$_invoke$arity$2((function (p__50507){
var vec__50508 = p__50507;
var k = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50508,(0),null);
var v = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__50508,(1),null);
if(cljs.core.not(new cljs.core.Keyword("db","valueType","db/valueType",1827971944).cljs$core$IFn$_invoke$arity$1(v))){
return k;
} else {
return null;
}
}),logseq.db.frontend.schema.schema));

//# sourceMappingURL=logseq.db.frontend.schema.js.map
