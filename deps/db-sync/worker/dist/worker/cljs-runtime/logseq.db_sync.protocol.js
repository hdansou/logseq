import "./cljs_env.js";
import "./cljs.core.js";
import "./logseq.db_sync.common.js";
goog.provide('logseq.db_sync.protocol');
logseq.db_sync.protocol.parse_message = (function logseq$db_sync$protocol$parse_message(raw){
try{var data = cljs.core.js__GT_clj.cljs$core$IFn$_invoke$arity$variadic(JSON.parse(raw),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"keywordize-keys","keywordize-keys",1310784252),true], 0));
if(cljs.core.map_QMARK_(data)){
return data;
} else {
return null;
}
}catch (e40578){var _ = e40578;
return null;
}});
logseq.db_sync.protocol.encode_message = (function logseq$db_sync$protocol$encode_message(m){
return JSON.stringify(cljs.core.clj__GT_js(m));
});
logseq.db_sync.protocol.transit__GT_tx = (function logseq$db_sync$protocol$transit__GT_tx(value){
return logseq.db_sync.common.read_transit(value);
});
logseq.db_sync.protocol.tx__GT_transit = (function logseq$db_sync$protocol$tx__GT_transit(tx_data){
return logseq.db_sync.common.write_transit(tx_data);
});
logseq.db_sync.protocol.datoms__GT_wire = (function logseq$db_sync$protocol$datoms__GT_wire(datoms){
return cljs.core.vec(cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (d){
return new cljs.core.PersistentArrayMap(null, 5, [new cljs.core.Keyword(null,"e","e",1381269198),new cljs.core.Keyword(null,"e","e",1381269198).cljs$core$IFn$_invoke$arity$1(d),new cljs.core.Keyword(null,"a","a",-2123407586),new cljs.core.Keyword(null,"a","a",-2123407586).cljs$core$IFn$_invoke$arity$1(d),new cljs.core.Keyword(null,"v","v",21465059),new cljs.core.Keyword(null,"v","v",21465059).cljs$core$IFn$_invoke$arity$1(d),new cljs.core.Keyword(null,"tx","tx",466630418),new cljs.core.Keyword(null,"tx","tx",466630418).cljs$core$IFn$_invoke$arity$1(d),new cljs.core.Keyword(null,"added","added",2057651688),new cljs.core.Keyword(null,"added","added",2057651688).cljs$core$IFn$_invoke$arity$1(d)], null);
}),datoms));
});

//# sourceMappingURL=logseq.db_sync.protocol.js.map
