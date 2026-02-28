import "./cljs_env.js";
import "./cljs.core.js";
import "./cognitect.transit.js";
goog.provide('logseq.db_sync.snapshot');
logseq.db_sync.snapshot.transit_w = cognitect.transit.writer.cljs$core$IFn$_invoke$arity$1(new cljs.core.Keyword(null,"json","json",1279968570));
logseq.db_sync.snapshot.transit_r = cognitect.transit.reader.cljs$core$IFn$_invoke$arity$1(new cljs.core.Keyword(null,"json","json",1279968570));
logseq.db_sync.snapshot.text_decoder = (new TextDecoder());
logseq.db_sync.snapshot.__GT_uint8 = (function logseq$db_sync$snapshot$__GT_uint8(data){
if((data instanceof Uint8Array)){
return data;
} else {
if((data instanceof ArrayBuffer)){
return (new Uint8Array(data));
} else {
if(typeof data === 'string'){
return (new TextEncoder()).encode(data);
} else {
return (new Uint8Array(data));

}
}
}
});
logseq.db_sync.snapshot.encode_rows = (function logseq$db_sync$snapshot$encode_rows(rows){
return logseq.db_sync.snapshot.__GT_uint8(cognitect.transit.write(logseq.db_sync.snapshot.transit_w,rows));
});
logseq.db_sync.snapshot.decode_rows = (function logseq$db_sync$snapshot$decode_rows(payload){
return cognitect.transit.read(logseq.db_sync.snapshot.transit_r,logseq.db_sync.snapshot.text_decoder.decode(logseq.db_sync.snapshot.__GT_uint8(payload)));
});
logseq.db_sync.snapshot.frame_bytes = (function logseq$db_sync$snapshot$frame_bytes(payload){
var len = payload.byteLength;
var out = (new Uint8Array(((4) + len)));
var view = (new DataView(out.buffer));
view.setUint32((0),len,false);

out.set(payload,(4));

return out;
});
logseq.db_sync.snapshot.concat_bytes = (function logseq$db_sync$snapshot$concat_bytes(a,b){
if((a == null)){
return b;
} else {
if((b == null)){
return a;
} else {
var out = (new Uint8Array((a.byteLength + b.byteLength)));
out.set(a,(0));

out.set(b,a.byteLength);

return out;

}
}
});
logseq.db_sync.snapshot.parse_framed_chunk = (function logseq$db_sync$snapshot$parse_framed_chunk(buffer,chunk){
var data = logseq.db_sync.snapshot.concat_bytes(buffer,chunk);
var total = data.byteLength;
var offset = (0);
var rows = cljs.core.PersistentVector.EMPTY;
while(true){
if(((total - offset) < (4))){
return new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"rows","rows",850049680),rows,new cljs.core.Keyword(null,"buffer","buffer",617295198),(((offset < total))?data.slice(offset,total):null)], null);
} else {
var view = (new DataView(data.buffer,offset,(4)));
var len = view.getUint32((0),false);
var next_offset = ((offset + (4)) + len);
if((next_offset <= total)){
var payload = data.slice((offset + (4)),next_offset);
var decoded = logseq.db_sync.snapshot.decode_rows(payload);
var G__50922 = next_offset;
var G__50923 = cljs.core.into.cljs$core$IFn$_invoke$arity$2(rows,decoded);
offset = G__50922;
rows = G__50923;
continue;
} else {
return new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"rows","rows",850049680),rows,new cljs.core.Keyword(null,"buffer","buffer",617295198),data.slice(offset,total)], null);
}
}
break;
}
});
logseq.db_sync.snapshot.finalize_framed_buffer = (function logseq$db_sync$snapshot$finalize_framed_buffer(buffer){
if((((buffer == null)) || ((buffer.byteLength === (0))))){
return cljs.core.PersistentVector.EMPTY;
} else {
var map__50897 = logseq.db_sync.snapshot.parse_framed_chunk(null,buffer);
var map__50897__$1 = cljs.core.__destructure_map(map__50897);
var rows = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__50897__$1,new cljs.core.Keyword(null,"rows","rows",850049680));
var buffer__$1 = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__50897__$1,new cljs.core.Keyword(null,"buffer","buffer",617295198));
if(((cljs.core.seq(rows)) && ((((buffer__$1 == null)) || ((buffer__$1.byteLength === (0))))))){
return rows;
} else {
throw cljs.core.ex_info.cljs$core$IFn$_invoke$arity$2("incomplete framed buffer",new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"buffer","buffer",617295198),buffer__$1,new cljs.core.Keyword(null,"rows","rows",850049680),rows], null));
}
}
});
logseq.db_sync.snapshot.framed_length = (function logseq$db_sync$snapshot$framed_length(rows_batches){
return cljs.core.reduce.cljs$core$IFn$_invoke$arity$3((function (total,rows){
var payload = logseq.db_sync.snapshot.encode_rows(rows);
return ((total + (4)) + payload.byteLength);
}),(0),rows_batches);
});

//# sourceMappingURL=logseq.db_sync.snapshot.js.map
