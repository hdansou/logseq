import "./cljs_env.js";
import "./cljs.core.js";
import "./clojure.string.js";
import "./lambdaisland.glogi.js";
import "./logseq.db.js";
import "./logseq.db_sync.batch.js";
import "./logseq.db_sync.common.js";
import "./logseq.db_sync.protocol.js";
import "./logseq.db_sync.snapshot.js";
import "./logseq.db_sync.storage.js";
import "./logseq.db_sync.worker.http.js";
import "./logseq.db_sync.worker.routes.sync.js";
import "./logseq.db_sync.worker.ws.js";
import "./promesa.core.js";
goog.provide('logseq.db_sync.worker.handler.sync');
logseq.db_sync.worker.handler.sync.snapshot_download_batch_size = (500);
logseq.db_sync.worker.handler.sync.snapshot_cache_control = "private, max-age=300";
logseq.db_sync.worker.handler.sync.snapshot_content_type = "application/transit+json";
logseq.db_sync.worker.handler.sync.snapshot_content_encoding = "gzip";
logseq.db_sync.worker.handler.sync.snapshot_multipart_part_size = (((10) * (1024)) * (1024));
logseq.db_sync.worker.handler.sync.parse_int = (function logseq$db_sync$worker$handler$sync$parse_int(value){
if((!((value == null)))){
var n = parseInt(value,(10));
if(cljs.core.truth_(isNaN(n))){
return null;
} else {
return n;
}
} else {
return null;
}
});
logseq.db_sync.worker.handler.sync.ensure_schema_BANG_ = (function logseq$db_sync$worker$handler$sync$ensure_schema_BANG_(self){
if(self.schema_ready === true){
return null;
} else {
logseq.db_sync.storage.init_schema_BANG_(self.sql);

return (self.schema_ready = true);
}
});
logseq.db_sync.worker.handler.sync.ensure_conn_BANG_ = (function logseq$db_sync$worker$handler$sync$ensure_conn_BANG_(self){
logseq.db_sync.worker.handler.sync.ensure_schema_BANG_(self);

if(cljs.core.truth_(self.conn)){
return null;
} else {
return (self.conn = logseq.db_sync.storage.open_conn(self.sql));
}
});
logseq.db_sync.worker.handler.sync.t_now = (function logseq$db_sync$worker$handler$sync$t_now(self){
logseq.db_sync.worker.handler.sync.ensure_schema_BANG_(self);

return logseq.db_sync.storage.get_t(self.sql);
});
logseq.db_sync.worker.handler.sync.fetch_kvs_rows = (function logseq$db_sync$worker$handler$sync$fetch_kvs_rows(sql,after,limit){
return logseq.db_sync.common.get_sql_rows(logseq.db_sync.common.sql_exec.cljs$core$IFn$_invoke$arity$variadic(sql,"select addr, content, addresses from kvs where addr > ? order by addr asc limit ?",cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([after,limit], 0)));
});
logseq.db_sync.worker.handler.sync.snapshot_row__GT_tuple = (function logseq$db_sync$worker$handler$sync$snapshot_row__GT_tuple(row){
if(cljs.core.truth_(cljs.core.array_QMARK_(row))){
return new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [(row[(0)]),(row[(1)]),(row[(2)])], null);
} else {
return new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [(row["addr"]),(row["content"]),(row["addresses"])], null);
}
});
logseq.db_sync.worker.handler.sync.import_snapshot_rows_BANG_ = (function logseq$db_sync$worker$handler$sync$import_snapshot_rows_BANG_(sql,table,rows){
if(cljs.core.seq(rows)){
var seq__41149 = cljs.core.seq(logseq.db_sync.batch.rows__GT_insert_batches(table,rows,null));
var chunk__41150 = null;
var count__41151 = (0);
var i__41152 = (0);
while(true){
if((i__41152 < count__41151)){
var batch = chunk__41150.cljs$core$IIndexed$_nth$arity$2(null,i__41152);
var sql_str_41186 = new cljs.core.Keyword(null,"sql","sql",1251448786).cljs$core$IFn$_invoke$arity$1(batch);
var args_41187 = new cljs.core.Keyword(null,"args","args",1315556576).cljs$core$IFn$_invoke$arity$1(batch);
cljs.core.apply.cljs$core$IFn$_invoke$arity$4(logseq.db_sync.common.sql_exec,sql,sql_str_41186,args_41187);


var G__41188 = seq__41149;
var G__41189 = chunk__41150;
var G__41190 = count__41151;
var G__41191 = (i__41152 + (1));
seq__41149 = G__41188;
chunk__41150 = G__41189;
count__41151 = G__41190;
i__41152 = G__41191;
continue;
} else {
var temp__5804__auto__ = cljs.core.seq(seq__41149);
if(temp__5804__auto__){
var seq__41149__$1 = temp__5804__auto__;
if(cljs.core.chunked_seq_QMARK_(seq__41149__$1)){
var c__5673__auto__ = cljs.core.chunk_first(seq__41149__$1);
var G__41192 = cljs.core.chunk_rest(seq__41149__$1);
var G__41193 = c__5673__auto__;
var G__41194 = cljs.core.count(c__5673__auto__);
var G__41195 = (0);
seq__41149 = G__41192;
chunk__41150 = G__41193;
count__41151 = G__41194;
i__41152 = G__41195;
continue;
} else {
var batch = cljs.core.first(seq__41149__$1);
var sql_str_41196 = new cljs.core.Keyword(null,"sql","sql",1251448786).cljs$core$IFn$_invoke$arity$1(batch);
var args_41197 = new cljs.core.Keyword(null,"args","args",1315556576).cljs$core$IFn$_invoke$arity$1(batch);
cljs.core.apply.cljs$core$IFn$_invoke$arity$4(logseq.db_sync.common.sql_exec,sql,sql_str_41196,args_41197);


var G__41198 = cljs.core.next(seq__41149__$1);
var G__41199 = null;
var G__41200 = (0);
var G__41201 = (0);
seq__41149 = G__41198;
chunk__41150 = G__41199;
count__41151 = G__41200;
i__41152 = G__41201;
continue;
}
} else {
return null;
}
}
break;
}
} else {
return null;
}
});
logseq.db_sync.worker.handler.sync.reset_import_BANG_ = (function logseq$db_sync$worker$handler$sync$reset_import_BANG_(sql){
logseq.db_sync.common.sql_exec(sql,"delete from kvs");

logseq.db_sync.common.sql_exec(sql,"delete from tx_log");

logseq.db_sync.common.sql_exec(sql,"delete from sync_meta");

return logseq.db_sync.storage.set_t_BANG_(sql,(0));
});
logseq.db_sync.worker.handler.sync.graph_id_from_request = (function logseq$db_sync$worker$handler$sync$graph_id_from_request(request){
var header_id = request.headers.get("x-graph-id");
var url = (new URL(request.url));
var param_id = url.searchParams.get("graph-id");
if(cljs.core.seq((function (){var or__5142__auto__ = header_id;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return param_id;
}
})())){
var or__5142__auto__ = header_id;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return param_id;
}
} else {
return null;
}
});
logseq.db_sync.worker.handler.sync.snapshot_key = (function logseq$db_sync$worker$handler$sync$snapshot_key(graph_id,snapshot_id){
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(graph_id)+"/"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(snapshot_id)+".snapshot");
});
logseq.db_sync.worker.handler.sync.snapshot_url = (function logseq$db_sync$worker$handler$sync$snapshot_url(request,graph_id,snapshot_id){
var url = (new URL(request.url));
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(url.origin)+"/assets/"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(graph_id)+"/"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(snapshot_id)+".snapshot");
});
logseq.db_sync.worker.handler.sync.maybe_decompress_stream = (function logseq$db_sync$worker$handler$sync$maybe_decompress_stream(stream,encoding){
if(((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(encoding,logseq.db_sync.worker.handler.sync.snapshot_content_encoding)) && ((typeof DecompressionStream !== 'undefined')))){
return stream.pipeThrough((new DecompressionStream("gzip")));
} else {
return stream;
}
});
logseq.db_sync.worker.handler.sync.__GT_uint8 = (function logseq$db_sync$worker$handler$sync$__GT_uint8(data){
if((data instanceof Uint8Array)){
return data;
} else {
if((data instanceof ArrayBuffer)){
return (new Uint8Array(data));
} else {
return (new Uint8Array(data));

}
}
});
logseq.db_sync.worker.handler.sync.concat_uint8 = (function logseq$db_sync$worker$handler$sync$concat_uint8(a,b){
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
logseq.db_sync.worker.handler.sync.snapshot_export_stream = (function logseq$db_sync$worker$handler$sync$snapshot_export_stream(self){
var sql = self.sql;
var state = cljs.core.volatile_BANG_(new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"after","after",594996914),(-1),new cljs.core.Keyword(null,"done?","done?",-1847001718),false], null));
return (new ReadableStream(({"pull": (function (controller){
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(cljs.core.deref(state)),(function (p__41154){
var map__41155 = p__41154;
var map__41155__$1 = cljs.core.__destructure_map(map__41155);
var after = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__41155__$1,new cljs.core.Keyword(null,"after","after",594996914));
var done_QMARK_ = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__41155__$1,new cljs.core.Keyword(null,"done?","done?",-1847001718));
return promesa.protocols._promise((cljs.core.truth_(done_QMARK_)?controller.close():(function (){var rows = logseq.db_sync.worker.handler.sync.fetch_kvs_rows(sql,after,logseq.db_sync.worker.handler.sync.snapshot_download_batch_size);
var rows__$1 = cljs.core.mapv.cljs$core$IFn$_invoke$arity$2(logseq.db_sync.worker.handler.sync.snapshot_row__GT_tuple,rows);
var last_addr = ((cljs.core.seq(rows__$1))?cljs.core.apply.cljs$core$IFn$_invoke$arity$2(cljs.core.max,cljs.core.map.cljs$core$IFn$_invoke$arity$2(cljs.core.first,rows__$1)):after);
var done_QMARK___$1 = (cljs.core.count(rows__$1) < logseq.db_sync.worker.handler.sync.snapshot_download_batch_size);
if(cljs.core.seq(rows__$1)){
var payload_41202 = logseq.db_sync.snapshot.encode_rows(rows__$1);
var framed_41203 = logseq.db_sync.snapshot.frame_bytes(payload_41202);
controller.enqueue(framed_41203);
} else {
}

return state.cljs$core$IVolatile$_vreset_BANG_$arity$2(null,cljs.core.assoc.cljs$core$IFn$_invoke$arity$variadic(state.cljs$core$IDeref$_deref$arity$1(null),new cljs.core.Keyword(null,"after","after",594996914),last_addr,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"done?","done?",-1847001718),done_QMARK___$1], 0)));
})()));
}));
}));
})})));
});
logseq.db_sync.worker.handler.sync.upload_multipart_BANG_ = (function logseq$db_sync$worker$handler$sync$upload_multipart_BANG_(bucket,key,stream,opts){
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(bucket.createMultipartUpload(key,opts)),(function (upload){
return promesa.protocols._promise((function (){var reader = stream.getReader();
return promesa.core.catch$.cljs$core$IFn$_invoke$arity$2(promesa.core.create.cljs$core$IFn$_invoke$arity$1((function (resolve_fn_41160,reject_fn_41159){
var loop_fn_41156 = (function logseq$db_sync$worker$handler$sync$upload_multipart_BANG__$_loop_fn_41156(buffer,part_number,parts){
return promesa.core.fnly.cljs$core$IFn$_invoke$arity$2((function (res_41157,err_41158){
if((!((err_41158 == null)))){
return (reject_fn_41159.cljs$core$IFn$_invoke$arity$1 ? reject_fn_41159.cljs$core$IFn$_invoke$arity$1(err_41158) : reject_fn_41159.call(null,err_41158));
} else {
if(promesa.core.recur_QMARK_(res_41157)){
promesa.exec.run_BANG_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"vthread","vthread",441141075),promesa.exec.wrap_bindings((function (){
return cljs.core.apply.cljs$core$IFn$_invoke$arity$2(logseq$db_sync$worker$handler$sync$upload_multipart_BANG__$_loop_fn_41156,new cljs.core.Keyword(null,"bindings","bindings",1271397192).cljs$core$IFn$_invoke$arity$1(res_41157));
})));

return null;
} else {
return (resolve_fn_41160.cljs$core$IFn$_invoke$arity$1 ? resolve_fn_41160.cljs$core$IFn$_invoke$arity$1(res_41157) : resolve_fn_41160.call(null,res_41157));
}
}
}),promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$1){
return promesa.protocols._mcat(promesa.protocols._promise(buffer),(function (buffer__$1){
return promesa.protocols._mcat(promesa.protocols._promise(part_number),(function (part_number__$1){
return promesa.protocols._mcat(promesa.protocols._promise(parts),(function (parts__$1){
return promesa.protocols._promise(promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$2){
return promesa.protocols._mcat(promesa.protocols._promise(reader.read()),(function (chunk){
return promesa.protocols._promise((cljs.core.truth_(chunk.done)?(cljs.core.truth_((function (){var and__5140__auto__ = buffer__$1;
if(cljs.core.truth_(and__5140__auto__)){
return (buffer__$1.byteLength > (0));
} else {
return and__5140__auto__;
}
})())?promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$3){
return promesa.protocols._mcat(promesa.protocols._promise(upload.uploadPart(part_number__$1,buffer__$1)),(function (resp){
return promesa.protocols._mcat(promesa.protocols._promise(cljs.core.conj.cljs$core$IFn$_invoke$arity$2(parts__$1,new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"partNumber","partNumber",-1649894165),part_number__$1,new cljs.core.Keyword(null,"etag","etag",-329255476),resp.etag], null))),(function (parts__$2){
return promesa.protocols._promise(promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$4){
return promesa.protocols._mcat(promesa.protocols._promise(upload.complete(cljs.core.clj__GT_js(parts__$2))),(function (_){
return promesa.protocols._promise(new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"ok","ok",967785236),true], null));
}));
})));
}));
}));
})):((cljs.core.seq(parts__$1))?promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$3){
return promesa.protocols._mcat(promesa.protocols._promise(upload.complete(cljs.core.clj__GT_js(parts__$1))),(function (_){
return promesa.protocols._promise(new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"ok","ok",967785236),true], null));
}));
})):promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$3){
return promesa.protocols._mcat(promesa.protocols._promise(upload.abort()),(function (_){
return promesa.protocols._promise(bucket.put(key,(new Uint8Array((0))),opts));
}));
}))
)):(function (){var value = chunk.value;
var buffer__$2 = logseq.db_sync.worker.handler.sync.concat_uint8(buffer__$1,logseq.db_sync.worker.handler.sync.__GT_uint8(value));
if((buffer__$2.byteLength >= logseq.db_sync.worker.handler.sync.snapshot_multipart_part_size)){
var part = buffer__$2.slice((0),logseq.db_sync.worker.handler.sync.snapshot_multipart_part_size);
var rest_parts = buffer__$2.slice(logseq.db_sync.worker.handler.sync.snapshot_multipart_part_size,buffer__$2.byteLength);
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$3){
return promesa.protocols._mcat(promesa.protocols._promise(upload.uploadPart(part_number__$1,part)),(function (resp){
return promesa.protocols._mcat(promesa.protocols._promise(cljs.core.conj.cljs$core$IFn$_invoke$arity$2(parts__$1,new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"partNumber","partNumber",-1649894165),part_number__$1,new cljs.core.Keyword(null,"etag","etag",-329255476),resp.etag], null))),(function (parts__$2){
return promesa.protocols._promise(promesa.core.__GT_Recur(new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [rest_parts,(part_number__$1 + (1)),parts__$2], null)));
}));
}));
}));
} else {
return promesa.core.__GT_Recur(new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [buffer__$2,part_number__$1,parts__$1], null));
}
})()));
}));
})));
}));
}));
}));
})));
});
return promesa.exec.run_BANG_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"vthread","vthread",441141075),promesa.exec.wrap_bindings((function (){
return loop_fn_41156(null,(1),cljs.core.PersistentVector.EMPTY);
})));
})),(function (error){
upload.abort();

throw error;
}));
})());
}));
}));
});
logseq.db_sync.worker.handler.sync.snapshot_export_length = (function logseq$db_sync$worker$handler$sync$snapshot_export_length(self){
var sql = self.sql;
return promesa.core.create.cljs$core$IFn$_invoke$arity$1((function (resolve_fn_41165,reject_fn_41164){
var loop_fn_41161 = (function logseq$db_sync$worker$handler$sync$snapshot_export_length_$_loop_fn_41161(after,total){
return promesa.core.fnly.cljs$core$IFn$_invoke$arity$2((function (res_41162,err_41163){
if((!((err_41163 == null)))){
return (reject_fn_41164.cljs$core$IFn$_invoke$arity$1 ? reject_fn_41164.cljs$core$IFn$_invoke$arity$1(err_41163) : reject_fn_41164.call(null,err_41163));
} else {
if(promesa.core.recur_QMARK_(res_41162)){
promesa.exec.run_BANG_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"vthread","vthread",441141075),promesa.exec.wrap_bindings((function (){
return cljs.core.apply.cljs$core$IFn$_invoke$arity$2(logseq$db_sync$worker$handler$sync$snapshot_export_length_$_loop_fn_41161,new cljs.core.Keyword(null,"bindings","bindings",1271397192).cljs$core$IFn$_invoke$arity$1(res_41162));
})));

return null;
} else {
return (resolve_fn_41165.cljs$core$IFn$_invoke$arity$1 ? resolve_fn_41165.cljs$core$IFn$_invoke$arity$1(res_41162) : resolve_fn_41165.call(null,res_41162));
}
}
}),promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(after),(function (after__$1){
return promesa.protocols._mcat(promesa.protocols._promise(total),(function (total__$1){
return promesa.protocols._promise((function (){var rows = logseq.db_sync.worker.handler.sync.fetch_kvs_rows(sql,after__$1,logseq.db_sync.worker.handler.sync.snapshot_download_batch_size);
if(cljs.core.empty_QMARK_(rows)){
return total__$1;
} else {
var rows__$1 = cljs.core.mapv.cljs$core$IFn$_invoke$arity$2(logseq.db_sync.worker.handler.sync.snapshot_row__GT_tuple,rows);
var payload = logseq.db_sync.snapshot.encode_rows(rows__$1);
var total__$2 = ((total__$1 + (4)) + payload.byteLength);
var last_addr = cljs.core.apply.cljs$core$IFn$_invoke$arity$2(cljs.core.max,cljs.core.map.cljs$core$IFn$_invoke$arity$2(cljs.core.first,rows__$1));
var done_QMARK_ = (cljs.core.count(rows__$1) < logseq.db_sync.worker.handler.sync.snapshot_download_batch_size);
if(done_QMARK_){
return total__$2;
} else {
return promesa.core.__GT_Recur(new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [last_addr,total__$2], null));
}
}
})());
}));
}));
})));
});
return promesa.exec.run_BANG_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"vthread","vthread",441141075),promesa.exec.wrap_bindings((function (){
return loop_fn_41161((-1),(0));
})));
}));
});
logseq.db_sync.worker.handler.sync.snapshot_export_fixed_length = (function logseq$db_sync$worker$handler$sync$snapshot_export_fixed_length(self){
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.worker.handler.sync.snapshot_export_length(self)),(function (length){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.worker.handler.sync.snapshot_export_stream(self)),(function (stream){
return promesa.protocols._promise((((typeof FixedLengthStream !== 'undefined'))?(function (){var fixed = (new FixedLengthStream(length));
var readable = fixed.readable;
var writable = fixed.writable;
var reader = stream.getReader();
var writer = writable.getWriter();
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$1){
return promesa.protocols._mcat(promesa.protocols._promise(promesa.core.create.cljs$core$IFn$_invoke$arity$1((function (resolve_fn_41170,reject_fn_41169){
var loop_fn_41166 = (function logseq$db_sync$worker$handler$sync$snapshot_export_fixed_length_$_loop_fn_41166(){
return promesa.core.fnly.cljs$core$IFn$_invoke$arity$2((function (res_41167,err_41168){
if((!((err_41168 == null)))){
return (reject_fn_41169.cljs$core$IFn$_invoke$arity$1 ? reject_fn_41169.cljs$core$IFn$_invoke$arity$1(err_41168) : reject_fn_41169.call(null,err_41168));
} else {
if(promesa.core.recur_QMARK_(res_41167)){
promesa.exec.run_BANG_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"vthread","vthread",441141075),promesa.exec.wrap_bindings(logseq$db_sync$worker$handler$sync$snapshot_export_fixed_length_$_loop_fn_41166));

return null;
} else {
return (resolve_fn_41170.cljs$core$IFn$_invoke$arity$1 ? resolve_fn_41170.cljs$core$IFn$_invoke$arity$1(res_41167) : resolve_fn_41170.call(null,res_41167));
}
}
}),promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35464__auto__){
return promesa.protocols._promise(promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$2){
return promesa.protocols._mcat(promesa.protocols._promise(reader.read()),(function (chunk){
return promesa.protocols._promise((cljs.core.truth_(chunk.done)?writer.close():promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$3){
return promesa.protocols._mcat(promesa.protocols._promise(writer.write(chunk.value)),(function (_){
return promesa.protocols._promise(promesa.core.__GT_Recur(cljs.core.PersistentVector.EMPTY));
}));
}))));
}));
})));
})));
});
return promesa.exec.run_BANG_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"vthread","vthread",441141075),promesa.exec.wrap_bindings(loop_fn_41166));
}))),(function (_){
return promesa.protocols._promise(readable);
}));
}));
})():promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$1){
return promesa.protocols._mcat(promesa.protocols._promise((new Response(stream))),(function (resp){
return promesa.protocols._mcat(promesa.protocols._promise(resp.arrayBuffer()),(function (buf){
return promesa.protocols._promise(buf);
}));
}));
}))));
}));
}));
}));
});
logseq.db_sync.worker.handler.sync.import_snapshot_stream_BANG_ = (function logseq$db_sync$worker$handler$sync$import_snapshot_stream_BANG_(self,stream,reset_QMARK_){
var reader = stream.getReader();
var reset_pending_QMARK_ = cljs.core.volatile_BANG_(reset_QMARK_);
var total_count = cljs.core.volatile_BANG_((0));
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (buffer){
return promesa.protocols._promise(promesa.core.catch$.cljs$core$IFn$_invoke$arity$2(promesa.core.create.cljs$core$IFn$_invoke$arity$1((function (resolve_fn_41175,reject_fn_41174){
var loop_fn_41171 = (function logseq$db_sync$worker$handler$sync$import_snapshot_stream_BANG__$_loop_fn_41171(buffer__$1){
return promesa.core.fnly.cljs$core$IFn$_invoke$arity$2((function (res_41172,err_41173){
if((!((err_41173 == null)))){
return (reject_fn_41174.cljs$core$IFn$_invoke$arity$1 ? reject_fn_41174.cljs$core$IFn$_invoke$arity$1(err_41173) : reject_fn_41174.call(null,err_41173));
} else {
if(promesa.core.recur_QMARK_(res_41172)){
promesa.exec.run_BANG_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"vthread","vthread",441141075),promesa.exec.wrap_bindings((function (){
return cljs.core.apply.cljs$core$IFn$_invoke$arity$2(logseq$db_sync$worker$handler$sync$import_snapshot_stream_BANG__$_loop_fn_41171,new cljs.core.Keyword(null,"bindings","bindings",1271397192).cljs$core$IFn$_invoke$arity$1(res_41172));
})));

return null;
} else {
return (resolve_fn_41175.cljs$core$IFn$_invoke$arity$1 ? resolve_fn_41175.cljs$core$IFn$_invoke$arity$1(res_41172) : resolve_fn_41175.call(null,res_41172));
}
}
}),promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$1){
return promesa.protocols._mcat(promesa.protocols._promise(buffer__$1),(function (buffer__$2){
return promesa.protocols._promise(promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$2){
return promesa.protocols._mcat(promesa.protocols._promise(reader.read()),(function (chunk){
return promesa.protocols._promise((cljs.core.truth_(chunk.done)?(function (){var rows = logseq.db_sync.snapshot.finalize_framed_buffer(buffer__$2);
var rows_count = cljs.core.count(rows);
var reset_QMARK___$1 = (function (){var and__5140__auto__ = cljs.core.deref(reset_pending_QMARK_);
if(cljs.core.truth_(and__5140__auto__)){
return true;
} else {
return and__5140__auto__;
}
})();
if(cljs.core.truth_((function (){var or__5142__auto__ = reset_QMARK___$1;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return cljs.core.seq(rows);
}
})())){
(logseq.db_sync.worker.handler.sync.import_snapshot_BANG_.cljs$core$IFn$_invoke$arity$3 ? logseq.db_sync.worker.handler.sync.import_snapshot_BANG_.cljs$core$IFn$_invoke$arity$3(self,rows,reset_QMARK___$1) : logseq.db_sync.worker.handler.sync.import_snapshot_BANG_.call(null,self,rows,reset_QMARK___$1));

cljs.core.vreset_BANG_(reset_pending_QMARK_,false);
} else {
}

total_count.cljs$core$IVolatile$_vreset_BANG_$arity$2(null,(total_count.cljs$core$IDeref$_deref$arity$1(null) + rows_count));

return cljs.core.deref(total_count);
})():(function (){var value = chunk.value;
var map__41176 = logseq.db_sync.snapshot.parse_framed_chunk(buffer__$2,value);
var map__41176__$1 = cljs.core.__destructure_map(map__41176);
var rows = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__41176__$1,new cljs.core.Keyword(null,"rows","rows",850049680));
var buffer__$3 = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__41176__$1,new cljs.core.Keyword(null,"buffer","buffer",617295198));
var rows_count = cljs.core.count(rows);
var reset_QMARK___$1 = (function (){var and__5140__auto__ = cljs.core.deref(reset_pending_QMARK_);
if(cljs.core.truth_(and__5140__auto__)){
return cljs.core.seq(rows);
} else {
return and__5140__auto__;
}
})();
if(cljs.core.seq(rows)){
var G__41177_41204 = self;
var G__41178_41205 = rows;
var G__41179_41206 = reset_QMARK___$1 === true;
(logseq.db_sync.worker.handler.sync.import_snapshot_BANG_.cljs$core$IFn$_invoke$arity$3 ? logseq.db_sync.worker.handler.sync.import_snapshot_BANG_.cljs$core$IFn$_invoke$arity$3(G__41177_41204,G__41178_41205,G__41179_41206) : logseq.db_sync.worker.handler.sync.import_snapshot_BANG_.call(null,G__41177_41204,G__41178_41205,G__41179_41206));

cljs.core.vreset_BANG_(reset_pending_QMARK_,false);
} else {
}

total_count.cljs$core$IVolatile$_vreset_BANG_$arity$2(null,(total_count.cljs$core$IDeref$_deref$arity$1(null) + rows_count));

return promesa.core.__GT_Recur(new cljs.core.PersistentVector(null, 1, 5, cljs.core.PersistentVector.EMPTY_NODE, [buffer__$3], null));
})()));
}));
})));
}));
})));
});
return promesa.exec.run_BANG_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword(null,"vthread","vthread",441141075),promesa.exec.wrap_bindings((function (){
return loop_fn_41171(buffer);
})));
})),(function (error){
throw error;
})));
}));
}));
});
logseq.db_sync.worker.handler.sync.pull_response = (function logseq$db_sync$worker$handler$sync$pull_response(self,since){
var sql = self.sql;
var txs = logseq.db_sync.storage.fetch_tx_since(sql,since);
var response = new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"type","type",1174270348),"pull/ok",new cljs.core.Keyword(null,"t","t",-1397832519),logseq.db_sync.worker.handler.sync.t_now(self),new cljs.core.Keyword(null,"txs","txs",2056038378),txs], null);
return response;
});
logseq.db_sync.worker.handler.sync.import_snapshot_BANG_ = (function logseq$db_sync$worker$handler$sync$import_snapshot_BANG_(self,rows,reset_QMARK_){
var sql = self.sql;
logseq.db_sync.worker.handler.sync.ensure_schema_BANG_(self);

if(cljs.core.truth_(reset_QMARK_)){
logseq.db_sync.worker.handler.sync.reset_import_BANG_(sql);
} else {
}

return logseq.db_sync.worker.handler.sync.import_snapshot_rows_BANG_(sql,"kvs",rows);
});
logseq.db_sync.worker.handler.sync.apply_tx_BANG_ = (function logseq$db_sync$worker$handler$sync$apply_tx_BANG_(self,sender,txs){
var sql = self.sql;
logseq.db_sync.worker.handler.sync.ensure_conn_BANG_(self);

var conn = self.conn;
var tx_data = logseq.db_sync.protocol.transit__GT_tx(txs);
logseq.db.transact_BANG_.cljs$core$IFn$_invoke$arity$3(conn,tx_data,new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"op","op",-1882987955),new cljs.core.Keyword(null,"apply-client-tx","apply-client-tx",-347898842)], null));

var new_t = logseq.db_sync.storage.get_t(sql);
logseq.db_sync.worker.ws.broadcast_BANG_(self,sender,new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"type","type",1174270348),"changed",new cljs.core.Keyword(null,"t","t",-1397832519),new_t], null));

return new_t;
});
logseq.db_sync.worker.handler.sync.handle_tx_batch_BANG_ = (function logseq$db_sync$worker$handler$sync$handle_tx_batch_BANG_(self,sender,txs,t_before){
var current_t = logseq.db_sync.worker.handler.sync.t_now(self);
if((((!(typeof t_before === 'number'))) || ((t_before < (0))))){
return new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"type","type",1174270348),"tx/reject",new cljs.core.Keyword(null,"reason","reason",-2070751759),"invalid t-before"], null);
} else {
if(cljs.core.not_EQ_.cljs$core$IFn$_invoke$arity$2(t_before,current_t)){
return new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"type","type",1174270348),"tx/reject",new cljs.core.Keyword(null,"reason","reason",-2070751759),"stale",new cljs.core.Keyword(null,"t","t",-1397832519),current_t], null);
} else {
if(cljs.core.truth_(txs)){
try{var new_t = logseq.db_sync.worker.handler.sync.apply_tx_BANG_(self,sender,txs);
if(((cljs.core.map_QMARK_(new_t)) && (cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2("tx/reject",new cljs.core.Keyword(null,"type","type",1174270348).cljs$core$IFn$_invoke$arity$1(new_t))))){
return new_t;
} else {
return new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"type","type",1174270348),"tx/batch/ok",new cljs.core.Keyword(null,"t","t",-1397832519),new_t], null);
}
}catch (e41180){var e = e41180;
if(goog.debug.LOGGING_ENABLED){
lambdaisland.glogi.log.cljs$core$IFn$_invoke$arity$4("logseq.db-sync.worker.handler.sync",new cljs.core.Keyword(null,"error","error",-978969032),cljs.core.identity(new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword("db-sync","transact-failed","db-sync/transact-failed",-1656014684),e,new cljs.core.Keyword(null,"line","line",212345235),274], null)),null);
} else {
}

return new cljs.core.PersistentArrayMap(null, 3, [new cljs.core.Keyword(null,"type","type",1174270348),"tx/reject",new cljs.core.Keyword(null,"reason","reason",-2070751759),"db transact failed",new cljs.core.Keyword(null,"t","t",-1397832519),current_t], null);
}} else {
return new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"type","type",1174270348),"tx/reject",new cljs.core.Keyword(null,"reason","reason",-2070751759),"empty tx data"], null);
}

}
}
});
logseq.db_sync.worker.handler.sync.handle = (function logseq$db_sync$worker$handler$sync$handle(p__41181){
var map__41182 = p__41181;
var map__41182__$1 = cljs.core.__destructure_map(map__41182);
var self = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__41182__$1,new cljs.core.Keyword(null,"self","self",-1547428899));
var request = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__41182__$1,new cljs.core.Keyword(null,"request","request",1772954723));
var url = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__41182__$1,new cljs.core.Keyword(null,"url","url",276297046));
var route = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__41182__$1,new cljs.core.Keyword(null,"route","route",329891309));
var G__41183 = new cljs.core.Keyword(null,"handler","handler",-195596612).cljs$core$IFn$_invoke$arity$1(route);
var G__41183__$1 = (((G__41183 instanceof cljs.core.Keyword))?G__41183.fqn:null);
switch (G__41183__$1) {
case "sync/health":
return logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("sync","health","sync/health",-290892908),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"ok","ok",967785236),true], null));

break;
case "sync/pull":
var raw_since = url.searchParams.get("since");
var since = (((!((raw_since == null))))?logseq.db_sync.worker.handler.sync.parse_int(raw_since):(0));
if((((((!((raw_since == null)))) && ((!(typeof since === 'number'))))) || ((since < (0))))){
return logseq.db_sync.worker.http.bad_request("invalid since");
} else {
return logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("sync","pull","sync/pull",-865389048),logseq.db_sync.worker.handler.sync.pull_response(self,since));
}

break;
case "sync/snapshot-download":
var graph_id = logseq.db_sync.worker.handler.sync.graph_id_from_request(request);
var bucket = self.env.LOGSEQ_SYNC_ASSETS;
if(cljs.core.not(cljs.core.seq(graph_id))){
return logseq.db_sync.worker.http.bad_request("missing graph id");
} else {
if((bucket == null)){
return logseq.db_sync.worker.http.error_response("missing assets bucket",(500));
} else {
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise((""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.random_uuid()))),(function (snapshot_id){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.worker.handler.sync.snapshot_key(graph_id,snapshot_id)),(function (key){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.worker.handler.sync.snapshot_export_stream(self)),(function (stream){
return promesa.protocols._mcat(promesa.protocols._promise((((!((bucket.createMultipartUpload == null)))) && (cljs.core.fn_QMARK_(bucket.createMultipartUpload)))),(function (multipart_QMARK_){
return promesa.protocols._mcat(promesa.protocols._promise(({"httpMetadata": ({"contentType": logseq.db_sync.worker.handler.sync.snapshot_content_type, "contentEncoding": null, "cacheControl": logseq.db_sync.worker.handler.sync.snapshot_cache_control}), "customMetadata": ({"purpose": "snapshot", "created-at": (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(logseq.db_sync.common.now_ms()))})})),(function (opts){
return promesa.protocols._mcat(promesa.protocols._promise((cljs.core.truth_(multipart_QMARK_)?logseq.db_sync.worker.handler.sync.upload_multipart_BANG_(bucket,key,stream,opts):promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto____$1){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.worker.handler.sync.snapshot_export_fixed_length(self)),(function (body){
return promesa.protocols._promise(bucket.put(key,body,opts));
}));
})))),(function (_){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.worker.handler.sync.snapshot_url(request,graph_id,snapshot_id)),(function (url__$1){
return promesa.protocols._promise(logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("sync","snapshot-download","sync/snapshot-download",-718483086),new cljs.core.PersistentArrayMap(null, 4, [new cljs.core.Keyword(null,"ok","ok",967785236),true,new cljs.core.Keyword(null,"key","key",-1516042587),key,new cljs.core.Keyword(null,"url","url",276297046),url__$1,new cljs.core.Keyword(null,"content-encoding","content-encoding",2011780217),null], null)));
}));
}));
}));
}));
}));
}));
}));
}));

}
}

break;
case "sync/admin-reset":
logseq.db_sync.common.sql_exec(self.sql,"drop table if exists kvs");

logseq.db_sync.common.sql_exec(self.sql,"drop table if exists tx_log");

logseq.db_sync.common.sql_exec(self.sql,"drop table if exists sync_meta");

logseq.db_sync.storage.init_schema_BANG_(self.sql);

(self.schema_ready = true);

(self.conn = null);

return logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("sync","admin-reset","sync/admin-reset",-1418203638),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"ok","ok",967785236),true], null));

break;
case "sync/tx-batch":
return logseq.db_sync.common.read_json(request).then((function (result){
if((result == null)){
return logseq.db_sync.worker.http.bad_request("missing body");
} else {
var body = cljs.core.js__GT_clj.cljs$core$IFn$_invoke$arity$variadic(result,cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"keywordize-keys","keywordize-keys",1310784252),true], 0));
var body__$1 = logseq.db_sync.worker.http.coerce_http_request(new cljs.core.Keyword("sync","tx-batch","sync/tx-batch",-321106770),body);
if((body__$1 == null)){
return logseq.db_sync.worker.http.bad_request("invalid tx");
} else {
var map__41184 = body__$1;
var map__41184__$1 = cljs.core.__destructure_map(map__41184);
var txs = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__41184__$1,new cljs.core.Keyword(null,"txs","txs",2056038378));
var t_before = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__41184__$1,new cljs.core.Keyword(null,"t-before","t-before",-507640180));
var t_before__$1 = logseq.db_sync.worker.handler.sync.parse_int(t_before);
if(typeof txs === 'string'){
return logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("sync","tx-batch","sync/tx-batch",-321106770),logseq.db_sync.worker.handler.sync.handle_tx_batch_BANG_(self,null,txs,t_before__$1));
} else {
return logseq.db_sync.worker.http.bad_request("invalid tx");
}
}
}
}));

break;
case "sync/snapshot-upload":
var graph_id = logseq.db_sync.worker.handler.sync.graph_id_from_request(request);
var reset_param = url.searchParams.get("reset");
var reset_QMARK_ = (((reset_param == null))?true:(!(cljs.core.contains_QMARK_(new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 2, ["false",null,"0",null], null), null),reset_param))));
var req_encoding = request.headers.get("content-encoding");
if(cljs.core.not(cljs.core.seq(graph_id))){
return logseq.db_sync.worker.http.bad_request("missing graph id");
} else {
if((request.body == null)){
return logseq.db_sync.worker.http.bad_request("missing body");
} else {
var stream = request.body;
var encoding = (function (){var or__5142__auto__ = req_encoding;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return "";
}
})();
if(((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(encoding,logseq.db_sync.worker.handler.sync.snapshot_content_encoding)) && ((!((typeof DecompressionStream !== 'undefined')))))){
return logseq.db_sync.worker.http.error_response("gzip not supported",(500));
} else {
return promesa.protocols._mcat(promesa.protocols._promise(null),(function (___35486__auto__){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.worker.handler.sync.maybe_decompress_stream(stream,encoding)),(function (stream__$1){
return promesa.protocols._mcat(promesa.protocols._promise(logseq.db_sync.worker.handler.sync.import_snapshot_stream_BANG_(self,stream__$1,reset_QMARK_)),(function (count){
return promesa.protocols._promise(logseq.db_sync.worker.http.json_response.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("sync","snapshot-upload","sync/snapshot-upload",2123375255),new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword(null,"ok","ok",967785236),true,new cljs.core.Keyword(null,"count","count",2139924085),count], null)));
}));
}));
}));
}

}
}

break;
default:
return logseq.db_sync.worker.http.not_found();

}
});
logseq.db_sync.worker.handler.sync.strip_sync_prefix = (function logseq$db_sync$worker$handler$sync$strip_sync_prefix(path){
if(clojure.string.starts_with_QMARK_(path,"/sync/")){
var rest_path = cljs.core.subs.cljs$core$IFn$_invoke$arity$2(path,(("/sync/").length));
var slash_idx = clojure.string.index_of.cljs$core$IFn$_invoke$arity$2(rest_path,"/");
if((slash_idx < (0))){
return "/";
} else {
return cljs.core.subs.cljs$core$IFn$_invoke$arity$2(rest_path,slash_idx);
}
} else {
return path;
}
});
logseq.db_sync.worker.handler.sync.handle_http = (function logseq$db_sync$worker$handler$sync$handle_http(self,request){
var with_cors_error = (function logseq$db_sync$worker$handler$sync$handle_http_$_with_cors_error(resp){
if((resp instanceof Promise)){
return resp.catch((function (e){
if(goog.debug.LOGGING_ENABLED){
lambdaisland.glogi.log.cljs$core$IFn$_invoke$arity$4("logseq.db-sync.worker.handler.sync",new cljs.core.Keyword(null,"error","error",-978969032),cljs.core.identity(new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword("db-sync","http-error","db-sync/http-error",1036490993),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"error","error",-978969032),e], null),new cljs.core.Keyword(null,"line","line",212345235),390], null)),null);
} else {
}

return logseq.db_sync.worker.http.error_response("server error",(500));
}));
} else {
return resp;
}
});
try{var url = (new URL(request.url));
var raw_path = url.pathname;
var path = logseq.db_sync.worker.handler.sync.strip_sync_prefix(raw_path);
var method = request.method;
return with_cors_error(((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(method,"OPTIONS"))?logseq.db_sync.common.options_response():(function (){var temp__5802__auto__ = logseq.db_sync.worker.routes.sync.match_route(method,path);
if(cljs.core.truth_(temp__5802__auto__)){
var route = temp__5802__auto__;
return logseq.db_sync.worker.handler.sync.handle(new cljs.core.PersistentArrayMap(null, 4, [new cljs.core.Keyword(null,"self","self",-1547428899),self,new cljs.core.Keyword(null,"request","request",1772954723),request,new cljs.core.Keyword(null,"url","url",276297046),url,new cljs.core.Keyword(null,"route","route",329891309),route], null));
} else {
return logseq.db_sync.worker.http.not_found();
}
})()
));
}catch (e41185){var e = e41185;
if(goog.debug.LOGGING_ENABLED){
lambdaisland.glogi.log.cljs$core$IFn$_invoke$arity$4("logseq.db-sync.worker.handler.sync",new cljs.core.Keyword(null,"error","error",-978969032),cljs.core.identity(new cljs.core.PersistentArrayMap(null, 2, [new cljs.core.Keyword("db-sync","http-error","db-sync/http-error",1036490993),new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"error","error",-978969032),e], null),new cljs.core.Keyword(null,"line","line",212345235),411], null)),null);
} else {
}

return logseq.db_sync.worker.http.error_response("server error",(500));
}});

//# sourceMappingURL=logseq.db_sync.worker.handler.sync.js.map
