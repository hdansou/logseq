import "./cljs_env.js";
import "./cljs.core.js";
import "./reitit.core.js";
goog.provide('logseq.db_sync.worker.routes.sync');
logseq.db_sync.worker.routes.sync.route_data = new cljs.core.PersistentVector(null, 6, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["/health",new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"methods","methods",453930866),new cljs.core.PersistentArrayMap(null, 1, ["GET",new cljs.core.Keyword("sync","health","sync/health",-290892908)], null)], null)], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["/pull",new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"methods","methods",453930866),new cljs.core.PersistentArrayMap(null, 1, ["GET",new cljs.core.Keyword("sync","pull","sync/pull",-865389048)], null)], null)], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["/snapshot/download",new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"methods","methods",453930866),new cljs.core.PersistentArrayMap(null, 1, ["GET",new cljs.core.Keyword("sync","snapshot-download","sync/snapshot-download",-718483086)], null)], null)], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["/admin/reset",new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"methods","methods",453930866),new cljs.core.PersistentArrayMap(null, 1, ["DELETE",new cljs.core.Keyword("sync","admin-reset","sync/admin-reset",-1418203638)], null)], null)], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["/tx/batch",new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"methods","methods",453930866),new cljs.core.PersistentArrayMap(null, 1, ["POST",new cljs.core.Keyword("sync","tx-batch","sync/tx-batch",-321106770)], null)], null)], null),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["/snapshot/upload",new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword(null,"methods","methods",453930866),new cljs.core.PersistentArrayMap(null, 1, ["POST",new cljs.core.Keyword("sync","snapshot-upload","sync/snapshot-upload",2123375255)], null)], null)], null)], null);
logseq.db_sync.worker.routes.sync.router = reitit.core.router.cljs$core$IFn$_invoke$arity$1(logseq.db_sync.worker.routes.sync.route_data);
logseq.db_sync.worker.routes.sync.match_route = (function logseq$db_sync$worker$routes$sync$match_route(method,path){
var temp__5804__auto__ = reitit.core.match_by_path(logseq.db_sync.worker.routes.sync.router,path);
if(cljs.core.truth_(temp__5804__auto__)){
var match = temp__5804__auto__;
var temp__5804__auto____$1 = cljs.core.get_in.cljs$core$IFn$_invoke$arity$2(match,new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"data","data",-232669377),new cljs.core.Keyword(null,"methods","methods",453930866),method], null));
if(cljs.core.truth_(temp__5804__auto____$1)){
var handler = temp__5804__auto____$1;
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(match,new cljs.core.Keyword(null,"handler","handler",-195596612),handler);
} else {
return null;
}
} else {
return null;
}
});

//# sourceMappingURL=logseq.db_sync.worker.routes.sync.js.map
