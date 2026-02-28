import "./cljs_env.js";
import "./cljs.core.js";
import "./lambdaisland.glogi.js";
import "./lambdaisland.glogi.console.js";
goog.provide('logseq.db_sync.logging');
/**
 * Installs console logging and sets the root logger level to `:info`.
 */
logseq.db_sync.logging.install_BANG_ = (function logseq$db_sync$logging$install_BANG_(){
lambdaisland.glogi.console.install_BANG_();

return lambdaisland.glogi.set_level(new cljs.core.Keyword("glogi","root","glogi/root",-77414841),new cljs.core.Keyword(null,"info","info",-317069002));
});

//# sourceMappingURL=logseq.db_sync.logging.js.map
