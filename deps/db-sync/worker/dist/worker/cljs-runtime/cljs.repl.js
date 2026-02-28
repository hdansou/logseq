goog.provide('cljs.repl');
cljs.repl.print_doc = (function cljs$repl$print_doc(p__53899){
var map__53900 = p__53899;
var map__53900__$1 = cljs.core.__destructure_map(map__53900);
var m = map__53900__$1;
var n = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__53900__$1,new cljs.core.Keyword(null,"ns","ns",441598760));
var nm = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__53900__$1,new cljs.core.Keyword(null,"name","name",1843675177));
cljs.core.println.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2(["-------------------------"], 0));

cljs.core.println.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([(function (){var or__5142__auto__ = new cljs.core.Keyword(null,"spec","spec",347520401).cljs$core$IFn$_invoke$arity$1(m);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1((function (){var temp__5804__auto__ = new cljs.core.Keyword(null,"ns","ns",441598760).cljs$core$IFn$_invoke$arity$1(m);
if(cljs.core.truth_(temp__5804__auto__)){
var ns = temp__5804__auto__;
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(ns)+"/");
} else {
return null;
}
})())+cljs.core.str.cljs$core$IFn$_invoke$arity$1(new cljs.core.Keyword(null,"name","name",1843675177).cljs$core$IFn$_invoke$arity$1(m)));
}
})()], 0));

if(cljs.core.truth_(new cljs.core.Keyword(null,"protocol","protocol",652470118).cljs$core$IFn$_invoke$arity$1(m))){
cljs.core.println.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2(["Protocol"], 0));
} else {
}

if(cljs.core.truth_(new cljs.core.Keyword(null,"forms","forms",2045992350).cljs$core$IFn$_invoke$arity$1(m))){
var seq__53901_54101 = cljs.core.seq(new cljs.core.Keyword(null,"forms","forms",2045992350).cljs$core$IFn$_invoke$arity$1(m));
var chunk__53902_54102 = null;
var count__53903_54103 = (0);
var i__53904_54104 = (0);
while(true){
if((i__53904_54104 < count__53903_54103)){
var f_54105 = chunk__53902_54102.cljs$core$IIndexed$_nth$arity$2(null,i__53904_54104);
cljs.core.println.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2(["  ",f_54105], 0));


var G__54106 = seq__53901_54101;
var G__54107 = chunk__53902_54102;
var G__54108 = count__53903_54103;
var G__54109 = (i__53904_54104 + (1));
seq__53901_54101 = G__54106;
chunk__53902_54102 = G__54107;
count__53903_54103 = G__54108;
i__53904_54104 = G__54109;
continue;
} else {
var temp__5804__auto___54110 = cljs.core.seq(seq__53901_54101);
if(temp__5804__auto___54110){
var seq__53901_54111__$1 = temp__5804__auto___54110;
if(cljs.core.chunked_seq_QMARK_(seq__53901_54111__$1)){
var c__5673__auto___54112 = cljs.core.chunk_first(seq__53901_54111__$1);
var G__54113 = cljs.core.chunk_rest(seq__53901_54111__$1);
var G__54114 = c__5673__auto___54112;
var G__54115 = cljs.core.count(c__5673__auto___54112);
var G__54116 = (0);
seq__53901_54101 = G__54113;
chunk__53902_54102 = G__54114;
count__53903_54103 = G__54115;
i__53904_54104 = G__54116;
continue;
} else {
var f_54117 = cljs.core.first(seq__53901_54111__$1);
cljs.core.println.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2(["  ",f_54117], 0));


var G__54118 = cljs.core.next(seq__53901_54111__$1);
var G__54119 = null;
var G__54120 = (0);
var G__54121 = (0);
seq__53901_54101 = G__54118;
chunk__53902_54102 = G__54119;
count__53903_54103 = G__54120;
i__53904_54104 = G__54121;
continue;
}
} else {
}
}
break;
}
} else {
if(cljs.core.truth_(new cljs.core.Keyword(null,"arglists","arglists",1661989754).cljs$core$IFn$_invoke$arity$1(m))){
var arglists_54122 = new cljs.core.Keyword(null,"arglists","arglists",1661989754).cljs$core$IFn$_invoke$arity$1(m);
if(cljs.core.truth_((function (){var or__5142__auto__ = new cljs.core.Keyword(null,"macro","macro",-867863404).cljs$core$IFn$_invoke$arity$1(m);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return new cljs.core.Keyword(null,"repl-special-function","repl-special-function",1262603725).cljs$core$IFn$_invoke$arity$1(m);
}
})())){
cljs.core.prn.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([arglists_54122], 0));
} else {
cljs.core.prn.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([((cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Symbol(null,"quote","quote",1377916282,null),cljs.core.first(arglists_54122)))?cljs.core.second(arglists_54122):arglists_54122)], 0));
}
} else {
}
}

if(cljs.core.truth_(new cljs.core.Keyword(null,"special-form","special-form",-1326536374).cljs$core$IFn$_invoke$arity$1(m))){
cljs.core.println.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2(["Special Form"], 0));

cljs.core.println.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([" ",new cljs.core.Keyword(null,"doc","doc",1913296891).cljs$core$IFn$_invoke$arity$1(m)], 0));

if(cljs.core.contains_QMARK_(m,new cljs.core.Keyword(null,"url","url",276297046))){
if(cljs.core.truth_(new cljs.core.Keyword(null,"url","url",276297046).cljs$core$IFn$_invoke$arity$1(m))){
return cljs.core.println.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([(""+"\n  Please see http://clojure.org/"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(new cljs.core.Keyword(null,"url","url",276297046).cljs$core$IFn$_invoke$arity$1(m)))], 0));
} else {
return null;
}
} else {
return cljs.core.println.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([(""+"\n  Please see http://clojure.org/special_forms#"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(new cljs.core.Keyword(null,"name","name",1843675177).cljs$core$IFn$_invoke$arity$1(m)))], 0));
}
} else {
if(cljs.core.truth_(new cljs.core.Keyword(null,"macro","macro",-867863404).cljs$core$IFn$_invoke$arity$1(m))){
cljs.core.println.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2(["Macro"], 0));
} else {
}

if(cljs.core.truth_(new cljs.core.Keyword(null,"spec","spec",347520401).cljs$core$IFn$_invoke$arity$1(m))){
cljs.core.println.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2(["Spec"], 0));
} else {
}

if(cljs.core.truth_(new cljs.core.Keyword(null,"repl-special-function","repl-special-function",1262603725).cljs$core$IFn$_invoke$arity$1(m))){
cljs.core.println.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2(["REPL Special Function"], 0));
} else {
}

cljs.core.println.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([" ",new cljs.core.Keyword(null,"doc","doc",1913296891).cljs$core$IFn$_invoke$arity$1(m)], 0));

if(cljs.core.truth_(new cljs.core.Keyword(null,"protocol","protocol",652470118).cljs$core$IFn$_invoke$arity$1(m))){
var seq__53943_54123 = cljs.core.seq(new cljs.core.Keyword(null,"methods","methods",453930866).cljs$core$IFn$_invoke$arity$1(m));
var chunk__53944_54124 = null;
var count__53945_54125 = (0);
var i__53946_54126 = (0);
while(true){
if((i__53946_54126 < count__53945_54125)){
var vec__53991_54127 = chunk__53944_54124.cljs$core$IIndexed$_nth$arity$2(null,i__53946_54126);
var name_54128 = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__53991_54127,(0),null);
var map__53994_54129 = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__53991_54127,(1),null);
var map__53994_54130__$1 = cljs.core.__destructure_map(map__53994_54129);
var doc_54131 = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__53994_54130__$1,new cljs.core.Keyword(null,"doc","doc",1913296891));
var arglists_54132 = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__53994_54130__$1,new cljs.core.Keyword(null,"arglists","arglists",1661989754));
cljs.core.println();

cljs.core.println.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([" ",name_54128], 0));

cljs.core.println.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([" ",arglists_54132], 0));

if(cljs.core.truth_(doc_54131)){
cljs.core.println.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([" ",doc_54131], 0));
} else {
}


var G__54133 = seq__53943_54123;
var G__54134 = chunk__53944_54124;
var G__54135 = count__53945_54125;
var G__54136 = (i__53946_54126 + (1));
seq__53943_54123 = G__54133;
chunk__53944_54124 = G__54134;
count__53945_54125 = G__54135;
i__53946_54126 = G__54136;
continue;
} else {
var temp__5804__auto___54137 = cljs.core.seq(seq__53943_54123);
if(temp__5804__auto___54137){
var seq__53943_54138__$1 = temp__5804__auto___54137;
if(cljs.core.chunked_seq_QMARK_(seq__53943_54138__$1)){
var c__5673__auto___54139 = cljs.core.chunk_first(seq__53943_54138__$1);
var G__54140 = cljs.core.chunk_rest(seq__53943_54138__$1);
var G__54141 = c__5673__auto___54139;
var G__54142 = cljs.core.count(c__5673__auto___54139);
var G__54143 = (0);
seq__53943_54123 = G__54140;
chunk__53944_54124 = G__54141;
count__53945_54125 = G__54142;
i__53946_54126 = G__54143;
continue;
} else {
var vec__54012_54144 = cljs.core.first(seq__53943_54138__$1);
var name_54145 = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__54012_54144,(0),null);
var map__54015_54146 = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__54012_54144,(1),null);
var map__54015_54147__$1 = cljs.core.__destructure_map(map__54015_54146);
var doc_54148 = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__54015_54147__$1,new cljs.core.Keyword(null,"doc","doc",1913296891));
var arglists_54149 = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__54015_54147__$1,new cljs.core.Keyword(null,"arglists","arglists",1661989754));
cljs.core.println();

cljs.core.println.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([" ",name_54145], 0));

cljs.core.println.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([" ",arglists_54149], 0));

if(cljs.core.truth_(doc_54148)){
cljs.core.println.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([" ",doc_54148], 0));
} else {
}


var G__54151 = cljs.core.next(seq__53943_54138__$1);
var G__54152 = null;
var G__54153 = (0);
var G__54154 = (0);
seq__53943_54123 = G__54151;
chunk__53944_54124 = G__54152;
count__53945_54125 = G__54153;
i__53946_54126 = G__54154;
continue;
}
} else {
}
}
break;
}
} else {
}

if(cljs.core.truth_(n)){
var temp__5804__auto__ = cljs.spec.alpha.get_spec(cljs.core.symbol.cljs$core$IFn$_invoke$arity$2((""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.ns_name(n))),cljs.core.name(nm)));
if(cljs.core.truth_(temp__5804__auto__)){
var fnspec = temp__5804__auto__;
cljs.core.print.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2(["Spec"], 0));

var seq__54020 = cljs.core.seq(new cljs.core.PersistentVector(null, 3, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"args","args",1315556576),new cljs.core.Keyword(null,"ret","ret",-468222814),new cljs.core.Keyword(null,"fn","fn",-1175266204)], null));
var chunk__54021 = null;
var count__54022 = (0);
var i__54023 = (0);
while(true){
if((i__54023 < count__54022)){
var role = chunk__54021.cljs$core$IIndexed$_nth$arity$2(null,i__54023);
var temp__5804__auto___54155__$1 = cljs.core.get.cljs$core$IFn$_invoke$arity$2(fnspec,role);
if(cljs.core.truth_(temp__5804__auto___54155__$1)){
var spec_54156 = temp__5804__auto___54155__$1;
cljs.core.print.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([(""+"\n "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.name(role))+":"),cljs.spec.alpha.describe(spec_54156)], 0));
} else {
}


var G__54157 = seq__54020;
var G__54158 = chunk__54021;
var G__54159 = count__54022;
var G__54160 = (i__54023 + (1));
seq__54020 = G__54157;
chunk__54021 = G__54158;
count__54022 = G__54159;
i__54023 = G__54160;
continue;
} else {
var temp__5804__auto____$1 = cljs.core.seq(seq__54020);
if(temp__5804__auto____$1){
var seq__54020__$1 = temp__5804__auto____$1;
if(cljs.core.chunked_seq_QMARK_(seq__54020__$1)){
var c__5673__auto__ = cljs.core.chunk_first(seq__54020__$1);
var G__54161 = cljs.core.chunk_rest(seq__54020__$1);
var G__54162 = c__5673__auto__;
var G__54163 = cljs.core.count(c__5673__auto__);
var G__54164 = (0);
seq__54020 = G__54161;
chunk__54021 = G__54162;
count__54022 = G__54163;
i__54023 = G__54164;
continue;
} else {
var role = cljs.core.first(seq__54020__$1);
var temp__5804__auto___54165__$2 = cljs.core.get.cljs$core$IFn$_invoke$arity$2(fnspec,role);
if(cljs.core.truth_(temp__5804__auto___54165__$2)){
var spec_54166 = temp__5804__auto___54165__$2;
cljs.core.print.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([(""+"\n "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.name(role))+":"),cljs.spec.alpha.describe(spec_54166)], 0));
} else {
}


var G__54167 = cljs.core.next(seq__54020__$1);
var G__54168 = null;
var G__54169 = (0);
var G__54170 = (0);
seq__54020 = G__54167;
chunk__54021 = G__54168;
count__54022 = G__54169;
i__54023 = G__54170;
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
} else {
return null;
}
}
});
/**
 * Constructs a data representation for a Error with keys:
 *  :cause - root cause message
 *  :phase - error phase
 *  :via - cause chain, with cause keys:
 *           :type - exception class symbol
 *           :message - exception message
 *           :data - ex-data
 *           :at - top stack element
 *  :trace - root cause stack elements
 */
cljs.repl.Error__GT_map = (function cljs$repl$Error__GT_map(o){
return cljs.core.Throwable__GT_map(o);
});
/**
 * Returns an analysis of the phase, error, cause, and location of an error that occurred
 *   based on Throwable data, as returned by Throwable->map. All attributes other than phase
 *   are optional:
 *  :clojure.error/phase - keyword phase indicator, one of:
 *    :read-source :compile-syntax-check :compilation :macro-syntax-check :macroexpansion
 *    :execution :read-eval-result :print-eval-result
 *  :clojure.error/source - file name (no path)
 *  :clojure.error/line - integer line number
 *  :clojure.error/column - integer column number
 *  :clojure.error/symbol - symbol being expanded/compiled/invoked
 *  :clojure.error/class - cause exception class symbol
 *  :clojure.error/cause - cause exception message
 *  :clojure.error/spec - explain-data for spec error
 */
cljs.repl.ex_triage = (function cljs$repl$ex_triage(datafied_throwable){
var map__54031 = datafied_throwable;
var map__54031__$1 = cljs.core.__destructure_map(map__54031);
var via = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__54031__$1,new cljs.core.Keyword(null,"via","via",-1904457336));
var trace = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__54031__$1,new cljs.core.Keyword(null,"trace","trace",-1082747415));
var phase = cljs.core.get.cljs$core$IFn$_invoke$arity$3(map__54031__$1,new cljs.core.Keyword(null,"phase","phase",575722892),new cljs.core.Keyword(null,"execution","execution",253283524));
var map__54032 = cljs.core.last(via);
var map__54032__$1 = cljs.core.__destructure_map(map__54032);
var type = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__54032__$1,new cljs.core.Keyword(null,"type","type",1174270348));
var message = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__54032__$1,new cljs.core.Keyword(null,"message","message",-406056002));
var data = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__54032__$1,new cljs.core.Keyword(null,"data","data",-232669377));
var map__54033 = data;
var map__54033__$1 = cljs.core.__destructure_map(map__54033);
var problems = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__54033__$1,new cljs.core.Keyword("cljs.spec.alpha","problems","cljs.spec.alpha/problems",447400814));
var fn = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__54033__$1,new cljs.core.Keyword("cljs.spec.alpha","fn","cljs.spec.alpha/fn",408600443));
var caller = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__54033__$1,new cljs.core.Keyword("cljs.spec.test.alpha","caller","cljs.spec.test.alpha/caller",-398302390));
var map__54034 = new cljs.core.Keyword(null,"data","data",-232669377).cljs$core$IFn$_invoke$arity$1(cljs.core.first(via));
var map__54034__$1 = cljs.core.__destructure_map(map__54034);
var top_data = map__54034__$1;
var source = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__54034__$1,new cljs.core.Keyword("clojure.error","source","clojure.error/source",-2011936397));
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3((function (){var G__54036 = phase;
var G__54036__$1 = (((G__54036 instanceof cljs.core.Keyword))?G__54036.fqn:null);
switch (G__54036__$1) {
case "read-source":
var map__54037 = data;
var map__54037__$1 = cljs.core.__destructure_map(map__54037);
var line = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__54037__$1,new cljs.core.Keyword("clojure.error","line","clojure.error/line",-1816287471));
var column = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__54037__$1,new cljs.core.Keyword("clojure.error","column","clojure.error/column",304721553));
var G__54038 = cljs.core.merge.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([new cljs.core.Keyword(null,"data","data",-232669377).cljs$core$IFn$_invoke$arity$1(cljs.core.second(via)),top_data], 0));
var G__54038__$1 = (cljs.core.truth_(source)?cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__54038,new cljs.core.Keyword("clojure.error","source","clojure.error/source",-2011936397),source):G__54038);
var G__54038__$2 = (cljs.core.truth_((function (){var fexpr__54039 = new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 2, ["NO_SOURCE_PATH",null,"NO_SOURCE_FILE",null], null), null);
return (fexpr__54039.cljs$core$IFn$_invoke$arity$1 ? fexpr__54039.cljs$core$IFn$_invoke$arity$1(source) : fexpr__54039.call(null,source));
})())?cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(G__54038__$1,new cljs.core.Keyword("clojure.error","source","clojure.error/source",-2011936397)):G__54038__$1);
if(cljs.core.truth_(message)){
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__54038__$2,new cljs.core.Keyword("clojure.error","cause","clojure.error/cause",-1879175742),message);
} else {
return G__54038__$2;
}

break;
case "compile-syntax-check":
case "compilation":
case "macro-syntax-check":
case "macroexpansion":
var G__54040 = top_data;
var G__54040__$1 = (cljs.core.truth_(source)?cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__54040,new cljs.core.Keyword("clojure.error","source","clojure.error/source",-2011936397),source):G__54040);
var G__54040__$2 = (cljs.core.truth_((function (){var fexpr__54041 = new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 2, ["NO_SOURCE_PATH",null,"NO_SOURCE_FILE",null], null), null);
return (fexpr__54041.cljs$core$IFn$_invoke$arity$1 ? fexpr__54041.cljs$core$IFn$_invoke$arity$1(source) : fexpr__54041.call(null,source));
})())?cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(G__54040__$1,new cljs.core.Keyword("clojure.error","source","clojure.error/source",-2011936397)):G__54040__$1);
var G__54040__$3 = (cljs.core.truth_(type)?cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__54040__$2,new cljs.core.Keyword("clojure.error","class","clojure.error/class",278435890),type):G__54040__$2);
var G__54040__$4 = (cljs.core.truth_(message)?cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__54040__$3,new cljs.core.Keyword("clojure.error","cause","clojure.error/cause",-1879175742),message):G__54040__$3);
if(cljs.core.truth_(problems)){
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__54040__$4,new cljs.core.Keyword("clojure.error","spec","clojure.error/spec",2055032595),data);
} else {
return G__54040__$4;
}

break;
case "read-eval-result":
case "print-eval-result":
var vec__54042 = cljs.core.first(trace);
var source__$1 = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__54042,(0),null);
var method = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__54042,(1),null);
var file = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__54042,(2),null);
var line = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__54042,(3),null);
var G__54047 = top_data;
var G__54047__$1 = (cljs.core.truth_(line)?cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__54047,new cljs.core.Keyword("clojure.error","line","clojure.error/line",-1816287471),line):G__54047);
var G__54047__$2 = (cljs.core.truth_(file)?cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__54047__$1,new cljs.core.Keyword("clojure.error","source","clojure.error/source",-2011936397),file):G__54047__$1);
var G__54047__$3 = (cljs.core.truth_((function (){var and__5140__auto__ = source__$1;
if(cljs.core.truth_(and__5140__auto__)){
return method;
} else {
return and__5140__auto__;
}
})())?cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__54047__$2,new cljs.core.Keyword("clojure.error","symbol","clojure.error/symbol",1544821994),(new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[source__$1,method],null))):G__54047__$2);
var G__54047__$4 = (cljs.core.truth_(type)?cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__54047__$3,new cljs.core.Keyword("clojure.error","class","clojure.error/class",278435890),type):G__54047__$3);
if(cljs.core.truth_(message)){
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__54047__$4,new cljs.core.Keyword("clojure.error","cause","clojure.error/cause",-1879175742),message);
} else {
return G__54047__$4;
}

break;
case "execution":
var vec__54054 = cljs.core.first(trace);
var source__$1 = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__54054,(0),null);
var method = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__54054,(1),null);
var file = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__54054,(2),null);
var line = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__54054,(3),null);
var file__$1 = cljs.core.first(cljs.core.remove.cljs$core$IFn$_invoke$arity$2((function (p1__54030_SHARP_){
var or__5142__auto__ = (p1__54030_SHARP_ == null);
if(or__5142__auto__){
return or__5142__auto__;
} else {
var fexpr__54057 = new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 2, ["NO_SOURCE_PATH",null,"NO_SOURCE_FILE",null], null), null);
return (fexpr__54057.cljs$core$IFn$_invoke$arity$1 ? fexpr__54057.cljs$core$IFn$_invoke$arity$1(p1__54030_SHARP_) : fexpr__54057.call(null,p1__54030_SHARP_));
}
}),new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [new cljs.core.Keyword(null,"file","file",-1269645878).cljs$core$IFn$_invoke$arity$1(caller),file], null)));
var err_line = (function (){var or__5142__auto__ = new cljs.core.Keyword(null,"line","line",212345235).cljs$core$IFn$_invoke$arity$1(caller);
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return line;
}
})();
var G__54058 = new cljs.core.PersistentArrayMap(null, 1, [new cljs.core.Keyword("clojure.error","class","clojure.error/class",278435890),type], null);
var G__54058__$1 = (cljs.core.truth_(err_line)?cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__54058,new cljs.core.Keyword("clojure.error","line","clojure.error/line",-1816287471),err_line):G__54058);
var G__54058__$2 = (cljs.core.truth_(message)?cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__54058__$1,new cljs.core.Keyword("clojure.error","cause","clojure.error/cause",-1879175742),message):G__54058__$1);
var G__54058__$3 = (cljs.core.truth_((function (){var or__5142__auto__ = fn;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
var and__5140__auto__ = source__$1;
if(cljs.core.truth_(and__5140__auto__)){
return method;
} else {
return and__5140__auto__;
}
}
})())?cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__54058__$2,new cljs.core.Keyword("clojure.error","symbol","clojure.error/symbol",1544821994),(function (){var or__5142__auto__ = fn;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return (new cljs.core.PersistentVector(null,2,(5),cljs.core.PersistentVector.EMPTY_NODE,[source__$1,method],null));
}
})()):G__54058__$2);
var G__54058__$4 = (cljs.core.truth_(file__$1)?cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__54058__$3,new cljs.core.Keyword("clojure.error","source","clojure.error/source",-2011936397),file__$1):G__54058__$3);
if(cljs.core.truth_(problems)){
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(G__54058__$4,new cljs.core.Keyword("clojure.error","spec","clojure.error/spec",2055032595),data);
} else {
return G__54058__$4;
}

break;
default:
throw (new Error((""+"No matching clause: "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(G__54036__$1))));

}
})(),new cljs.core.Keyword("clojure.error","phase","clojure.error/phase",275140358),phase);
});
/**
 * Returns a string from exception data, as produced by ex-triage.
 *   The first line summarizes the exception phase and location.
 *   The subsequent lines describe the cause.
 */
cljs.repl.ex_str = (function cljs$repl$ex_str(p__54061){
var map__54062 = p__54061;
var map__54062__$1 = cljs.core.__destructure_map(map__54062);
var triage_data = map__54062__$1;
var phase = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__54062__$1,new cljs.core.Keyword("clojure.error","phase","clojure.error/phase",275140358));
var source = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__54062__$1,new cljs.core.Keyword("clojure.error","source","clojure.error/source",-2011936397));
var line = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__54062__$1,new cljs.core.Keyword("clojure.error","line","clojure.error/line",-1816287471));
var column = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__54062__$1,new cljs.core.Keyword("clojure.error","column","clojure.error/column",304721553));
var symbol = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__54062__$1,new cljs.core.Keyword("clojure.error","symbol","clojure.error/symbol",1544821994));
var class$ = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__54062__$1,new cljs.core.Keyword("clojure.error","class","clojure.error/class",278435890));
var cause = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__54062__$1,new cljs.core.Keyword("clojure.error","cause","clojure.error/cause",-1879175742));
var spec = cljs.core.get.cljs$core$IFn$_invoke$arity$2(map__54062__$1,new cljs.core.Keyword("clojure.error","spec","clojure.error/spec",2055032595));
var loc = (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1((function (){var or__5142__auto__ = source;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return "<cljs repl>";
}
})())+":"+cljs.core.str.cljs$core$IFn$_invoke$arity$1((function (){var or__5142__auto__ = line;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return (1);
}
})())+cljs.core.str.cljs$core$IFn$_invoke$arity$1((cljs.core.truth_(column)?(""+":"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(column)):"")));
var class_name = cljs.core.name((function (){var or__5142__auto__ = class$;
if(cljs.core.truth_(or__5142__auto__)){
return or__5142__auto__;
} else {
return "";
}
})());
var simple_class = class_name;
var cause_type = ((cljs.core.contains_QMARK_(new cljs.core.PersistentHashSet(null, new cljs.core.PersistentArrayMap(null, 2, ["RuntimeException",null,"Exception",null], null), null),simple_class))?"":(""+" ("+cljs.core.str.cljs$core$IFn$_invoke$arity$1(simple_class)+")"));
var format = goog.string.format;
var G__54063 = phase;
var G__54063__$1 = (((G__54063 instanceof cljs.core.Keyword))?G__54063.fqn:null);
switch (G__54063__$1) {
case "read-source":
return (format.cljs$core$IFn$_invoke$arity$3 ? format.cljs$core$IFn$_invoke$arity$3("Syntax error reading source at (%s).\n%s\n",loc,cause) : format.call(null,"Syntax error reading source at (%s).\n%s\n",loc,cause));

break;
case "macro-syntax-check":
var G__54064 = "Syntax error macroexpanding %sat (%s).\n%s";
var G__54065 = (cljs.core.truth_(symbol)?(""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(symbol)+" "):"");
var G__54066 = loc;
var G__54067 = (cljs.core.truth_(spec)?(function (){var sb__5795__auto__ = (new goog.string.StringBuffer());
var _STAR_print_newline_STAR__orig_val__54068_54178 = cljs.core._STAR_print_newline_STAR_;
var _STAR_print_fn_STAR__orig_val__54069_54179 = cljs.core._STAR_print_fn_STAR_;
var _STAR_print_newline_STAR__temp_val__54070_54180 = true;
var _STAR_print_fn_STAR__temp_val__54071_54181 = (function (x__5796__auto__){
return sb__5795__auto__.append(x__5796__auto__);
});
(cljs.core._STAR_print_newline_STAR_ = _STAR_print_newline_STAR__temp_val__54070_54180);

(cljs.core._STAR_print_fn_STAR_ = _STAR_print_fn_STAR__temp_val__54071_54181);

try{cljs.spec.alpha.explain_out(cljs.core.update.cljs$core$IFn$_invoke$arity$3(spec,new cljs.core.Keyword("cljs.spec.alpha","problems","cljs.spec.alpha/problems",447400814),(function (probs){
return cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (p1__54059_SHARP_){
return cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(p1__54059_SHARP_,new cljs.core.Keyword(null,"in","in",-1531184865));
}),probs);
}))
);
}finally {(cljs.core._STAR_print_fn_STAR_ = _STAR_print_fn_STAR__orig_val__54069_54179);

(cljs.core._STAR_print_newline_STAR_ = _STAR_print_newline_STAR__orig_val__54068_54178);
}
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(sb__5795__auto__));
})():(format.cljs$core$IFn$_invoke$arity$2 ? format.cljs$core$IFn$_invoke$arity$2("%s\n",cause) : format.call(null,"%s\n",cause)));
return (format.cljs$core$IFn$_invoke$arity$4 ? format.cljs$core$IFn$_invoke$arity$4(G__54064,G__54065,G__54066,G__54067) : format.call(null,G__54064,G__54065,G__54066,G__54067));

break;
case "macroexpansion":
var G__54072 = "Unexpected error%s macroexpanding %sat (%s).\n%s\n";
var G__54073 = cause_type;
var G__54074 = (cljs.core.truth_(symbol)?(""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(symbol)+" "):"");
var G__54075 = loc;
var G__54076 = cause;
return (format.cljs$core$IFn$_invoke$arity$5 ? format.cljs$core$IFn$_invoke$arity$5(G__54072,G__54073,G__54074,G__54075,G__54076) : format.call(null,G__54072,G__54073,G__54074,G__54075,G__54076));

break;
case "compile-syntax-check":
var G__54077 = "Syntax error%s compiling %sat (%s).\n%s\n";
var G__54078 = cause_type;
var G__54079 = (cljs.core.truth_(symbol)?(""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(symbol)+" "):"");
var G__54080 = loc;
var G__54081 = cause;
return (format.cljs$core$IFn$_invoke$arity$5 ? format.cljs$core$IFn$_invoke$arity$5(G__54077,G__54078,G__54079,G__54080,G__54081) : format.call(null,G__54077,G__54078,G__54079,G__54080,G__54081));

break;
case "compilation":
var G__54082 = "Unexpected error%s compiling %sat (%s).\n%s\n";
var G__54083 = cause_type;
var G__54084 = (cljs.core.truth_(symbol)?(""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(symbol)+" "):"");
var G__54085 = loc;
var G__54086 = cause;
return (format.cljs$core$IFn$_invoke$arity$5 ? format.cljs$core$IFn$_invoke$arity$5(G__54082,G__54083,G__54084,G__54085,G__54086) : format.call(null,G__54082,G__54083,G__54084,G__54085,G__54086));

break;
case "read-eval-result":
return (format.cljs$core$IFn$_invoke$arity$5 ? format.cljs$core$IFn$_invoke$arity$5("Error reading eval result%s at %s (%s).\n%s\n",cause_type,symbol,loc,cause) : format.call(null,"Error reading eval result%s at %s (%s).\n%s\n",cause_type,symbol,loc,cause));

break;
case "print-eval-result":
return (format.cljs$core$IFn$_invoke$arity$5 ? format.cljs$core$IFn$_invoke$arity$5("Error printing return value%s at %s (%s).\n%s\n",cause_type,symbol,loc,cause) : format.call(null,"Error printing return value%s at %s (%s).\n%s\n",cause_type,symbol,loc,cause));

break;
case "execution":
if(cljs.core.truth_(spec)){
var G__54087 = "Execution error - invalid arguments to %s at (%s).\n%s";
var G__54088 = symbol;
var G__54089 = loc;
var G__54090 = (function (){var sb__5795__auto__ = (new goog.string.StringBuffer());
var _STAR_print_newline_STAR__orig_val__54091_54183 = cljs.core._STAR_print_newline_STAR_;
var _STAR_print_fn_STAR__orig_val__54092_54184 = cljs.core._STAR_print_fn_STAR_;
var _STAR_print_newline_STAR__temp_val__54093_54185 = true;
var _STAR_print_fn_STAR__temp_val__54094_54186 = (function (x__5796__auto__){
return sb__5795__auto__.append(x__5796__auto__);
});
(cljs.core._STAR_print_newline_STAR_ = _STAR_print_newline_STAR__temp_val__54093_54185);

(cljs.core._STAR_print_fn_STAR_ = _STAR_print_fn_STAR__temp_val__54094_54186);

try{cljs.spec.alpha.explain_out(cljs.core.update.cljs$core$IFn$_invoke$arity$3(spec,new cljs.core.Keyword("cljs.spec.alpha","problems","cljs.spec.alpha/problems",447400814),(function (probs){
return cljs.core.map.cljs$core$IFn$_invoke$arity$2((function (p1__54060_SHARP_){
return cljs.core.dissoc.cljs$core$IFn$_invoke$arity$2(p1__54060_SHARP_,new cljs.core.Keyword(null,"in","in",-1531184865));
}),probs);
}))
);
}finally {(cljs.core._STAR_print_fn_STAR_ = _STAR_print_fn_STAR__orig_val__54092_54184);

(cljs.core._STAR_print_newline_STAR_ = _STAR_print_newline_STAR__orig_val__54091_54183);
}
return (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(sb__5795__auto__));
})();
return (format.cljs$core$IFn$_invoke$arity$4 ? format.cljs$core$IFn$_invoke$arity$4(G__54087,G__54088,G__54089,G__54090) : format.call(null,G__54087,G__54088,G__54089,G__54090));
} else {
var G__54095 = "Execution error%s at %s(%s).\n%s\n";
var G__54096 = cause_type;
var G__54097 = (cljs.core.truth_(symbol)?(""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(symbol)+" "):"");
var G__54098 = loc;
var G__54099 = cause;
return (format.cljs$core$IFn$_invoke$arity$5 ? format.cljs$core$IFn$_invoke$arity$5(G__54095,G__54096,G__54097,G__54098,G__54099) : format.call(null,G__54095,G__54096,G__54097,G__54098,G__54099));
}

break;
default:
throw (new Error((""+"No matching clause: "+cljs.core.str.cljs$core$IFn$_invoke$arity$1(G__54063__$1))));

}
});
cljs.repl.error__GT_str = (function cljs$repl$error__GT_str(error){
return cljs.repl.ex_str(cljs.repl.ex_triage(cljs.repl.Error__GT_map(error)));
});

//# sourceMappingURL=cljs.repl.js.map
