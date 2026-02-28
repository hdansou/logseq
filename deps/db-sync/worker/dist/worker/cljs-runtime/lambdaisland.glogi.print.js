import "./cljs_env.js";
import "./cljs.core.js";
import "./lambdaisland.glogi.js";
import "./goog.object.object.js";
goog.provide('lambdaisland.glogi.print');
goog.scope(function(){
  lambdaisland.glogi.print.goog$module$goog$object = goog.module.get('goog.object');
});
lambdaisland.glogi.print.colors = cljs.core.PersistentHashMap.fromArrays([new cljs.core.Keyword(null,"orange","orange",73816386),new cljs.core.Keyword(null,"gray1","gray1",23549221),new cljs.core.Keyword(null,"white","white",-483998618),new cljs.core.Keyword(null,"gray3","gray3",-617553786),new cljs.core.Keyword(null,"gray4","gray4",460735815),new cljs.core.Keyword(null,"yellow","yellow",-881035449),new cljs.core.Keyword(null,"green","green",-945526839),new cljs.core.Keyword(null,"turqoise","turqoise",1674869457),new cljs.core.Keyword(null,"gray2","gray2",-1424527469),new cljs.core.Keyword(null,"red","red",-969428204),new cljs.core.Keyword(null,"blue","blue",-622100620),new cljs.core.Keyword(null,"purple","purple",-876021126),new cljs.core.Keyword(null,"gray6","gray6",-2106469670),new cljs.core.Keyword(null,"gray5","gray5",1481094938),new cljs.core.Keyword(null,"brown","brown",1414854429),new cljs.core.Keyword(null,"black","black",1294279647)],["#f5871f","#e0e0e0","#ffffff","#8e908c","#969896","#eab700","#718c00","#3e999f","#d6d6d6","#c82829","#4271ae","#8959a8","#282a2e","#4d4d4c","#a3685a","#1d1f21"]);
lambdaisland.glogi.print.level_color = (function lambdaisland$glogi$print$level_color(level){
var pred__42670 = cljs.core._LT__EQ_;
var expr__42671 = lambdaisland.glogi.level_value(level);
if(cljs.core.truth_((function (){var G__42673 = lambdaisland.glogi.level_value(new cljs.core.Keyword(null,"severe","severe",-1364500238));
var G__42674 = expr__42671;
return (pred__42670.cljs$core$IFn$_invoke$arity$2 ? pred__42670.cljs$core$IFn$_invoke$arity$2(G__42673,G__42674) : pred__42670.call(null,G__42673,G__42674));
})())){
return new cljs.core.Keyword(null,"red","red",-969428204);
} else {
if(cljs.core.truth_((function (){var G__42675 = lambdaisland.glogi.level_value(new cljs.core.Keyword(null,"warning","warning",-1685650671));
var G__42676 = expr__42671;
return (pred__42670.cljs$core$IFn$_invoke$arity$2 ? pred__42670.cljs$core$IFn$_invoke$arity$2(G__42675,G__42676) : pred__42670.call(null,G__42675,G__42676));
})())){
return new cljs.core.Keyword(null,"orange","orange",73816386);
} else {
if(cljs.core.truth_((function (){var G__42678 = lambdaisland.glogi.level_value(new cljs.core.Keyword(null,"info","info",-317069002));
var G__42679 = expr__42671;
return (pred__42670.cljs$core$IFn$_invoke$arity$2 ? pred__42670.cljs$core$IFn$_invoke$arity$2(G__42678,G__42679) : pred__42670.call(null,G__42678,G__42679));
})())){
return new cljs.core.Keyword(null,"blue","blue",-622100620);
} else {
if(cljs.core.truth_((function (){var G__42681 = lambdaisland.glogi.level_value(new cljs.core.Keyword(null,"config","config",994861415));
var G__42682 = expr__42671;
return (pred__42670.cljs$core$IFn$_invoke$arity$2 ? pred__42670.cljs$core$IFn$_invoke$arity$2(G__42681,G__42682) : pred__42670.call(null,G__42681,G__42682));
})())){
return new cljs.core.Keyword(null,"green","green",-945526839);
} else {
if(cljs.core.truth_((function (){var G__42683 = lambdaisland.glogi.level_value(new cljs.core.Keyword(null,"fine","fine",-873037193));
var G__42684 = expr__42671;
return (pred__42670.cljs$core$IFn$_invoke$arity$2 ? pred__42670.cljs$core$IFn$_invoke$arity$2(G__42683,G__42684) : pred__42670.call(null,G__42683,G__42684));
})())){
return new cljs.core.Keyword(null,"yellow","yellow",-881035449);
} else {
if(cljs.core.truth_((function (){var G__42685 = lambdaisland.glogi.level_value(new cljs.core.Keyword(null,"finer","finer",974902846));
var G__42686 = expr__42671;
return (pred__42670.cljs$core$IFn$_invoke$arity$2 ? pred__42670.cljs$core$IFn$_invoke$arity$2(G__42685,G__42686) : pred__42670.call(null,G__42685,G__42686));
})())){
return new cljs.core.Keyword(null,"gray3","gray3",-617553786);
} else {
if(cljs.core.truth_((function (){var G__42687 = lambdaisland.glogi.level_value(new cljs.core.Keyword(null,"finest","finest",-1359568890));
var G__42688 = expr__42671;
return (pred__42670.cljs$core$IFn$_invoke$arity$2 ? pred__42670.cljs$core$IFn$_invoke$arity$2(G__42687,G__42688) : pred__42670.call(null,G__42687,G__42688));
})())){
return new cljs.core.Keyword(null,"gray4","gray4",460735815);
} else {
return new cljs.core.Keyword(null,"gray2","gray2",-1424527469);
}
}
}
}
}
}
}
});
lambdaisland.glogi.print.add = (function lambdaisland$glogi$print$add(var_args){
var G__42692 = arguments.length;
switch (G__42692) {
case 2:
return lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$2((arguments[(0)]),(arguments[(1)]));

break;
case 3:
return lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$3((arguments[(0)]),(arguments[(1)]),(arguments[(2)]));

break;
case 4:
return lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$4((arguments[(0)]),(arguments[(1)]),(arguments[(2)]),(arguments[(3)]));

break;
default:
throw (new Error(["Invalid arity: ",arguments.length].join("")));

}
});

(lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$2 = (function (p__42702,s){
var vec__42703 = p__42702;
var res = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42703,(0),null);
var res_css = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42703,(1),null);
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(res)+cljs.core.str.cljs$core$IFn$_invoke$arity$1(s)),res_css], null);
}));

(lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$3 = (function (p__42709,s,color){
var vec__42710 = p__42709;
var res = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42710,(0),null);
var res_css = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42710,(1),null);
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(res)+"%c"+cljs.core.str.cljs$core$IFn$_invoke$arity$1((""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(s)))+"%c"),cljs.core.conj.cljs$core$IFn$_invoke$arity$variadic(res_css,(""+"color:"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.get.cljs$core$IFn$_invoke$arity$2(lambdaisland.glogi.print.colors,color))),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2(["color:black"], 0))], null);
}));

(lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$4 = (function (p__42714,s,fg,bg){
var vec__42716 = p__42714;
var res = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42716,(0),null);
var res_css = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42716,(1),null);
return new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, [(""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(res)+"%c"+cljs.core.str.cljs$core$IFn$_invoke$arity$1((""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(s)))+"%c"),cljs.core.conj.cljs$core$IFn$_invoke$arity$variadic(res_css,(""+"color:"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.get.cljs$core$IFn$_invoke$arity$2(lambdaisland.glogi.print.colors,fg))+";background-color:"+cljs.core.str.cljs$core$IFn$_invoke$arity$1(cljs.core.get.cljs$core$IFn$_invoke$arity$2(lambdaisland.glogi.print.colors,bg))),cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2(["color:black;background-color:inherit"], 0))], null);
}));

(lambdaisland.glogi.print.add.cljs$lang$maxFixedArity = 4);

lambdaisland.glogi.print.print_console_log_css = (function lambdaisland$glogi$print$print_console_log_css(res,value){
while(true){
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("lambdaisland.glogi.print","comma","lambdaisland.glogi.print/comma",-725182078),value)){
return lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$3(res,", ",new cljs.core.Keyword(null,"gray2","gray2",-1424527469));
} else {
if(cljs.core._EQ_.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("lambdaisland.glogi.print","space","lambdaisland.glogi.print/space",1682088588),value)){
return lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$2(res," ");
} else {
if((value instanceof cljs.core.Keyword)){
return lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$3(res,value,new cljs.core.Keyword(null,"blue","blue",-622100620));
} else {
if((value instanceof cljs.core.Symbol)){
return lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$3(res,value,new cljs.core.Keyword(null,"green","green",-945526839));
} else {
if(typeof value === 'string'){
return lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$3(res,cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([value], 0)),new cljs.core.Keyword(null,"turqoise","turqoise",1674869457));
} else {
if(cljs.core.map_entry_QMARK_(value)){
var G__42723 = lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$2((function (){var G__42725 = res;
var G__42726 = cljs.core.key(value);
return (lambdaisland.glogi.print.print_console_log_css.cljs$core$IFn$_invoke$arity$2 ? lambdaisland.glogi.print.print_console_log_css.cljs$core$IFn$_invoke$arity$2(G__42725,G__42726) : lambdaisland.glogi.print.print_console_log_css.call(null,G__42725,G__42726));
})()," ");
var G__42724 = cljs.core.val(value);
return (lambdaisland.glogi.print.print_console_log_css.cljs$core$IFn$_invoke$arity$2 ? lambdaisland.glogi.print.print_console_log_css.cljs$core$IFn$_invoke$arity$2(G__42723,G__42724) : lambdaisland.glogi.print.print_console_log_css.call(null,G__42723,G__42724));
} else {
if((((value instanceof cljs.core.PersistentArrayMap)) || ((value instanceof cljs.core.PersistentHashMap)))){
var _PERCENT_ = res;
var _PERCENT___$1 = lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$3(_PERCENT_,"{",new cljs.core.Keyword(null,"purple","purple",-876021126));
var _PERCENT___$2 = cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(lambdaisland.glogi.print.print_console_log_css,_PERCENT___$1,cljs.core.interpose.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("lambdaisland.glogi.print","comma","lambdaisland.glogi.print/comma",-725182078),value));
return lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$3(_PERCENT___$2,"}",new cljs.core.Keyword(null,"purple","purple",-876021126));
} else {
if(cljs.core.map_QMARK_(value)){
var _PERCENT_ = res;
var _PERCENT___$1 = lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$3(_PERCENT_,(""+"#"+cljs.core.str.cljs$core$IFn$_invoke$arity$1((function (){var t = cljs.core.type(value);
var n = t.name;
if(cljs.core.empty_QMARK_(n)){
return cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([t], 0));
} else {
return n;
}
})())+" "),new cljs.core.Keyword(null,"brown","brown",1414854429));
var _PERCENT___$2 = lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$3(_PERCENT___$1,"{",new cljs.core.Keyword(null,"purple","purple",-876021126));
var _PERCENT___$3 = cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(lambdaisland.glogi.print.print_console_log_css,_PERCENT___$2,cljs.core.interpose.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("lambdaisland.glogi.print","comma","lambdaisland.glogi.print/comma",-725182078),value));
return lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$3(_PERCENT___$3,"}",new cljs.core.Keyword(null,"purple","purple",-876021126));
} else {
if(cljs.core.set_QMARK_(value)){
var _PERCENT_ = res;
var _PERCENT___$1 = lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$3(_PERCENT_,"#{",new cljs.core.Keyword(null,"purple","purple",-876021126));
var _PERCENT___$2 = cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(lambdaisland.glogi.print.print_console_log_css,_PERCENT___$1,cljs.core.interpose.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("lambdaisland.glogi.print","space","lambdaisland.glogi.print/space",1682088588),value));
return lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$3(_PERCENT___$2,"}",new cljs.core.Keyword(null,"purple","purple",-876021126));
} else {
if(cljs.core.vector_QMARK_(value)){
var _PERCENT_ = res;
var _PERCENT___$1 = lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$3(_PERCENT_,"[",new cljs.core.Keyword(null,"purple","purple",-876021126));
var _PERCENT___$2 = cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(lambdaisland.glogi.print.print_console_log_css,_PERCENT___$1,cljs.core.interpose.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("lambdaisland.glogi.print","space","lambdaisland.glogi.print/space",1682088588),value));
return lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$3(_PERCENT___$2,"]",new cljs.core.Keyword(null,"purple","purple",-876021126));
} else {
if((value instanceof cljs.core.PersistentQueue)){
var G__42765 = lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$3(res,"#queue ",new cljs.core.Keyword(null,"brown","brown",1414854429));
var G__42766 = cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentVector.EMPTY,value);
res = G__42765;
value = G__42766;
continue;
} else {
if(cljs.core.seq_QMARK_(value)){
var _PERCENT_ = res;
var _PERCENT___$1 = lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$3(_PERCENT_,"(",new cljs.core.Keyword(null,"brown","brown",1414854429));
var _PERCENT___$2 = cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(lambdaisland.glogi.print.print_console_log_css,_PERCENT___$1,cljs.core.interpose.cljs$core$IFn$_invoke$arity$2(new cljs.core.Keyword("lambdaisland.glogi.print","space","lambdaisland.glogi.print/space",1682088588),value));
return lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$3(_PERCENT___$2,")",new cljs.core.Keyword(null,"brown","brown",1414854429));
} else {
if((((!((value == null))))?(((((value.cljs$lang$protocol_mask$partition1$ & (16384))) || ((cljs.core.PROTOCOL_SENTINEL === value.cljs$core$IAtom$))))?true:(((!value.cljs$lang$protocol_mask$partition1$))?cljs.core.native_satisfies_QMARK_(cljs.core.IAtom,value):false)):cljs.core.native_satisfies_QMARK_(cljs.core.IAtom,value))){
var G__42767 = lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$3(res,"#atom ",new cljs.core.Keyword(null,"brown","brown",1414854429));
var G__42768 = cljs.core.deref(value);
res = G__42767;
value = G__42768;
continue;
} else {
if(cljs.core.uuid_QMARK_(value)){
var G__42769 = lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$3(res,"#uuid ",new cljs.core.Keyword(null,"brown","brown",1414854429));
var G__42770 = (""+cljs.core.str.cljs$core$IFn$_invoke$arity$1(value));
res = G__42769;
value = G__42770;
continue;
} else {
if(cljs.core.object_QMARK_(value)){
var G__42771 = lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$3(res,"#js ",new cljs.core.Keyword(null,"brown","brown",1414854429));
var G__42772 = cljs.core.reduce.cljs$core$IFn$_invoke$arity$3(((function (res,value){
return (function (p1__42720_SHARP_,p2__42721_SHARP_){
return cljs.core.assoc.cljs$core$IFn$_invoke$arity$3(p1__42720_SHARP_,cljs.core.keyword.cljs$core$IFn$_invoke$arity$1(p2__42721_SHARP_),lambdaisland.glogi.print.goog$module$goog$object.get(value,p2__42721_SHARP_));
});})(res,value))
,cljs.core.PersistentArrayMap.EMPTY,Object.keys(value));
res = G__42771;
value = G__42772;
continue;
} else {
if(cljs.core.truth_(cljs.core.array_QMARK_(value))){
var G__42773 = lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$3(res,"#js ",new cljs.core.Keyword(null,"brown","brown",1414854429));
var G__42774 = cljs.core.into.cljs$core$IFn$_invoke$arity$2(cljs.core.PersistentVector.EMPTY,value);
res = G__42773;
value = G__42774;
continue;
} else {
return lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$3(res,cljs.core.pr_str.cljs$core$IFn$_invoke$arity$variadic(cljs.core.prim_seq.cljs$core$IFn$_invoke$arity$2([value], 0)),new cljs.core.Keyword(null,"gray5","gray5",1481094938));

}
}
}
}
}
}
}
}
}
}
}
}
}
}
}
}
break;
}
});
lambdaisland.glogi.print.format = (function lambdaisland$glogi$print$format(var_args){
var G__42747 = arguments.length;
switch (G__42747) {
case 3:
return lambdaisland.glogi.print.format.cljs$core$IFn$_invoke$arity$3((arguments[(0)]),(arguments[(1)]),(arguments[(2)]));

break;
case 4:
return lambdaisland.glogi.print.format.cljs$core$IFn$_invoke$arity$4((arguments[(0)]),(arguments[(1)]),(arguments[(2)]),(arguments[(3)]));

break;
default:
throw (new Error(["Invalid arity: ",arguments.length].join("")));

}
});

(lambdaisland.glogi.print.format.cljs$core$IFn$_invoke$arity$3 = (function (level,logger_name,value){
return lambdaisland.glogi.print.format.cljs$core$IFn$_invoke$arity$4(new cljs.core.PersistentVector(null, 2, 5, cljs.core.PersistentVector.EMPTY_NODE, ["",cljs.core.PersistentVector.EMPTY], null),level,logger_name,value);
}));

(lambdaisland.glogi.print.format.cljs$core$IFn$_invoke$arity$4 = (function (init,level,logger_name,value){
var color = lambdaisland.glogi.print.level_color(level);
var vec__42753 = lambdaisland.glogi.print.print_console_log_css(lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$2(lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$4(lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$4(lambdaisland.glogi.print.add.cljs$core$IFn$_invoke$arity$4(init,"[",new cljs.core.Keyword(null,"white","white",-483998618),color),logger_name,new cljs.core.Keyword(null,"white","white",-483998618),color),"]",new cljs.core.Keyword(null,"white","white",-483998618),color)," "),value);
var res = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42753,(0),null);
var res_css = cljs.core.nth.cljs$core$IFn$_invoke$arity$3(vec__42753,(1),null);
return cljs.core.cons(res,res_css);
}));

(lambdaisland.glogi.print.format.cljs$lang$maxFixedArity = 4);


//# sourceMappingURL=lambdaisland.glogi.print.js.map
