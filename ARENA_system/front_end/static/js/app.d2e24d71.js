(function(e){function t(t){for(var l,i,o=t[0],r=t[1],c=t[2],p=0,u=[];p<o.length;p++)i=o[p],Object.prototype.hasOwnProperty.call(s,i)&&s[i]&&u.push(s[i][0]),s[i]=0;for(l in r)Object.prototype.hasOwnProperty.call(r,l)&&(e[l]=r[l]);d&&d(t);while(u.length)u.shift()();return n.push.apply(n,c||[]),a()}function a(){for(var e,t=0;t<n.length;t++){for(var a=n[t],l=!0,o=1;o<a.length;o++){var r=a[o];0!==s[r]&&(l=!1)}l&&(n.splice(t--,1),e=i(i.s=a[0]))}return e}var l={},s={app:0},n=[];function i(t){if(l[t])return l[t].exports;var a=l[t]={i:t,l:!1,exports:{}};return e[t].call(a.exports,a,a.exports,i),a.l=!0,a.exports}i.m=e,i.c=l,i.d=function(e,t,a){i.o(e,t)||Object.defineProperty(e,t,{enumerable:!0,get:a})},i.r=function(e){"undefined"!==typeof Symbol&&Symbol.toStringTag&&Object.defineProperty(e,Symbol.toStringTag,{value:"Module"}),Object.defineProperty(e,"__esModule",{value:!0})},i.t=function(e,t){if(1&t&&(e=i(e)),8&t)return e;if(4&t&&"object"===typeof e&&e&&e.__esModule)return e;var a=Object.create(null);if(i.r(a),Object.defineProperty(a,"default",{enumerable:!0,value:e}),2&t&&"string"!=typeof e)for(var l in e)i.d(a,l,function(t){return e[t]}.bind(null,l));return a},i.n=function(e){var t=e&&e.__esModule?function(){return e["default"]}:function(){return e};return i.d(t,"a",t),t},i.o=function(e,t){return Object.prototype.hasOwnProperty.call(e,t)},i.p="/";var o=window["webpackJsonp"]=window["webpackJsonp"]||[],r=o.push.bind(o);o.push=t,o=o.slice();for(var c=0;c<o.length;c++)t(o[c]);var d=r;n.push([0,"chunk-vendors"]),a()})({0:function(e,t,a){e.exports=a("56d7")},"0156":function(e,t,a){"use strict";a("fa7c")},"034f":function(e,t,a){"use strict";a("85ec")},"2aff":function(e,t,a){e.exports=a.p+"static/img/step3.b5b01c50.jpg"},"4d1a":function(e,t,a){"use strict";a("a82f")},"56d7":function(e,t,a){"use strict";a.r(t);var l=a("a026"),s=function(){var e=this,t=e.$createElement,a=e._self._c||t;return a("div",{attrs:{id:"app"}},[a("router-view")],1)},n=[],i={name:"App"},o=i,r=(a("034f"),a("2877")),c=Object(r["a"])(o,s,n,!1,null,null,null),d=c.exports,p=a("8c4f"),u=function(){var e=this,t=e.$createElement,a=e._self._c||t;return a("div",{staticClass:"hello"},[a("el-container",{staticClass:"layout-container"},[a("el-header",{staticClass:"el-header"},[a("div",{staticClass:"title"},[a("span",[e._v("ARENA")])])]),a("el-container",[a("el-aside",{attrs:{width:"300px"}},[a("el-scrollbar",[a("el-menu",{attrs:{"active-text-color":"#409EFF"}},[a("el-submenu",{attrs:{index:"1"},scopedSlots:e._u([{key:"title",fn:function(){return[a("i",{staticClass:"el-icon-s-data"}),a("span",[e._v("Database Information")])]},proxy:!0}])},e._l(e.tableList,(function(t,l){return a("el-submenu",{key:l,attrs:{index:l},scopedSlots:e._u([{key:"title",fn:function(){return[e._v(e._s(l))]},proxy:!0}],null,!0)},e._l(t,(function(t,l){return a("el-menu-item",{key:l,attrs:{index:l}},[e._v(" "+e._s(l)+" ("+e._s(t)+") ")])})),1)})),1),a("el-submenu",{attrs:{index:"4"},scopedSlots:e._u([{key:"title",fn:function(){return[a("i",{staticClass:"el-icon-s-promotion"}),a("span",[e._v("Feedback")])]},proxy:!0}])},[a("el-menu-item-group",{staticClass:"feedbackGroup"},[a("template",{slot:"title"},[e._v("The most valuable plan")]),a("el-menu-item",{attrs:{index:"4-1-1"}},[a("el-input",{staticStyle:{"font-size":"13px"},attrs:{placeholder:"plan id",size:"mini"},model:{value:e.bestPlanId,callback:function(t){e.bestPlanId=t},expression:"bestPlanId"}},[a("template",{slot:"prepend"},[e._v("Plan")])],2)],1),a("el-menu-item",{staticClass:"noHighLight",attrs:{index:"4-1-2"}},[a("el-input",{staticStyle:{"font-size":"13px"},attrs:{type:"textarea",rows:5,placeholder:"Please input the reason."},model:{value:e.bestPlanReason,callback:function(t){e.bestPlanReason=t},expression:"bestPlanReason"}})],1)],2),a("el-menu-item-group",{staticClass:"feedbackGroup"},[a("template",{slot:"title"},[e._v("The least valuable plan")]),a("el-menu-item",{attrs:{index:"4-2-1"}},[a("el-input",{staticStyle:{"font-size":"13px"},attrs:{placeholder:"plan id",size:"mini"},model:{value:e.worstPlanId,callback:function(t){e.worstPlanId=t},expression:"worstPlanId"}},[a("template",{slot:"prepend"},[e._v("Plan")])],2)],1),a("el-menu-item",{staticClass:"noHighLight",attrs:{index:"4-2-2"}},[a("el-input",{staticStyle:{"font-size":"13px"},attrs:{type:"textarea",rows:5,placeholder:"Please input the reason."},model:{value:e.worstPlanReason,callback:function(t){e.worstPlanReason=t},expression:"worstPlanReason"}})],1)],2),a("el-menu-item-group",[a("template",{slot:"title"},[e._v("Submit your feedback")]),a("el-menu-item",{attrs:{index:"4-3-1"}},[a("el-button",{attrs:{size:"mini"}},[e._v("Submit")])],1)],2)],1),a("el-submenu",{attrs:{index:"2"},scopedSlots:e._u([{key:"title",fn:function(){return[a("i",{staticClass:"el-icon-s-tools"}),a("span",[e._v("Setting")])]},proxy:!0}])},[a("el-menu-item",{attrs:{index:"2-1"}},[a("el-switch",{attrs:{"active-text":"Compare Plan"},model:{value:e.isComparePlan,callback:function(t){e.isComparePlan=t},expression:"isComparePlan"}})],1)],1),a("el-submenu",{attrs:{index:"3"},scopedSlots:e._u([{key:"title",fn:function(){return[a("i",{staticClass:"el-icon-question"}),a("span",[e._v("Help")])]},proxy:!0}])},[a("el-menu-item",{attrs:{index:"3-1"}},[a("router-link",{attrs:{tag:"a",target:"_blank",to:"about"}},[e._v(" About ")])],1)],1)],1)],1)],1),a("el-main",[a("el-row",{staticClass:"el-row",attrs:{gutter:20}},[a("el-col",{staticClass:"el-col",attrs:{span:9}},[a("el-row",{staticClass:"el-row"},[a("el-input",{attrs:{type:"textarea",placeholder:"Input your SQL or select an example SQL",autosize:{minRows:10,maxRows:20}},model:{value:e.SQLstatement,callback:function(t){e.SQLstatement=t},expression:"SQLstatement"}})],1),a("el-row",{staticClass:"el-row",attrs:{gutter:10}},[a("el-col",{staticClass:"el-col",attrs:{span:7}},[a("el-select",{attrs:{placeholder:"Example SQL",clearable:""},on:{change:function(t){return e.loadSQLFile(t)},clear:e.clearTextarea},model:{value:e.selectedSQL,callback:function(t){e.selectedSQL=t},expression:"selectedSQL"}},e._l(e.SQLFiles,(function(t){return a("el-option",{key:t,attrs:{value:t}},[e._v(" "+e._s(t)+" ")])})),1)],1),a("el-col",{staticClass:"el-col",attrs:{span:2}},[a("el-button",{attrs:{loading:e.isLoading,type:"primary"},on:{click:e.submitSQL}},[e._v("Execute")])],1)],1),a("el-row",{staticClass:"el-row"},[a("el-col",{staticClass:"el-col"},[e.tableData.length>0?a("el-card",{staticClass:"box-card",attrs:{shadow:"hover"}},[e.tableData.length>0?a("el-table",{staticStyle:{width:"100%"},attrs:{data:e.tableData,"highlight-current-row":"","default-sort":{prop:"number"},stripe:""},on:{"current-change":e.handleTableSelect}},[a("el-table-column",{attrs:{prop:"number",label:"Id",sortable:""}}),a("el-table-column",{attrs:{prop:"cost",label:"Cost",sortable:""}}),a("el-table-column",{attrs:{prop:"s_dist",label:"S_dist",sortable:""}}),a("el-table-column",{attrs:{prop:"c_dist",label:"C_dist",sortable:""}}),a("el-table-column",{attrs:{prop:"cost_dist",label:"Cost_dist"}})],1):e._e()],1):e._e()],1)],1)],1),a("el-col",{staticClass:"el-col",attrs:{span:15}},[a("el-card",{directives:[{name:"show",rawName:"v-show",value:null!=e.needToDraw,expression:"needToDraw != null"}],staticClass:"box-card"},[a("draw-tree",{ref:"drawTree"})],1)],1)],1)],1)],1)],1)],1)},h=[],f=function(){var e=this,t=e.$createElement,a=e._self._c||t;return a("div",[a("div",{directives:[{name:"loading",rawName:"v-loading",value:e.g_loading,expression:"g_loading"}],staticStyle:{width:"calc(100% - 10px)",height:"calc(100vh - 140px)"}},[a("SeeksRelationGraph",{ref:"seeksRelationGraph",attrs:{options:e.graphOptions,"on-node-expand":e.onNodeExpand}})],1)])},m=[],b=a("0491"),g=a.n(b),v={name:"DrawTree",components:{SeeksRelationGraph:g.a},data(){return{g_loading:!0,graphOptions:{layouts:[{label:"中心",layoutName:"tree",layoutClassName:"seeks-layout-tree",defaultJunctionPoint:"border",defaultNodeShape:0,defaultLineShape:1,centerOffset_x:-300,centerOffset_y:0,min_per_width:"60",min_per_height:"400"}],defaultExpandHolderPosition:"bottom",defaultLineShape:4,defaultJunctionPoint:"tb",defaultNodeShape:1,defaultNodeWidth:"50",defaultNodeHeight:"250",defaultNodeBorderWidth:0,defaultNodeColor:" #f8e3c5",defaultNodeFontColor:"#606266",defaultLineColor:"#606266"}}},created(){},mounted(){this.setGraphData({rootId:1,nodes:[{id:1,text:"",color:"white"}],links:[]})},methods:{setGraphData(e){var t=e;t.nodes.forEach(e=>{e.width=100,e.height=50}),setTimeout(function(){this.g_loading=!1,this.$refs.seeksRelationGraph.setJsonData(t,e=>{})}.bind(this),1e3)},onNodeExpand(e,t){if(e.data&&!0===e.data.asyncChild&&!1===e.data.loaded)return this.g_loading=!0,e.data.loaded=!0,void setTimeout(function(){this.g_loading=!1;var t={nodes:[{id:e.id+"-child-1",text:e.id+"-的子节点1"},{id:e.id+"-child-2",text:e.id+"-的子节点2"},{id:e.id+"-child-3",text:e.id+"-的子节点3"}],links:[{from:e.id,to:e.id+"-child-1",text:"动态子节点"},{from:e.id,to:e.id+"-child-2",text:"动态子节点"},{from:e.id,to:e.id+"-child-3",text:"动态子节点"}]};this.$refs.seeksRelationGraph.appendJsonData(t,e=>{})}.bind(this),1e3)}}},x=v,w=Object(r["a"])(x,f,m,!1,null,"645eec8a",null),_=w.exports,y={name:"HelloWorld",components:{DrawTree:_},data(){return{dbName:"imdb",tableList:{},SQLstatement:"",selectedSQL:"",SQLFiles:[],isLoading:!1,tableData:[],isComparePlan:!1,bestPlanId:null,bestPlanReason:"",worstPlanId:null,worstPlanReason:"",needToDraw:null,aboutURL:"",helpURL:""}},methods:{getSQLFiles(){console.log(Object({NODE_ENV:"production",VUE_APP_BASE_API:"/prod-api",BASE_URL:"/"}).BASE_API),this.$axios.get("/sqlFile").then(e=>{for(var t in e.data)this.SQLFiles.push(e.data[t]);this.SQLFiles.sort((function(e,t){return Number(e.split(".")[0])-Number(t.split(".")[0])}))}).catch(e=>{console.log("获取sql文件列表失败"),console.log(e),this.SQLFiles=[]})},getDBInfo(){this.$axios.get("/databaseInfo/"+this.dbName).then(e=>{this.tableList=JSON.parse(e.data)}).catch(e=>{console.log("获取数据库信息失败"),console.log(e),this.tableList={"请求数据库失败":{"Cat't":"get dbInfo"}}})},clearTextarea(){this.SQLstatement="",this.tableData=[],this.needToDraw=null},loadSQLFile(e){if(this.selectedSQL.length>0){var t="/sqlFile/"+this.selectedSQL;this.$axios.get(t).then(e=>{this.SQLstatement=e.data}).catch(e=>{console.log("请求sql文件内容失败, 目标sql文件为："+this.selectedSQL),console.log(e),this.SQLstatement="I can't load the sql file ╮(╯-╰)╭"})}},submitSQL(){if(console.log("点击按钮"),""!==this.SQLstatement){var e=new FormData,t=this.SQLstatement.toLowerCase();t=t.replace(/\s+/g," "),e.append("statement",t),e.append("fileName",this.selectedSQL),this.$axios.post("/search",e,{header:{"Content-Type":"application/x-www-form-urlencoded"}}).then(e=>{let t,a;console.log("发送SQL查询请求成功!"),console.log(typeof e.data),console.log(e.data);let l={rootId:null,nodes:[],links:[]};for(let s in e.data){t={number:e.data[s][0]},a=JSON.parse(e.data[s][1]),t.cost=Math.round(a.cost),t.c_dist=a.c_dist.toFixed(2),t.s_dist=a.s_dist.toFixed(2),t.cost_dist=a.cost_dist.toFixed(2);let n=1,i=a.content;i.parent="-1";let o=[i];l.rootId="1";while(o.length>0){let e=o.length;for(let a=0;a<e;a++){let e=o.shift(),a={id:this.selectedSQL+"_"+t.number+"_"+n.toString(),text:e.name};if(n++,l.nodes.push(a),"-1"!==e.parent){let t={from:e.parent,to:a.id};l.links.push(t)}if(e.child_flag)for(let t of e.child)t.parent=a.id,o.push(t)}}console.log("生成的树结构为：",l),t.tree=l,this.tableData.push(t)}}).catch(e=>{console.log("发送SQL查询请求失败!"),console.log(e)})}else this.$message({message:"Please input the SQL!",showClose:!0,type:"error"})},handleTableSelect(e){console.log(e),this.needToDraw=e.tree,this.$refs.drawTree.setGraphData(this.needToDraw)}},mounted(){var e;this.getSQLFiles(),this.getDBInfo(),e=window.location.href.split("/").slice(0,3),this.aboutURL=e.join("/")+"/about",this.helpURL=e.join("/")+"/help"}},S=y,L=(a("5f4e"),a("0156"),Object(r["a"])(S,u,h,!1,null,"5e95876a",null)),C=L.exports,k=function(){var e=this,t=e.$createElement,l=e._self._c||t;return l("div",{staticClass:"about"},[l("el-container",{staticClass:"layout-container"},[l("el-header",{staticClass:"el-header"},[l("div",{staticClass:"title"},[l("span",[e._v("ARENA")])])]),l("el-main",{attrs:{id:"largeFont"}},[l("el-collapse",{model:{value:e.activeNames,callback:function(t){e.activeNames=t},expression:"activeNames"}},[l("el-collapse-item",{attrs:{title:"What is ARENA?",name:"1"}},[l("div",{staticStyle:{"font-size":"16px"}},[l("ul",[l("li",[e._v(" ARENA is a system that helps users learn database optimization knowledge. ")]),l("li",[e._v(" For a SQL, it will return not only the optimal plan whose execution time is smallest but also k valuable alternative plans. ")]),l("li",[e._v(" Users can learn the optimization knowledge used in the optimizer and why the plan is optimal by comparing the difference between alternative plans and the optimal plan. ")])])])]),l("el-collapse-item",{attrs:{title:"How to use it?",name:"2"}},[l("el-collapse",{staticStyle:{"margin-left":"14px"}},[l("div",{attrs:{id:"mediumFont"}},[l("el-collapse-item",{attrs:{title:"Step 1",name:"3"}},[l("el-card",{attrs:{"body-style":{padding:"0px"},shadow:"hover"}},[l("div",{staticStyle:{"text-align":"center","margin-top":"14px"}},[l("img",{staticClass:"image",attrs:{src:a("f33d")}})]),l("div",{staticStyle:{padding:"14px","font-size":"16px"}},[l("span",[e._v('First step, enter a SQL statement or select an SQL instance, then click the "Execute" button.')])])])],1),l("el-collapse-item",{attrs:{title:"Step 2",name:"4"}},[l("el-card",{attrs:{"body-style":{padding:"0px"},shadow:"hover"}},[l("div",{staticStyle:{"text-align":"center","margin-top":"14px"}},[l("img",{staticClass:"image",attrs:{src:a("848d")}})]),l("div",{staticStyle:{padding:"14px","font-size":"16px"}},[l("span",[e._v('Second step, after clicking the "Execute" button and returning successfully, you will get a table like this where each row represents a plan. The plan with id 0 is the best plan and others are alternative plans. The information represented by each column in the table is:')]),l("ul",[l("li",[e._v(" Cost: Execution time of each plan ")]),l("li",[e._v(" S_dist: Structural distance between the current plan and the best plan ")]),l("li",[e._v(" C_dist: The content distance between the current plan and the best plan ")]),l("li",[e._v(" Cost_dist: The distance between the execution time of the current plan and the best plan ")])]),l("span",[e._v(" If you click on a row, the detailed plan tree for that plan will be displayed on the right. ")])])])],1),l("el-collapse-item",{attrs:{title:"Step 3",name:"5"}},[l("el-card",{attrs:{"body-style":{padding:"0px"},shadow:"hover"}},[l("div",{staticStyle:{"text-align":"center","margin-top":"14px"}},[l("img",{staticClass:"image",attrs:{src:a("2aff")}})]),l("div",{staticStyle:{padding:"14px","font-size":"16px"}},[l("span",[e._v("Third step, the sidebar displays some additional information and provides some control options.")]),l("ul",[l("li",[e._v(" Database Information: This section shows the details of the database, what tables it contains, and the fields of each table. ")]),l("li",[l("b",[e._v(" Feedback: This section is used to collect the information we need. We hope that for each SQL query, you can choose two plans from the alternative plans. One is the plan you think is the most valuable, and the other is the least valuable. We would also like you to tell us why you chose these two plans. (P.S. A plan is valuable if you can learn something through the difference between it and the best plan. If you can't learn anything, it's worthless.) ")])]),l("li",[e._v(' Setting: There is a switch called "Compare Plan". If you turn it on, when you click on the table in Step 2, a graph of the difference between the best plan and the current plan will be drawn. ')])])])])],1)],1)])],1),l("el-collapse-item",{attrs:{title:"Other questions?",name:"6"}},[l("div",{staticStyle:{"font-size":"16px","margin-left":"14px"}},[e._v(" Please contact "),l("i",[e._v("wh2mail@163.com")]),e._v(" . ")])])],1)],1)],1)],1)},P=[],Q={name:"About",data(){return{activeNames:""}}},N=Q,T=(a("9540"),a("4d1a"),Object(r["a"])(N,k,P,!1,null,"42c55e52",null)),D=T.exports;l["default"].use(p["a"]);var I=new p["a"]({mode:"hash",routes:[{path:"/",name:"HelloWorld",component:C},{path:"/about",name:"about",component:D}]}),O=a("5c96"),R=a.n(O),E=(a("0fae"),a("bc3a")),F=a.n(E);l["default"].config.productionTip=!1,l["default"].use(R.a),F.a.defaults.baseURL="http://82.157.149.11:6001/",l["default"].prototype.$axios=F.a,new l["default"]({el:"#app",router:I,components:{App:d},template:"<App/>"})},"5f4e":function(e,t,a){"use strict";a("fe12")},"848d":function(e,t,a){e.exports=a.p+"static/img/step2.d8961ff7.jpg"},"85ec":function(e,t,a){},9540:function(e,t,a){"use strict";a("dfb1")},a82f:function(e,t,a){},dfb1:function(e,t,a){},f33d:function(e,t,a){e.exports=a.p+"static/img/step1.d08d3282.jpg"},fa7c:function(e,t,a){},fe12:function(e,t,a){}});
//# sourceMappingURL=app.d2e24d71.js.map