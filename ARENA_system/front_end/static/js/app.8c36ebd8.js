(function(e){function t(t){for(var s,l,n=t[0],r=t[1],c=t[2],h=0,p=[];h<n.length;h++)l=n[h],Object.prototype.hasOwnProperty.call(i,l)&&i[l]&&p.push(i[l][0]),i[l]=0;for(s in r)Object.prototype.hasOwnProperty.call(r,s)&&(e[s]=r[s]);d&&d(t);while(p.length)p.shift()();return o.push.apply(o,c||[]),a()}function a(){for(var e,t=0;t<o.length;t++){for(var a=o[t],s=!0,n=1;n<a.length;n++){var r=a[n];0!==i[r]&&(s=!1)}s&&(o.splice(t--,1),e=l(l.s=a[0]))}return e}var s={},i={app:0},o=[];function l(t){if(s[t])return s[t].exports;var a=s[t]={i:t,l:!1,exports:{}};return e[t].call(a.exports,a,a.exports,l),a.l=!0,a.exports}l.m=e,l.c=s,l.d=function(e,t,a){l.o(e,t)||Object.defineProperty(e,t,{enumerable:!0,get:a})},l.r=function(e){"undefined"!==typeof Symbol&&Symbol.toStringTag&&Object.defineProperty(e,Symbol.toStringTag,{value:"Module"}),Object.defineProperty(e,"__esModule",{value:!0})},l.t=function(e,t){if(1&t&&(e=l(e)),8&t)return e;if(4&t&&"object"===typeof e&&e&&e.__esModule)return e;var a=Object.create(null);if(l.r(a),Object.defineProperty(a,"default",{enumerable:!0,value:e}),2&t&&"string"!=typeof e)for(var s in e)l.d(a,s,function(t){return e[t]}.bind(null,s));return a},l.n=function(e){var t=e&&e.__esModule?function(){return e["default"]}:function(){return e};return l.d(t,"a",t),t},l.o=function(e,t){return Object.prototype.hasOwnProperty.call(e,t)},l.p="/";var n=window["webpackJsonp"]=window["webpackJsonp"]||[],r=n.push.bind(n);n.push=t,n=n.slice();for(var c=0;c<n.length;c++)t(n[c]);var d=r;o.push([0,"chunk-vendors"]),a()})({0:function(e,t,a){e.exports=a("56d7")},"0156":function(e,t,a){"use strict";a("fa7c")},"034f":function(e,t,a){"use strict";a("85ec")},"05f6":function(e,t,a){e.exports=a.p+"static/img/step4.eccf60c9.jpg"},"0ab6":function(e,t,a){},"2aff":function(e,t,a){e.exports=a.p+"static/img/step3.5092fd68.jpg"},"2b44":function(e,t,a){},3203:function(e,t,a){"use strict";a("90c7")},"4d1a":function(e,t,a){"use strict";a("a82f")},"56d7":function(e,t,a){"use strict";a.r(t);var s=a("a026"),i=function(){var e=this,t=e.$createElement,a=e._self._c||t;return a("div",{attrs:{id:"app"}},[a("router-view")],1)},o=[],l={name:"App"},n=l,r=(a("034f"),a("2877")),c=Object(r["a"])(n,i,o,!1,null,null,null),d=c.exports,h=a("8c4f"),p=function(){var e=this,t=e.$createElement,a=e._self._c||t;return a("div",{staticClass:"hello"},[a("el-container",{staticClass:"layout-container"},[a("el-header",{staticClass:"el-header"},[a("div",{staticClass:"title"},[a("span",[e._v("ARENA")])])]),a("el-container",[a("el-aside",{staticStyle:{height:"90vh"},attrs:{width:"300px"}},[a("el-scrollbar",{staticStyle:{height:"100%"},attrs:{wrapStyle:"overflow-x:hidden;"}},[a("el-menu",{attrs:{"active-text-color":"#409EFF"}},[a("el-submenu",{attrs:{index:"1"},scopedSlots:e._u([{key:"title",fn:function(){return[a("i",{staticClass:"el-icon-s-data"}),a("span",[e._v("Database Information")])]},proxy:!0}])},e._l(e.tableList,(function(t,s){return a("el-submenu",{key:s,attrs:{index:s},scopedSlots:e._u([{key:"title",fn:function(){return[e._v(e._s(s))]},proxy:!0}],null,!0)},e._l(t,(function(t,s){return a("el-menu-item",{key:s,attrs:{index:s}},[e._v(" "+e._s(s)+" ("+e._s(t)+") ")])})),1)})),1),a("el-submenu",{attrs:{index:"4"},scopedSlots:e._u([{key:"title",fn:function(){return[a("i",{staticClass:"el-icon-s-promotion"}),a("span",[e._v("Feedback")])]},proxy:!0}])},[a("el-menu-item-group",{staticClass:"feedbackGroup"},[a("template",{slot:"title"},[e._v("The most valuable plan")]),a("el-menu-item",{attrs:{index:"4-1-1"}},[a("el-input",{staticStyle:{"font-size":"13px"},attrs:{placeholder:"plan id",size:"mini"},model:{value:e.bestPlanId,callback:function(t){e.bestPlanId=t},expression:"bestPlanId"}},[a("template",{slot:"prepend"},[e._v("Plan")])],2)],1),a("el-menu-item",{staticClass:"noHighLight",attrs:{index:"4-1-2"}},[a("el-input",{staticStyle:{"font-size":"13px"},attrs:{type:"textarea",rows:5,placeholder:"Please input the reason."},model:{value:e.bestPlanReason,callback:function(t){e.bestPlanReason=t},expression:"bestPlanReason"}})],1)],2),a("el-menu-item-group",{staticClass:"feedbackGroup"},[a("template",{slot:"title"},[e._v("The least valuable plan")]),a("el-menu-item",{attrs:{index:"4-2-1"}},[a("el-input",{staticStyle:{"font-size":"13px"},attrs:{placeholder:"plan id",size:"mini"},model:{value:e.worstPlanId,callback:function(t){e.worstPlanId=t},expression:"worstPlanId"}},[a("template",{slot:"prepend"},[e._v("Plan")])],2)],1),a("el-menu-item",{staticClass:"noHighLight",attrs:{index:"4-2-2"}},[a("el-input",{staticStyle:{"font-size":"13px"},attrs:{type:"textarea",rows:5,placeholder:"Please input the reason."},model:{value:e.worstPlanReason,callback:function(t){e.worstPlanReason=t},expression:"worstPlanReason"}})],1)],2),a("el-menu-item-group",[a("template",{slot:"title"},[e._v("Submit your feedback")]),a("el-menu-item",{attrs:{index:"4-3-1"}},[a("el-button",{attrs:{size:"mini"},on:{click:e.submitFeedback}},[e._v("Submit")])],1)],2)],1),a("el-submenu",{attrs:{index:"2"},scopedSlots:e._u([{key:"title",fn:function(){return[a("i",{staticClass:"el-icon-s-tools"}),a("span",[e._v("Setting")])]},proxy:!0}])},[a("el-menu-item",{attrs:{index:"2-1"}},[a("el-switch",{attrs:{"active-text":"Compare Plan"},model:{value:e.isComparePlan,callback:function(t){e.isComparePlan=t},expression:"isComparePlan"}})],1),a("el-menu-item",{attrs:{index:"2-1"}},[a("el-switch",{attrs:{"active-text":"Different Color"},model:{value:e.isDifferentColor,callback:function(t){e.isDifferentColor=t},expression:"isDifferentColor"}})],1)],1),a("el-submenu",{attrs:{index:"5"},scopedSlots:e._u([{key:"title",fn:function(){return[a("i",{staticClass:"el-icon-s-comment"}),a("span",[e._v("Suggest")])]},proxy:!0}])},[a("el-menu-item-group",{staticStyle:{height:"130px"}},[a("el-menu-item",{staticClass:"noHighLight",attrs:{index:"5-1"}},[a("el-input",{staticStyle:{"font-size":"13px"},attrs:{type:"textarea",rows:6,placeholder:"Please input your suggestion."},model:{value:e.userSuggestion,callback:function(t){e.userSuggestion=t},expression:"userSuggestion"}})],1)],1),a("el-menu-item-group",[a("el-menu-item",{attrs:{index:"5-2"}},[a("el-button",{attrs:{size:"mini"},on:{click:e.submitSuggest}},[e._v("Submit")])],1)],1)],1),a("el-submenu",{attrs:{index:"3"},scopedSlots:e._u([{key:"title",fn:function(){return[a("i",{staticClass:"el-icon-question"}),a("span",[e._v("Help")])]},proxy:!0}])},[a("el-menu-item",{attrs:{index:"3-1"}},[a("router-link",{attrs:{tag:"a",target:"_blank",to:"about"}},[e._v(" About ")])],1)],1)],1)],1)],1),a("el-main",[a("el-row",{staticClass:"el-row",attrs:{gutter:20}},[a("el-col",{staticClass:"el-col",attrs:{span:9}},[a("el-row",{staticClass:"el-row"},[a("el-input",{attrs:{type:"textarea",placeholder:"Input your SQL or select an example SQL",autosize:{minRows:10,maxRows:20}},model:{value:e.SQLstatement,callback:function(t){e.SQLstatement=t},expression:"SQLstatement"}})],1),a("el-row",{staticClass:"el-row",attrs:{gutter:10}},[a("el-col",{staticClass:"el-col",attrs:{span:7}},[a("el-select",{attrs:{placeholder:"Example SQL",clearable:""},on:{change:function(t){return e.loadSQLFile(t)},clear:e.clearTextarea},model:{value:e.selectedSQL,callback:function(t){e.selectedSQL=t},expression:"selectedSQL"}},e._l(e.SQLFiles,(function(t){return a("el-option",{key:t,attrs:{value:t}},[e._v(" "+e._s(t)+" ")])})),1)],1),a("el-col",{staticClass:"el-col",attrs:{span:2}},[a("el-button",{attrs:{loading:e.isLoading,type:"primary"},on:{click:e.submitSQL}},[e._v("Execute")])],1)],1),a("el-row",{staticClass:"el-row"},[a("el-col",{staticClass:"el-col"},[e.tableData.length>0?a("el-card",{staticClass:"box-card",attrs:{shadow:"hover"}},[e.tableData.length>0?a("el-table",{staticStyle:{width:"100%"},attrs:{data:e.tableData,"highlight-current-row":"","default-sort":{prop:"number"},stripe:""},on:{"current-change":e.handleTableSelect}},[a("el-table-column",{attrs:{prop:"number",label:"Id",sortable:""}}),a("el-table-column",{attrs:{prop:"cost",label:"Cost",sortable:""}}),a("el-table-column",{attrs:{prop:"s_dist",label:"S_dist",sortable:""}}),a("el-table-column",{attrs:{prop:"c_dist",label:"C_dist",sortable:""}}),a("el-table-column",{attrs:{prop:"cost_dist",label:"Cost_dist"}})],1):e._e()],1):e._e()],1)],1)],1),a("el-col",{staticClass:"el-col",attrs:{span:15}},[a("el-card",{directives:[{name:"show",rawName:"v-show",value:null!=e.needToDraw,expression:"needToDraw != null"}],staticClass:"box-card"},[a("draw-tree",{ref:"drawTree"})],1)],1)],1)],1)],1)],1)],1)},u=[],f=function(){var e=this,t=e.$createElement,a=e._self._c||t;return a("div",[a("div",{directives:[{name:"loading",rawName:"v-loading",value:e.g_loading,expression:"g_loading"}],staticStyle:{width:"calc(100% - 10px)",height:"calc(100vh - 140px)"}},[a("SeeksRelationGraph",{ref:"seeksRelationGraph",attrs:{options:e.graphOptions}})],1)])},m=[],g=a("0491"),b=a.n(g),v={name:"DrawTree",components:{SeeksRelationGraph:b.a},data(){return{g_loading:!0,graphOptions:{layouts:[{label:"中心",layoutName:"tree",defaultJunctionPoint:"border",defaultNodeShape:0,defaultLineShape:1,centerOffset_y:0,min_per_width:"60",min_per_height:"150",max_per_height:"300"}],defaultExpandHolderPosition:"bottom",defaultLineShape:4,defaultJunctionPoint:"tb",defaultNodeShape:1,defaultNodeWidth:"50",defaultNodeHeight:"250",defaultNodeBorderWidth:0,defaultNodeColor:"#f8e3c5",defaultNodeFontColor:"#606266",defaultLineColor:"#606266",moveToCenterWhenResize:!1,allowShowMiniNameFilter:!1}}},created(){},mounted(){this.setGraphData({rootId:1,nodes:[{id:1,text:"",color:"white",width:100,height:50}],links:[]},!1)},methods:{setGraphData(e,t){this.setDiffColor(e,t),setTimeout(function(){this.g_loading=!1,this.$refs.seeksRelationGraph.setJsonData(e,e=>{})}.bind(this),1e3)}}},S=v,w=Object(r["a"])(S,f,m,!1,null,"1b279b5c",null),y=w.exports,x={name:"HelloWorld",components:{DrawTree:y},data(){return{dbName:"school",tableList:{},SQLstatement:"",selectedSQL:"",SQLFiles:[],isLoading:!1,tableData:[],isComparePlan:!1,isDifferentColor:!0,bestPlanId:"",bestPlanReason:"",worstPlanId:"",worstPlanReason:"",userSuggestion:"",needToDraw:null}},methods:{getSQLFiles(){console.log(Object({NODE_ENV:"production",VUE_APP_BASE_API:"/prod-api",BASE_URL:"/"}).BASE_API),this.$axios.get("/sqlFile").then(e=>{for(var t in e.data)this.SQLFiles.push(e.data[t]);this.SQLFiles.sort((function(e,t){return Number(e.split(".")[0])-Number(t.split(".")[0])}))}).catch(e=>{console.log("获取sql文件列表失败"),console.log(e),this.SQLFiles=[]})},getDBInfo(){this.$axios.get("/databaseInfo/"+this.dbName).then(e=>{this.tableList=JSON.parse(e.data)}).catch(e=>{console.log("获取数据库信息失败"),console.log(e),this.tableList={"请求数据库失败":{"Cat't":"get dbInfo"}}})},getCookie(e){let t=document.cookie,a=t.split("; ");for(let s of a){let t=s.split("=");if(t[0]===e)return t[1]}return""},clearTextarea(){this.SQLstatement="",this.tableData=[],this.needToDraw=null},loadSQLFile(e){if(this.selectedSQL.length>0){var t="/sqlFile/"+this.selectedSQL;this.$axios.get(t).then(e=>{this.SQLstatement=e.data}).catch(e=>{console.log("请求sql文件内容失败, 目标sql文件为："+this.selectedSQL),console.log(e),this.SQLstatement="I can't load the sql file ╮(╯-╰)╭"}),this.tableData=[],this.needToDraw=null}},submitSQL(){if(console.log("点击按钮"),""===this.SQLstatement)return void this.$message({message:"Please input the SQL!",showClose:!0,type:"error"});this.isLoading=!0;let e=new FormData,t=this.SQLstatement.toLowerCase(),a=t.split("\n");for(let s=0;s<a.length;s++){let e=a[s],t=e.indexOf("--");-1!==t&&(e=e.substring(0,t)),a[s]=e}t=a.join("\n"),t=t.replace(/\s+/g," "),e.append("statement",t),e.append("fileName",this.selectedSQL),this.$axios.post("/search",e,{header:{"Content-Type":"application/x-www-form-urlencoded"}}).then(e=>{let t,a;console.log("发送SQL查询请求成功!"),console.log(typeof e.data),console.log(e.data);for(let s in e.data){let i={rootId:null,nodes:[],links:[],TABLE:{},SCAN:{},JOIN:{},diffInfo:null};t={number:e.data[s][0]},a=JSON.parse(e.data[s][1]),t.cost=a.cost.toFixed(1),t.c_dist=a.c_dist.toFixed(2),t.s_dist=a.s_dist.toFixed(2),t.cost_dist=a.cost_dist.toFixed(2),t.workerType=a.workerType,localStorage[t.number]=t.workerType;let o=1,l=a.content;l.parent="-1";let n=[l],r=(new Date).getTime().toString();i.rootId=this.selectedSQL+"_"+t.number+"_1_"+r;while(n.length>0){let e=n.length;for(let a=0;a<e;a++){let e=n.shift(),a={id:this.selectedSQL+"_"+t.number+"_"+o.toString()+"_"+e.name+"_"+r,text:e.name+"\nCost:"+e.cost.toFixed(1),width:100,height:50};if(e.id=a.id,"TABLE"===e.tag?(i.TABLE[a.id]=!0,a.text=e.name):"SCAN"===e.tag?i.SCAN[a.id]=!0:"JOIN"===e.tag&&(i.JOIN[a.id]=!0),o++,i.nodes.push(a),"-1"!==e.parent){let t={from:e.parent,to:a.id};i.links.push(t)}if(e.child_flag)for(let t of e.child)t.parent=a.id,n.push(t)}}i.diffInfo=this.generateDiffInfo(a.content),console.log("生成的树结构为：",i),t.tree=i,this.tableData.push(t)}this.isLoading=!1}).catch(e=>{this.$message({message:"SQL Execution Failed",showClose:!0,type:"error"}),this.isLoading=!1,console.log("发送SQL查询请求失败!"),console.log(e)})},submitFeedback(){if(0===this.bestPlanId.length||0===this.bestPlanReason.length||0===this.worstPlanId.length||0===this.worstPlanReason.length)return void this.$message({message:"Please Complete the Information",showClose:!0,type:"error"});if(""===this.selectedSQL)return void this.$message({message:"Please execute a SQL!",showClose:!0,type:"error"});let e=new FormData;e.append("uid",this.getCookie("uid")),e.append("fileName",this.selectedSQL),e.append("bestPlanId",this.bestPlanId),e.append("bestPlanReason",this.bestPlanReason),e.append("worstPlanId",this.worstPlanId),e.append("worstPlanReason",this.worstPlanReason),this.$axios.post("/feedback",e,{header:{"Content-Type":"application/x-www-form-urlencoded"}}).then(()=>{console.log("发送反馈信息成功"),this.$message({message:"thank you for your feedback!",showClose:!0,type:"success"}),this.bestPlanId="",this.bestPlanReason="",this.worstPlanId="",this.worstPlanReason="",this.isLoading=!1}).catch(e=>{this.$message({message:"Can't send the feedback.",showClose:!0,type:"error"}),console.log("发送反馈失败!"),console.log(e)})},submitSuggest(){if(""===this.userSuggestion)return void this.$message({message:"suggestion is empty!",showClose:!0,type:"warning"});let e=new FormData;e.append("uid",this.getCookie("uid")),e.append("suggestion",this.userSuggestion),this.$axios.post("/suggest",e,{header:{"Content-Type":"application/x-www-form-urlencoded"}}).then(()=>{this.$message({message:"thank you for your suggestion!",showClose:!0,type:"success"}),this.userSuggestion=""}).catch(e=>{this.$message({message:"Can't send the suggestion.",showClose:!0,type:"error"}),console.log("发送建议失败!"),console.log(e)})},handleTableSelect(e){if(!1===this.isComparePlan){console.log("选择的数据为：",e),this.needToDraw=e.tree;for(let e=0;e<this.needToDraw.nodes.length;e++)delete this.needToDraw.nodes[e].flated;this.$refs.drawTree.setGraphData(this.needToDraw,this.isDifferentColor)}else{let t=(new Date).getTime().toString()+"_"+this.selectedSQL+"_"+e.number,a=null;for(let e of this.tableData)0===e.number&&(a=e);let s=JSON.stringify([a.tree,e.tree]);localStorage.setItem(t,s),localStorage.setItem(t+"diffColor",this.isDifferentColor);let i=this.$router.resolve({path:"/compareTree",query:{index:t}});window.open(i.href)}},generateDiffInfo(e){let t=this.generateScanInfo(e),a={};for(let l of t)for(let e in l)a[e]=l[e];let s=this.generateJoinTree(e),i=this.generateJoinInfo(s),o={};return o["scanInfo"]=a,o["joinInfo"]=i,o},generateScanInfo(e){let t=e;if(0===Object.keys(t).length)return[];if("SCAN"===t.tag){let e=t.child,a={};return a[e[0].name]=[t.name,e[0].id],[a]}let a=[];for(let s of t.child){let e=this.generateScanInfo(s);e.length>0&&(a=a.concat(e))}return a},generateJoinInfo(e){let t={level:{},map:{}},a=1,s=[e];while(0!==s.length){let e=s.length;t.level[a]=[];for(let i=0;i<e;i++){let e=s.shift();if("JOIN"===e.tag){t.level[a].push(e.id);let i=[];for(let t of e.child)i.push(t.id),s.push(t);t.map[e.id]=i}}a++}a--,delete t.level[a];for(let i=1,o=a-1;i<o;i++,o--){let e=t.level[i];t.level[i]=t.level[o],t.level[o]=e}return t},generateJoinTree(e){let t=e;if(0===Object.keys(t).length)return{};if("TABLE"===t.tag)return{tag:t.tag,id:t.id,child:[]};if("JOIN"===t.tag){let e={tag:t.tag,id:t.id,child:[]};for(let a of t.child){let t=this.generateJoinTree(a);Object.keys(t).length>0&&e.child.push(t)}return e}return t.child.length>1?(this.$message({message:"dfsTree Wrong: "+t.name+"有两个子节点",showClose:!0,type:"error"}),{}):this.generateJoinTree(t.child[0])}},mounted(){this.getSQLFiles(),this.getDBInfo()}},_=x,C=(a("874a"),a("0156"),Object(r["a"])(_,p,u,!1,null,"012e83e2",null)),k=C.exports,I=function(){var e=this,t=e.$createElement,s=e._self._c||t;return s("div",{staticClass:"about"},[s("el-container",{staticClass:"layout-container"},[s("el-header",{staticClass:"el-header"},[s("div",{staticClass:"title"},[s("span",[e._v("ARENA")])])]),s("el-main",{attrs:{id:"largeFont"}},[s("el-collapse",{model:{value:e.activeNames,callback:function(t){e.activeNames=t},expression:"activeNames"}},[s("el-collapse-item",{attrs:{title:"What is ARENA?",name:"1"}},[s("div",{staticStyle:{"font-size":"16px"}},[s("ul",[s("li",[e._v(" ARENA is a system that helps users learn database optimization knowledge. ")]),s("li",[e._v(" For a SQL, it will return not only the optimal plan whose execution time is smallest but also k valuable alternative plans. ")]),s("li",[e._v(" Users can learn the optimization knowledge used in the optimizer and why the plan is optimal by comparing the difference between alternative plans and the optimal plan. ")])])])]),s("el-collapse-item",{attrs:{title:"How to use it?",name:"2"}},[s("el-collapse",{staticStyle:{"margin-left":"14px"},attrs:{accordion:""}},[s("div",{attrs:{id:"mediumFont"}},[s("el-collapse-item",{attrs:{title:"Step 1",name:"3"}},[s("el-card",{attrs:{"body-style":{padding:"0px"},shadow:"hover"}},[s("div",{staticStyle:{"text-align":"center","margin-top":"14px"}},[s("img",{staticClass:"image",attrs:{src:a("f33d")}})]),s("div",{staticStyle:{padding:"14px","font-size":"16px"}},[s("span",[e._v('First step, enter a SQL statement or select an SQL instance, then click the "Execute" button.')])])])],1),s("el-collapse-item",{attrs:{title:"Step 2",name:"4"}},[s("el-card",{attrs:{"body-style":{padding:"0px"},shadow:"hover"}},[s("div",{staticStyle:{"text-align":"center","margin-top":"14px"}},[s("img",{staticClass:"image",attrs:{src:a("848d")}})]),s("div",{staticStyle:{padding:"14px","font-size":"16px"}},[s("span",[e._v('Second step, after clicking the "Execute" button and returning successfully, you will get a table like this where each row represents a plan. The plan with id 0 is the best plan and others are alternative plans. The information represented by each column in the table is:')]),s("ul",[s("li",[e._v(" Cost: Execution time of each plan ")]),s("li",[e._v(" S_dist: Structural distance between the current plan and the best plan ")]),s("li",[e._v(" C_dist: The content distance between the current plan and the best plan ")]),s("li",[e._v(" Cost_dist: The distance between the execution time of the current plan and the best plan ")])]),s("span",[e._v(" If you click on a row, the detailed plan tree for that plan will be displayed on the right. ")])])])],1),s("el-collapse-item",{attrs:{title:"Step 3",name:"5"}},[s("el-card",{attrs:{"body-style":{padding:"0px"},shadow:"hover"}},[s("div",{staticStyle:{"text-align":"center","margin-top":"14px"}},[s("img",{staticClass:"image",attrs:{src:a("2aff")}})]),s("div",{staticStyle:{padding:"14px","font-size":"16px"}},[s("span",[e._v("Third step, the sidebar displays some additional information and provides some control options.")]),s("ul",[s("li",[e._v(" Database Information: This section shows the details of the database, what tables it contains, and the fields of each table. ")]),s("li",[s("b",[e._v(' Feedback: This section is used to collect the information we need. We hope that for "5.sql" and "6.sql", you can choose two plans from the alternative plans. One is the plan you think is the most valuable, and the other is the least valuable. We would also like you to tell us why you chose these two plans. (P.S. A plan is valuable if you can learn something through the difference between it and the best plan. If you can\'t learn anything, it\'s worthless.) ')])]),s("li",[e._v(" Setting: "),s("ul",[s("li",[e._v(" Compare Plan: If you turn it on, when you click on the table in Step 2, a graph of the difference between the best plan and the current plan will be drawn. ")]),s("li",[e._v(" Different Color: whether to use different colors to represent different operators in the plan tree. ")])])]),s("li",[e._v(" Suggest: If you have any suggestions for ARENA, whether it's the features or the UI, you can send it to us through this section. ")])])])])],1)],1)])],1),s("el-collapse-item",{attrs:{title:"Info about the database?",name:"7"}},[s("div",{staticStyle:{"font-size":"16px","margin-left":"14px"}},[e._v(" For more information about the database, please refer to the "),s("a",{attrs:{href:"https://www.db-book.com/university-lab-dir/sample_tables-dir/index.html",target:"_blank"}},[e._v("link")]),e._v(". ")])]),s("el-collapse-item",{attrs:{title:"Other questions?",name:"6"}},[s("div",{staticStyle:{"font-size":"16px","margin-left":"14px"}},[e._v(" Please contact "),s("i",[e._v("wh2mail@163.com")]),e._v(" . ")])])],1)],1)],1)],1)},T=[],N={name:"About",data(){return{activeNames:""}}},D=N,P=(a("3203"),a("4d1a"),Object(r["a"])(D,I,T,!1,null,"5785d162",null)),L=P.exports,F=function(){var e=this,t=e.$createElement,a=e._self._c||t;return a("div",{attrs:{id:"compareTree"}},[a("el-container",{staticClass:"layout-container"},[a("el-header",{staticClass:"el-header"},[a("div",{staticClass:"title"},[a("span",[e._v("ARENA")])]),a("div",{staticClass:"compareTreeMenu"},[a("el-menu",{staticClass:"el-menu",attrs:{"background-color":"#C6E2FF","active-text-color":"#606266",mode:"horizontal"}},[a("el-submenu",{attrs:{index:"1"}},[a("template",{slot:"title"},[e._v("Setting")]),a("el-menu-item",{staticClass:"compareTreeMenu",attrs:{index:"1-1"}},[a("el-switch",{attrs:{"active-text":"Show Difference Info"},model:{value:e.showDiff,callback:function(t){e.showDiff=t},expression:"showDiff"}})],1),a("el-menu-item",{staticClass:"compareTreeMenu",attrs:{index:"1-1"}},[a("el-switch",{attrs:{"active-text":"Show ToolTip"},model:{value:e.showToolTip,callback:function(t){e.showToolTip=t},expression:"showToolTip"}})],1)],2)],1)],1)]),a("el-container",[a("el-aside",{directives:[{name:"show",rawName:"v-show",value:e.showDiff,expression:"showDiff"}],staticStyle:{height:"90vh"},attrs:{width:"300px"}},[a("el-collapse",{staticClass:"my-collapse"},[a("el-collapse-item",{attrs:{title:"SCAN DIFFERENCE",name:"1"}},[e.isScanDiff?e._e():a("div",[e._v(" The scan operation of tables is same. ")]),e.isScanDiff?a("div",{staticClass:"diffTable"},[a("el-tooltip",{staticClass:"item",attrs:{effect:"dark",content:"Click to locate the operator",placement:"top","hide-after":"2000",disabled:!e.showToolTip}},[a("el-table",{staticStyle:{width:"100%"},attrs:{data:e.scanTableData,"highlight-current-row":"","default-sort":{prop:"name"}},on:{"row-click":e.tableRowSelect}},[a("el-table-column",{attrs:{prop:"name",label:"Name"}}),a("el-table-column",{attrs:{prop:"qep",label:"QEP"}}),a("el-table-column",{attrs:{prop:"ap",label:"AP"}})],1)],1)],1):e._e()]),e.isJoinDiff?a("el-collapse-item",{attrs:{title:"JOIN DIFFERENCE",name:"2"}},[1===e.isJoinDiff?a("div",[e._v(" The join operation of tables is same. ")]):e._e(),2===e.isJoinDiff?a("div",[e._v(" The join tree structure is different. ")]):e._e(),3===e.isJoinDiff?a("div",{staticClass:"diffTable"},[a("el-tooltip",{staticClass:"item",attrs:{effect:"dark",placement:"top","hide-after":"4000",disabled:!e.showToolTip}},[a("div",{attrs:{slot:"content"},slot:"content"},[e._v("Click '>' to show more info. "),a("br"),a("br"),e._v(" Click row to locate the operator.")]),a("el-table",{staticStyle:{width:"100%"},attrs:{data:e.joinTableData,"highlight-current-row":"","default-sort":{prop:"order"}},on:{"row-click":e.tableRowSelect}},[a("el-table-column",{attrs:{type:"expand"},scopedSlots:e._u([{key:"default",fn:function(t){return[a("el-form",{staticClass:"demo-table-expand",attrs:{"label-position":"left",inline:""}},[a("el-form-item",{attrs:{label:"QEP"}},[a("span",[e._v(e._s(t.row.qep))])]),a("el-form-item",{attrs:{label:"AP"}},[a("span",[e._v(e._s(t.row.ap))])])],1)]}}],null,!1,2802590729)}),a("el-table-column",{attrs:{prop:"order",label:"Order"}}),a("el-table-column",{attrs:{prop:"message",label:"Description",width:"170px"}})],1)],1)],1):e._e()]):e._e()],1)],1),a("el-main",[a("el-row",{attrs:{gutter:20}},[a("el-col",{attrs:{span:12}},[a("el-card",[a("div",[a("div",{staticStyle:{width:"calc(100% - 10px)",height:"calc(100vh - 140px)"}},[a("SeeksRelationGraph",{ref:"bestRelationGraph",attrs:{options:e.graphOptions}})],1)]),a("div",{staticStyle:{"text-align":"center","font-size":"large",color:"#409EFF"}},[e._v(" Best Plan (QEP) ")])])],1),a("el-col",{attrs:{span:12}},[a("el-card",[a("div",[a("div",{staticStyle:{width:"calc(100% - 10px)",height:"calc(100vh - 140px)"}},[a("SeeksRelationGraph",{ref:"selectedRelationGraph",attrs:{options:e.graphOptions}})],1)]),a("div",{staticStyle:{"text-align":"center","font-size":"large",color:"#409EFF"}},[e._v(" Alternative Plan (AP): "+e._s(e.planId)+" ")])])],1)],1)],1)],1)],1)],1)},A=[],E={name:"CompareTree",components:{SeeksRelationGraph:b.a},data(){return{data_index:this.$route.query.index,data_buffer:null,planId:null,showDiff:!0,showToolTip:!0,isScanDiff:!1,isJoinDiff:1,scanTableData:[],joinTableData:[],graphOptions:{layouts:[{label:"中心",layoutName:"tree",defaultJunctionPoint:"border",defaultNodeShape:0,defaultLineShape:1,centerOffset_y:0,min_per_width:"60",min_per_height:"150",max_per_height:"300"}],defaultExpandHolderPosition:"bottom",defaultLineShape:4,defaultJunctionPoint:"tb",defaultNodeShape:1,defaultNodeWidth:"50",defaultNodeHeight:"250",defaultNodeBorderWidth:0,defaultNodeColor:"#f8e3c5",defaultNodeFontColor:"#606266",defaultLineColor:"#606266",moveToCenterWhenResize:!1,allowShowMiniNameFilter:!1}}},mounted(){let e;null==this.data_buffer&&(this.data_buffer=JSON.parse(localStorage[this.data_index]),e=localStorage[this.data_index+"diffColor"],e="true"===e,localStorage.removeItem(this.data_index),localStorage.removeItem(this.data_index+"diffColor"));let t=this.data_index.split("_");this.planId=t[t.length-1],this.setDiffInfo(this.data_buffer[0],this.data_buffer[1]),this.setDiffColor(this.data_buffer[0],e),this.setDiffColor(this.data_buffer[1],e),setTimeout(function(){this.$refs.bestRelationGraph.setJsonData(this.data_buffer[0],e=>{})}.bind(this),1e3),setTimeout(function(){this.$refs.selectedRelationGraph.setJsonData(this.data_buffer[1],e=>{})}.bind(this),1e3)},methods:{clearStorage(){localStorage.clear()},tableRowSelect(e){this.$refs.bestRelationGraph.focusNodeById(e["qepId"]),this.$refs.selectedRelationGraph.focusNodeById(e["apId"])},setDiffInfo(e,t){let a=e.diffInfo.scanInfo,s=t.diffInfo.scanInfo;for(let c in a)a[c][0]!==s[c][0]&&(this.isScanDiff=!0,this.scanTableData.push({name:c,qep:a[c][0],ap:s[c][0],qepId:a[c][1],apId:s[c][1]}));let i={},o=e.diffInfo.joinInfo.level,l=e.diffInfo.joinInfo.map,n=t.diffInfo.joinInfo.level,r=t.diffInfo.joinInfo.map;if(0!==Object.keys(o).length)if(Object.keys(o).length===Object.keys(n).length){for(let e in o)if(o[e].length!==n[e].length)return void(this.isJoinDiff=2);for(let e in o){let t=o[e],a=n[e];for(let s=0;s<t.length;s++){console.log(e,s);let o,n,c,d,h=t[s],p=a[s],u=l[h][0],f=l[h][1],m=r[p][0],g=r[p][1];if(console.log(u),u=this.getChildTable(u,i),f=this.getChildTable(f,i),m=this.getChildTable(m,i),g=this.getChildTable(g,i),i[h]={left:u,right:f},i[p]={left:m,right:g},o=e+"-"+(s+1),c=this.getNameFromId(h)+": ",d=this.getNameFromId(p)+": ",this.eqSet(u,m)&&this.eqSet(f,g)||this.eqSet(u,g)&&this.eqSet(f,m))if(this.eqSet(u,m)){if(c===d)continue;n="different join operator"}else n="different join order";else n="joined table is different";c=c+this.printSet(u)+" X "+this.printSet(f),d=d+this.printSet(m)+" X "+this.printSet(g),this.isJoinDiff=3,this.joinTableData.push({order:o,message:n,qep:c,ap:d,qepId:h,apId:p})}}}else this.isJoinDiff=2},eqSet(e,t){if(e.size!==t.size)return!1;for(let a of e)if(!t.has(a))return!1;return!0},printSet(e){let t="[ ";for(let a of e)t+=a,t+=" ";return t+="]",t},getNameFromId(e){return e.split("_")[3]},typeOfName(e){return-1!==e.indexOf("Join")?"JOIN":"TABLE"},getChildTable(e,t){return"JOIN"===this.typeOfName(this.getNameFromId(e))?new Set([...t[e].left,...t[e].right]):new Set([this.getNameFromId(e)])}}},R=E,O=(a("ed46"),a("f803"),Object(r["a"])(R,F,A,!1,null,"c40c9250",null)),Q=O.exports,j=function(){var e=this,t=e.$createElement,s=e._self._c||t;return s("div",{staticClass:"about"},[s("el-container",{staticClass:"layout-container"},[s("el-header",{staticClass:"el-header"},[s("div",{staticClass:"title"},[s("span",[e._v("ARENA")])])]),s("el-main",{staticClass:"el-main",staticStyle:{height:"85vh"},attrs:{id:"largeFont"}},[s("el-steps",{attrs:{active:e.activeStep,"finish-status":"success"}},[s("el-step",{attrs:{title:"Step 1"}}),s("el-step",{attrs:{title:"Step 2"}}),s("el-step",{attrs:{title:"Step 3"}}),s("el-step",{attrs:{title:"Step 4"}}),s("el-step",{attrs:{title:"Step 5"}})],1),s("el-card",{directives:[{name:"show",rawName:"v-show",value:0===e.activeStep,expression:"activeStep === 0"}],staticStyle:{height:"60vh","margin-top":"40px","font-size":"18px"}},[e._v(" Please follow this guide to complete the survey. ")]),s("el-card",{directives:[{name:"show",rawName:"v-show",value:1===e.activeStep,expression:"activeStep === 1"}],staticStyle:{height:"60vh","margin-top":"40px","font-size":"18px"}},[s("ul",[s("li",{staticStyle:{"margin-bottom":"15px"}},[e._v(" Please refer to this "),s("router-link",{attrs:{tag:"a",target:"_blank",to:"/"}},[e._v(" link ")]),e._v(" to access the ARENA system. ")],1),s("li",{staticStyle:{"margin-bottom":"15px"}},[e._v(" Refer to this "),s("router-link",{attrs:{tag:"a",target:"_blank",to:"/about"}},[e._v(" link ")]),e._v(" for help on how to use the ARENA system. ")],1)]),s("div",{staticStyle:{"line-height":"40px"}},[e._v(' Firstly, please refer to the above two links to learn how to use the ARENA system. In particular, read the "how to use it" section of the help page carefully. '),s("br"),e._v(" In this step, you don't need to think about how to complete the survey, you just need to be able to use the ARENA system. ")])]),s("el-card",{directives:[{name:"show",rawName:"v-show",value:2===e.activeStep,expression:"activeStep === 2"}],staticStyle:{height:"60vh","margin-top":"40px","font-size":"18px"}},[s("div",{staticStyle:{"text-align":"center","margin-bottom":"20px"}},[s("img",{staticClass:"image",attrs:{src:a("05f6")}})]),s("div",{staticStyle:{"line-height":"40px"}},[e._v(" When you can use the ARENA system, please use the given SQL examples to view the best plan and corresponding alternative plans for different SQL. "),s("br"),e._v(" Compare the difference between the alternative plans and the best plan and examine how these differences affect the Cost. Try to think about why. ")])]),s("el-card",{directives:[{name:"show",rawName:"v-show",value:3===e.activeStep,expression:"activeStep === 3"}],staticStyle:{"margin-top":"40px","font-size":"18px"}},[s("div",{staticStyle:{"text-align":"center","margin-bottom":"20px"}},[s("img",{staticClass:"image",attrs:{src:a("71ca")}})]),s("div",{staticStyle:{"line-height":"20px"}},[s("p",[e._v(" You have seen how to use the ARENA system using SQL examples and understood the the results it returns. ")]),s("p",{staticStyle:{color:"#409EFF"}},[e._v(' Now, please check carefully the results of "5.sql" and "6.sql" in the example, and send us the feedback, which is the main part of the survey. ')]),s("p",[e._v(" For each of these two SQLs(5.sql and 6.sql), you need to feed back the following information: ")]),s("ul",[s("li",{staticStyle:{"margin-bottom":"20px",color:"#409EFF"}},[e._v(" The ID of the alternative plan you think is the most valuable and the reason. ")]),s("li",{staticStyle:{color:"#409EFF"}},[e._v(" The ID of the alternative plan you think is the least valuable and the reason. ")])]),s("p",{staticStyle:{"font-size":"14px","text-indent":"20px","font-style":"italic",color:"#909399"}},[e._v(" (P.S. A plan is valuable if you can learn something through the difference between it and the best plan. If you can't learn anything, it's worthless.) ")]),s("p",[e._v(' Fill this information into the form shown above and click the "submit" button to submit. For each of "5.sql" and "6.sql", you need to submit it once. If a duplicate submission is made to one of them, the previous feedback will be overwritten. ')]),s("p",[e._v(" *** This step is the key to the survey, please complete it carefully. *** ")])])]),s("el-card",{directives:[{name:"show",rawName:"v-show",value:4===e.activeStep,expression:"activeStep === 4"}],staticStyle:{height:"60vh","margin-top":"40px"}},[s("div",{staticStyle:{"text-align":"center","margin-bottom":"20px"}},[s("img",{staticClass:"image",attrs:{src:a("c939")}})]),s("p",[e._v(" If you have any suggestions for the ARENA system, please give us feedback through the section shown above. Any suggestions are welcome. ")]),s("p",[e._v(" Congratulations, you have completed the survey ! ")])])],1),s("el-footer",{staticClass:"footer"},[s("el-form",[s("el-form-item",[s("el-button",{staticStyle:{width:"120px","margin-right":"40px"},attrs:{type:"primary"},on:{click:e.previousStep}},[e._v(" Previous Step ")]),s("el-button",{staticStyle:{width:"120px"},attrs:{type:"primary"},on:{click:e.nextStep}},[e._v(" Next Step ")])],1)],1)],1)],1)],1)},J=[],q={name:"Guide",data(){return{activeStep:0}},methods:{previousStep(){0!==this.activeStep&&(this.activeStep-=1)},nextStep(){this.activeStep<4&&(this.activeStep+=1)}}},z=q,$=(a("9602"),a("d7f0"),Object(r["a"])(z,j,J,!1,null,"199ffd9c",null)),G=$.exports;s["default"].use(h["a"]);var B=new h["a"]({mode:"hash",routes:[{path:"/",name:"HelloWorld",component:k},{path:"/about",name:"about",component:L},{path:"/compareTree",name:"compareTree",component:Q},{path:"/Guide",name:"Guide",component:G}]}),W=a("5c96"),H=a.n(W),M=(a("0fae"),a("bc3a")),U=a.n(M);s["default"].config.productionTip=!1,s["default"].use(H.a),U.a.defaults.baseURL="http://82.157.149.11:6001/",s["default"].prototype.$axios=U.a,s["default"].prototype.setDiffColor=function(e,t){if(t){let t=e.TABLE,a=e.SCAN,s=e.JOIN;for(let i=0;i<e.nodes.length;i++){let o=e.nodes[i].id,l=e.nodes[i].text;o in t?e.nodes[i].color="#f4f4f5":o in a?(e.nodes[i].color="#c6e2ff",-1!==l.search("TableScan")||(-1!==l.search("IndexScan")?e.nodes[i].fontColor="#F56C6C":-1!==l.search("IndexOnlyScan")&&(e.nodes[i].fontColor="#E6A23C"))):o in s?(e.nodes[i].color="#f8e3c5",-1!==l.search("HashJoin")||-1!==l.search("IndexNLJoin")&&(e.nodes[i].fontColor="#409EFF")):e.nodes[i].color="#c8c9cc"}}else if(1!==e.rootId)for(let a=0;a<e.nodes.length;a++)delete e.nodes[a].color,delete e.nodes[a].fontColor},new s["default"]({el:"#app",router:B,components:{App:d},template:"<App/>"})},"57f1":function(e,t,a){},"71ca":function(e,t,a){e.exports=a.p+"static/img/feedback.5ca00943.jpg"},"848d":function(e,t,a){e.exports=a.p+"static/img/step2.d8961ff7.jpg"},"85ec":function(e,t,a){},"874a":function(e,t,a){"use strict";a("8ca0")},"8ca0":function(e,t,a){},"90c7":function(e,t,a){},9602:function(e,t,a){"use strict";a("0ab6")},a82f:function(e,t,a){},c939:function(e,t,a){e.exports=a.p+"static/img/suggest.af99574e.jpg"},d246:function(e,t,a){},d7f0:function(e,t,a){"use strict";a("2b44")},ed46:function(e,t,a){"use strict";a("d246")},f33d:function(e,t,a){e.exports=a.p+"static/img/step1.d08d3282.jpg"},f803:function(e,t,a){"use strict";a("57f1")},fa7c:function(e,t,a){}});
//# sourceMappingURL=app.8c36ebd8.js.map