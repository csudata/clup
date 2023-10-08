var g_memu_list = {
	"app": 2,
	"returnurl":"#",
	"titleMessageText":"数据库管理系统",
	"routers": [{
			"path": "/login",
			"name": "登录",
			"component": "Login"
		},
		{
			"path": "/",
			"name": "总览页",
			"component": "Home",
			"redirect": "/dashboard",
			"menuShow": true,
			"leaf": true,
			"iconCls": "el-icon-menu",
			"children": [
				{ "path": "/dashboard", "component": "dashboard", "name": "总览", "menuShow": true }
			]
		},
		{
			"path": "/",
			"component": "Home",
			"name": "数据库管理",
			"menuShow": true,
			"iconCls": "el-icon-coin",
			"children": [
				{ "path": "/dbLists", "iconCls": "el-icon-s-data", "component": "dbLists", "name": "数据库列表", "menuShow": true },
				{ "path": "/SessionManage", "iconCls": "el-icon-chat-dot-square", "component": "SessionManage", "name": "会话管理", "menuShow": true },
				{ "path": "/lockManage", "iconCls": "el-icon-lock", "component": "lockManage", "name": "锁管理", "menuShow": true },
				{ "path": "/dbLogView?logtype=1", "iconCls": "el-icon-tickets", "component": "dbLogView", "name": "日志查看", "menuShow": true }
			]
		},
		{
			"path": "/",
			"component": "Home",
			"name": "HA集群",
			"menuShow": true,
			"iconCls": "iconfont el-icon-gkjq",
			"children": [
				{ "path": "/clusterDefine", "iconCls": "el-icon-setting", "component": "clusterDefine", "name": "集群定义", "menuShow": true },
				{ "path": "/HAManage", "iconCls": "iconfont el-icon-jqgl", "component": "HAManage", "name": "HA管理", "menuShow": true },
				{ "path": "/HALogView?logtype=3", "iconCls": "el-icon-tickets", "component": "HALogView", "name": "HA日志查看", "menuShow": true }
			]
		},
		{
			"path": "/",
			"name": "系统管理",
			"component": "Home",
			"menuShow": true,
			"iconCls": "el-icon-menu",
			"children": [
				{ "path": "/AgentStatusView", "iconCls": "iconfont el-icon-xnjk", "component": "AgentStatusView", "name": "Agent状态管理", "menuShow": true },
				{ "path": "/JournalTabManage", "iconCls": "iconfont el-icon-rizhishezhi","component": "JournalTabManage", "name": "日志设置", "menuShow": true },
				{ "path": "/clupParameterConfiguration", "iconCls": "iconfont el-icon-canshushezhi","component": "clupParameterConfiguration", "name": "clup参数设置", "menuShow": true },
			]
		},
	]
}