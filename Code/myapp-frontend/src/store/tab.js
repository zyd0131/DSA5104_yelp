export default {
    state: {
        isCollapse: false, //导航栏是否折叠
        tabList: [{
            path: '/num1',
            name: 'num1',
            label: 'Query 1',
            icon: 's-home',
            url: 'Num1.vue'
        }], //面包屑的数据:点了哪个路由,首页是一定有的
        menu: [{
                path: '/num1',
                name: 'num1',
                label: 'Query 1',
                icon: 's-home',
                url: 'Num1.vue'
            },{
                path: '/num2',
                name: 'num2',
                label: 'Query 2',
                icon: 's-home',
                url: 'Num2.vue'
            },{
                path: '/num3',
                name: 'num3',
                label: 'Query 3',
                icon: 's-home',
                url: 'Num3.vue'
            },{
                path: '/num4',
                name: 'num4',
                label: 'Query 4',
                icon: 's-home',
                url: 'Num4.vue'
            },{
                path: '/num5',
                name: 'num5',
                label: 'Query 5',
                icon: 's-home',
                url: 'Num5.vue'
            },{
                path: '/num6',
                name: 'num6',
                label: 'Query 6',
                icon: 's-home',
                url: 'Num6.vue'
            },{
                path: '/num7',
                name: 'num7',
                label: 'Query 7',
                icon: 's-home',
                url: 'Num7.vue'
            },{
                path: '/num8',
                name: 'num8',
                label: 'Query 8',
                icon: 's-home',
                url: 'Num8.vue'
            },{
                path: '/num9',
                name: 'num9',
                label: 'Query 9',
                icon: 's-home',
                url: 'Num9.vue'
            },{
                path: '/num10',
                name: 'num10',
                label: 'Query 10',
                icon: 's-home',
                url: 'Num10.vue'
            },{
                path: '/num11',
                name: 'num11',
                label: 'Query 11',
                icon: 's-home',
                url: 'Num11.vue'
            },{
                path: '/num12',
                name: 'num12',
                label: 'Query 12',
                icon: 's-home',
                url: 'Num12.vue'
            },
            

        ]

    },
    mutations: {
        // 修改导航栏展开和收起的方法
        CollapseMenu(state) {
            state.isCollapse = !state.isCollapse
        },
        // 更新面包屑的数据
        SelectMenu(state, item) {
            // 如果点击的不在面包屑数据中,则添加
            const index = state.tabList.findIndex(val => val.name === item.name)
            if (index === -1) {
                state.tabList.push(item)
            }
        },
        // 删除tag:删除tabList中对应的item
        closeTag(state, item) {
            // 要删除的是state.tabList中的item
            const index = state.tabList.findIndex(val => val.name === item.name)
            state.tabList.splice(index, 1)
        },

    }
}