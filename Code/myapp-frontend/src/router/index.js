import Vue from "vue";
import VueRouter from "vue-router";
import Main from '../Views/Main'
import Num1 from '../Views/Num1'
import Num2 from '../Views/Num2'
import Num3 from '../Views/Num3'
import Num4 from '../Views/Num4'
import Num5 from '../Views/Num5'
import Num6 from '../Views/Num6'
import Num7 from '../Views/Num7'
import Num8 from '../Views/Num8'
import Num9 from '../Views/Num9'
import Num10 from '../Views/Num10'
import Num11 from '../Views/Num11'
import Num12 from '../Views/Num12'


Vue.use(VueRouter)

const routes = [
    // 主路由
    {
        path: '/',
        name: 'Main',
        component: Main,
        redirect: '/num1', // 重定向
        children: [
            // 子路由
            {
                path: 'num1',
                name: 'num1',
                component: Num1,
            },
            // 子路由
            {
                path: '/num2',
                name: 'num2',
                component: Num2,
            },
            // 子路由
            {
                path: '/num3',
                name: 'num3',
                component: Num3,
            },
            // 子路由
            {
                path: '/num4',
                name: 'num4',
                component: Num4,
            },
            // 子路由
            {
                path: '/num5',
                name: 'num5',
                component: Num5,
            },
            {
                path: '/num6',
                name: 'num6',
                component: Num6,
            },
            {
                path: '/num7',
                name: 'num7',
                component: Num7,
            },
            {
                path: '/num8',
                name: 'num8',
                component: Num8,
            },
            {
                path: '/num9',
                name: 'num9',
                component: Num9,
            },
            {
                path: '/num10',
                name: 'num10',
                component: Num10,
            },
            {
                path: '/num11',
                name: 'num11',
                component: Num11,
            },
            {
                path: '/num12',
                name: 'num12',
                component: Num12,
            },
        ]
    },

]

const router = new VueRouter({
    routes
})



export default router