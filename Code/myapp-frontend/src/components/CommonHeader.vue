<template>
  <div class="header-container">
    <div class="l-content">
      <el-button
        @click="handleMenu"
        icon="el-icon-menu"
        size="mini"
      ></el-button>
      面包屑 
      <el-breadcrumb separator="/">
        <el-breadcrumb-item
          v-for="item in list"
          :key="item.name"
          >{{ item.label }}
        </el-breadcrumb-item>
      </el-breadcrumb>
    </div>
   
  </div>
</template>


<script>
import { mapState } from "vuex";
export default {
  methods: {
    handleMenu() {
      // 相当于调用这个方法
      this.$store.commit("CollapseMenu");
    },
  },
  computed: {
    ...mapState({
      menus: (state) => state.tab.menu,
    }),
    list() {
      for (let i = 0; i < this.menus.length; i++) {
        if (this.menus[i].name === this.$route.name) {
          return [this.menus[i]];
        }
        if (this.menus[i].children) {
          for (
            let j = 0;
            j < this.menus[i].children.length;
            j++
          ) {
            if (
              this.menus[i].children[j].name ===
              this.$route.name
            ) {
              return [this.menus[i].children[j]];
            }
          }
        }
      }
      return "";
    },
  },
};
</script>

<style lang="less" scoped>

  .header-title {
    color: white;
    text-align: center;
    font-weight: bold;
    flex-grow: 1;
  }

  /* 现有样式保持不变 */

.header-container {
  background-color: #333;
  height: 60px;

  // 让按钮和头像居中
  display: flex;
  justify-content: space-between;
  align-items: center;
  // 不要紧贴边框
  padding: 0 20px;

  .el-dropdown-link {
    cursor: pointer;
    color: #409eff;

    .user {
      width: 40px;
      height: 40px;
      // 50%变圆形
      border-radius: 50%;
    }
  }
}

.l-content {
  display: flex;
  // 上下居中
  align-items: center;

  .el-breadcrumb {
    margin-left: 15px;

    // deep 强制生效
    /deep/.el-breadcrumb__item {
      .el-breadcrumb__inner {
        &.is-link {
          color: #666;
        }
      }

      &:last-child {
        .el-breadcrumb__inner {
          color: #fff;
        }
      }
    }
  }
}
</style>
