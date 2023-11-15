<template>
  <div class="manage">
    <div class="manage-header">
      <el-form :inline="true">
        <el-form-item>
          <el-input
            v-model="needSql"
            readonly
            placeholder=""
            autosize
            style="width: 1500px;"
          ></el-input>
        </el-form-item>
      </el-form>
    </div>
    <div class="common-table">
      <div class="show-table">
        <el-table
          :data="mysqlTableData"
          stripe
          style="width: 100%"
          height="70%"
          border
        >
          <el-table-column
            v-for="item in mysqlColList"
            :key="item.prop"
            :prop="item.prop"
            :label="item.label"
          >
          </el-table-column>
        </el-table>
        <!-- 分页 -->
        <div class="pager">
          <el-pagination
            layout="prev, pager, next"
            :total="mysqlTotal"
            @current-change="mysqlCurrentChange"
          >
          </el-pagination>
        </div>
        <el-form
          inline
          readonly
          class="my-form"
          label-width="140px"
        >
          <el-form-item
            label="Mysql Query"
            style="width: 100%"
          >
            <el-input
              :value="mysql"
              type="textarea"
              autosize
              style="width: 100%"
            >
            </el-input>
          </el-form-item>
          <el-form-item
            label="Mysql Query Time"
            style="width: 100%"
          >
            <el-input
              :value="mysqlTime"
              style="width: 100%"
            >
            </el-input>
          </el-form-item>
          <el-form-item
            label="Mysql Query Number"
            style="width: 100%"
          >
            <el-input :value="mysqlTps" style="width: 100%">
            </el-input>
          </el-form-item>
        </el-form>
      </div>
      <div class="show-table">
        <el-table
          :data="mangoTableData"
          stripe
          style="width: 100%"
          height="70%"
          border
        >
          <el-table-column
            v-for="item in mangoColList"
            :key="item.prop"
            :prop="item.prop"
            :label="item.label"
          >
          </el-table-column>
        </el-table>
        <!-- 分页 -->
        <div class="pager">
          <el-pagination
            layout="prev, pager, next"
            :total="mangoTotal"
            @current-change="mangoCurrentChange"
          >
          </el-pagination>
        </div>
        <el-form
          inline
          readonly
          class="my-form"
          label-width="140px"
        >
          <el-form-item
            label="MongoDB Query"
            style="width: 100%"
          >
            <el-input
              :value="mql"
              type="textarea"
              autosize
              style="width: 100%"
            >
            </el-input>
          </el-form-item>
          <el-form-item
            label="MongoDB Query Time"
            style="width: 100%"
          >
            <el-input
              :value="mongoTime"
              style="width: 100%"
            >
            </el-input>
          </el-form-item>
          <el-form-item
            label="MongoDB Query Number"
            style="width: 100%"
          >
            <el-input :value="mongoTps" style="width: 100%">
            </el-input>
          </el-form-item>
        </el-form>
      </div>
    </div>
  </div>
</template>

<script>
import { getNum3Data } from "@/api/index";
import { changePage } from "@/utils/page";
export default {
  data() {
    return {
      mangoRawData: null,
      mysqlRawData: null,
      options: [],
      needSql: null,
      mysql: null,
      mql: null,
      mysqlTime: null,
      mysqlTps: null,
      mongoTime: null,
      mongoTps: null,
      mysqlPage: 1,
      mysqlTotal: 0,
      mangoPage: 1,
      mangoTotal: 0,
    };
  },
  computed: {
    mysqlTableData() {
      return this.mysqlRawData === null
        ? []
        : this.mysqlRawData[this.mysqlPage - 1];
    },
    mangoTableData() {
      return this.mangoRawData === null
        ? []
        : this.mangoRawData[this.mangoPage - 1];
    },
    mysqlColList() {
      if (!this.mysqlTableData) return [];
      const keys = Object.keys(
        this.mysqlTableData[0] || []
      );
      return keys.map((key) => {
        return { prop: key, label: key };
      });
    },
    mangoColList() {
      if (!this.mangoTableData) return [];
      const keys = Object.keys(
        this.mangoTableData[0] || []
      );
      return keys.map((key) => {
        return { prop: key, label: key };
      });
    },
  },
  methods: {
    search() {
      this.getList();
    },
    mysqlCurrentChange(val) {
      this.mysqlPage = val;
    },
    mangoCurrentChange(val) {
      this.mangoPage = val;
    },
    getList() {
      getNum3Data().then((res) => {
        const list = res.data;
        this.mangoRawData = changePage(list.MongoDB.data);
        this.mysqlRawData = changePage(list.MySQL.data);
        this.mysqlTotal = list.MySQL.data.length;
        this.mangoTotal = list.MongoDB.data.length;
        this.mysql = list.MySQL.SQL;
        this.mql = list.MongoDB.MQL;
        this.mysqlTime = list.MySQL.time;
        this.mysqlTps = list.MySQL.tps;
        this.mongoTime = list.MongoDB.time;
        this.mongoTps = list.MongoDB.tps;
        this.needSql = list.query; 
      });
    },
  },
  mounted() {
    this.getList();
  },
};
</script>
