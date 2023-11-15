<template>
  <div id="app">
    <router-view></router-view>
  </div>
</template>

<script>
export default {
  mounted(){
    fetch("http://127.0.0.1:5000/api/num1")
  }

};
</script>

<style>
html,
body,
h3,
p {
  margin: 0;
  padding: 0;
}
</style>
