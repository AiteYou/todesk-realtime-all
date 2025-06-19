## 本地测试时需要：
注释掉pom文件中  
slf4j和log4j相关依赖的<scope>  
com.todesk.bigdata.realtime.utils中EnvConfig的  
```
//        env.getCheckpointConfig().setCheckpointTimeout(120000L);
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://cluster-hdfs/realtime/checkpoint");
```
## 打包上线时需要：
将com.todesk.bigdata.realtime.utils中EnvConfig的注释掉的放开   
pom文件中slf4j和log4j相关依赖三个都注释掉
