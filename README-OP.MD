1，模块编译
1,kafka-connect-jdbc
mvn clean install -DskipTests -Dcheckstyle.skip

2, debezium-mysql
mvn clean install -pl debezium-core -am -DskipTests -Dcheckstyle.skip -Denforcer.skip=true

mvn clean install -pl debezium-connector-mysql -am -Dmaven.test.skip=true -Denforcer.skip=true -Dcheckstyle.skip=true -Dmaven.enforcer.skip=true

2，连接启动
账号
kafka-ui http://192.168.8.47:9080
kafka-connect-ui   http://192.168.8.47:8000
mysql-source 192.168.8.47:13306 root/root
mysql-sink 192.168.8.47:23306 root/root



#建表语句
CREATE TABLE IF NOT EXISTS debezium.`user` (
  `uid` bigint(20) unsigned NOT NULL COMMENT '用户 id',
  `user_name` varchar(64) NOT NULL DEFAULT '' COMMENT '用户名称',
  `user_email` varchar(128) NOT NULL DEFAULT '' COMMENT '用户 email 地址',
  `status` tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT '状态，0、待审核，1、有效，2、无效',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`uid`),
  UNIQUE KEY `uniq_user_email` (`user_email`),
  KEY `idx_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='用户表';

INSERT INTO debezium.`user`
(uid, user_name, user_email, status, create_time, update_time)
VALUES(1, 'ceshi', '998', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);


INSERT INTO debezium.`user`
(uid, user_name, user_email, status, create_time, update_time)
VALUES(2, 'ceshi2', '9982', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);


INSERT INTO debezium.`user`
(uid, user_name, user_email, status, create_time, update_time)
VALUES(3, 'ceshi3', '9983', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT INTO debezium.`user`
(uid, user_name, user_email, status, create_time, update_time)
VALUES(4, 'ceshi4', '9984', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT INTO debezium.`user`
(uid, user_name, user_email, status, create_time, update_time)
VALUES(5, 'ceshi5', '9985', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);


# mysqlsource连接  需要开发
name=ceshi-source
connector.class=io.debezium.connector.mysql.MySqlConnector
database.user=root
database.hostname=dbzui-mysql-source
database.password=root
schema.history.internal.kafka.topic=schemhistory
schema.history.internal.kafka.bootstrap.servers=dbzui-kafka:9092
schema.history.internal.name=schemhistory
#database.history.kafka.topic=mysqlhistory
database.server.id=1
database.server.name=ceshi
database.port=3306
database.include.list=debezium
include.schema.changes=true
database.allowPublicKeyRetrieva=true
topic.prefix=source15
snapshot.mode=initial
#选择字段名
column.include.list=debezium.user.uid,debezium.user.user_name,debezium.user.user_email
#topic路由,数据转换
transforms=ExtractNewRecord
transforms.ExtractNewRecord.type=io.debezium.transforms.ExtractNewRecordState
transforms.ExtractNewRecord.add.headers=op,db,table,source_name
transforms.ExtractNewRecord.drop.tombstones=false
#心跳策略
# 心跳检测时间,需要有建库权限
heartbeat.interval.ms=60
topic.heartbeat.exact=debezium.heartbeat.mysql



#使用kafka-connect-sink的连接
name=ceshi-source
connector.class=io.confluent.connect.jdbc.JdbcSourceConnector
tasks.max=10
connection.user=root
connection.password=root
connection.url=jdbc:mysql://dbzui-mysql-source:3306/debezium?useInformationSchema=true&nullCatalogMeansCurrent=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&connectTimeout=30000
#数据表白名单
table.whitelist=user
mode=timestamp
timestamp.column.name=create_time
#incrementing.column.name=id
validate.non.null=false
#topic的前缀，confulent平台会为每张表创建一个topic,topic的名称为前缀+表名
topic.prefix=mysql-test-


使用kafka-connect-sink的连接
name=ceshi-sink
connector.class=io.confluent.connect.jdbc.JdbcSinkConnector
tasks.max=1
connection.user=root
connection.password=root
connection.url=jdbc:mysql://dbzui-mysql-sink:3306/debezium?useInformationSchema=true&nullCatalogMeansCurrent=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&connectTimeout=30000
insert.mode=upsert
topics=source9.debezium.user
pk.fields=uid
pk.mode=record_key
delete.enabled=true

#备注
C 增加
u 修改
d 删除


name=ceshi-source
connector.class=io.debezium.connector.mysql.MySqlConnector
database.user=root
database.hostname=dbzui-mysql-source
database.password=root
schema.history.internal.kafka.topic=schemhistory
schema.history.internal.kafka.bootstrap.servers=dbzui-kafka:9092
schema.history.internal.name=schemhistory
#database.history.kafka.topic=mysqlhistory
database.server.id=1
database.server.name=ceshi
database.port=3306
database.include.list=debezium
include.schema.changes=true
topic.prefix=source9