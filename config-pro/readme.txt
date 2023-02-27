注意事项：
1.启动前请编辑好application-pro.properties文件中的oracle的信息：
spring.datasource.oracle.url=jdbc:oracle:thin:@${ip}:1521/${service}
spring.datasource.oracle.username=${username}
spring.datasource.oracle.password=${password}
spring.datasource.oracle.database=${database}

启动程序：
./service.sh start

停止
./service.sh stop