#Blazer/数据传播者
Blazer意为**传播者**，主要实现不同数据库之间的数据同步，导出数据库表结构创建语句。

1. 导出数据库表数据到txt/CSV文件（多线程）或者excel(.xlsx/.xls)；

2. 把txt文件或者excel中的数据导入到数据库表，支持基于javascript脚本对数据的处理；

3. 翻译特定的数据库表结构ddl成其他数据库的语法，方便数据迁移；

4. 数据库之间的数据同步；

5. 目前支持数据有mysql/MariaDB,Greenplum,Oracle,DB2,PostgreSQL;

------------

   项目基于Java Spring Framework框架，简化了系统的配置，引入"管道"（PipeLine）的概念，一个管道包含多个任务，任务可以是数据同步、导入、导出或者表结构的导出。


   在不同的XML中引入不同管道任务，实现不同的功能，其中pipeLine.xml描述管道的功能，需要根据需求进行定义，可以基于以下的命令内容copy出不同脚本功能。
PipeLineRunner.bat
````bash
@echo off
SET JAVA_HOME=%cd%/jre

SET JAVA_MARK=PipeLineRunner
SET JAVA_OPTS=" -Xms128m "
SET JAVA_OPTS=%JAVA_OPTS% -Xmx1024m"
rem SET JAVA_OPTS="%JAVA_OPTS% -Dfile.encoding=UTF-8"
SET JAVA_OPTS="%JAVA_OPTS% -Dfile.encoding=GBK"
SET JAVA_OPTS="%JAVA_OPTS% -DjavaMark=%JAVA_MARK%"

SET JAVA_CONF=./etc
SET JAVA_LIBPATH=./lib
SET JAVA_CLASSPATH=./classes;./bin;%JAVA_CONF%
SET JAVA_MAINCLASS=com.blazer.pipeline.PipeLineRunner
SET JAVA_EXEC=%JAVA_HOME%/bin/java

rem mk logs dir
if NOT EXIST "./logs" MKDIR "logs"
rem init TEMP_CLASSPATH
SET TEMP_CLASSPATH=
rem new setclasspath.bat
echo SET TEMP_CLASSPATH=%%TEMP_CLASSPATH%%;%%1> setclasspath.bat

FOR  %%i IN (%JAVA_LIBPATH%/*.jar) DO (
CALL setclasspath.bat %JAVA_LIBPATH%/%%i
)

SET JAVA_CLASSPATH=%JAVA_CLASSPATH%;%TEMP_CLASSPATH%
rem delete setclasspath.bat
DEL setclasspath.bat
echo %JAVA_CLASSPATH%

rem Display our environment
echo ===============================================================================  
echo Bootstrap Environment 
echo.  
echo JAVA_CLASSPATH =  %JAVA_CLASSPATH%
echo JAVA_CONF      =  %JAVA_CONF%  
echo JAVA_OPTS      =  %JAVA_OPTS%  
echo JAVA_HOME      =  %JAVA_HOME%  
echo JAVA           =  %JAVA_EXEC%  
echo.  
%JAVA_EXEC% -version
echo.  
echo ===============================================================================  
echo.  
  
%JAVA_EXEC% %JAVA_OPTS%  -classpath %JAVA_CLASSPATH% %JAVA_MAINCLASS% --config pipeLine.xml

echo run finished
PAUSE
````

PipeLineRunner.sh
```shell
#!/bin/bash
JAVA_HOME=/opt/java6

JAVA_MARK=PipeLineRunner
JAVA_OPTS=" -Xms128m "
JAVA_OPTS="${JAVA_OPTS} -Xmx1024m"
JAVA_OPTS="${JAVA_OPTS} -Dfile.encoding=UTF-8"
JAVA_OPTS="${JAVA_OPTS} -DjavaMark=${JAVA_MARK}"

JAVA_CONF=./etc
JAVA_LIBPATH=./lib
JAVA_CLASSPATH=./classes:./bin
JAVA_MAINCLASS=com.blazer.pipeline.PipeLineRunner
JAVA_EXEC=${JAVA_HOME}/bin/java

export JAVA_CLASSPATH
export JAVA_LIBPATH

for LL in `ls $JAVA_LIBPATH/*.jar`
        do
                JAVA_CLASSPATH=$JAVA_CLASSPATH:$LL
               
done

export JAVA_CLASSPATH
# Display our environment
echo "========================================================================="
echo "  Bootstrap Environment"
echo ""
echo JAVA_CLASSPATH :  ${JAVA_CLASSPATH}
echo JAVA_CONF      :  ${JAVA_CONF}
echo JAVA_OPTS      :  ${JAVA_OPTS}
echo JAVA_HOME      :  ${JAVA_HOME}  
echo JAVA           :  ${JAVA_EXEC} 
${JAVA_EXEC} -version
echo ""
echo "========================================================================="
echo ""

${JAVA_EXEC} ${JAVA_OPTS} -classpath ${JAVA_CLASSPATH} ${JAVA_MAINCLASS} --config pipeLine.xml

echo run finished
```

实现Oracle到greenplum数据同步
```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans 	xmlns="http://www.springframework.org/schema/beans"
		xmlns:context="http://www.springframework.org/schema/context"
		xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
		xmlns:util="http://www.springframework.org/schema/util"
		xsi:schemaLocation="
	        http://www.springframework.org/schema/beans     
	        http://www.springframework.org/schema/beans/spring-beans.xsd
	        http://www.springframework.org/schema/context 
	        http://www.springframework.org/schema/context/spring-context.xsd
	        http://www.springframework.org/schema/util     
	        http://www.springframework.org/schema/util/spring-util.xsd">
 	   
    <!-- Application properties configs  -->
	<bean id="propertySourcesPlaceholderConfigurer" class="org.springframework.context.support.PropertySourcesPlaceholderConfigurer">
	  <property name="locations">
	    <list>
	   	  <value>/pipeline/config/applicationConfig.properties</value>
	    </list>
	  </property>
	  <property name="ignoreUnresolvablePlaceholders" value="true"/>
	</bean>
	
	<!-- enable component scanning (beware that this does not enable mapper scanning!) -->    
    <context:component-scan base-package="com.blazer" />
 	
 	<!-- Datastore configuration /数据源配置 -->
 	<import resource="database.xml"/>
 	
 	<!-- FULL      在插入前进行删除,默认    -->
	<!-- INCREMENT 先清除条件相关数据，然后按照条件进行增量插入 -->
 	<bean id="transData" class="com.blazer.trans.TransData">
 		<!--源数据库-->
		<property name="sourceDataSource" ref="datasource_oracle"/>
		<property name="fromUrl" value="${datasource_oracle.url}"/>
 		<property name="fromUser" value="${datasource_oracle.username}"/> 
 		<!--目标数据库-->
 		<property name="targetDataSource" ref="datasource_greenplum"/>
 		<property name="toUrl" value="${datasource_greenplum.url}"/>
 		<property name="toUser" value="${datasource_greenplum.username}"/> 
 		
		<!--全量-->
 		<property name="transType" value="FULL"/> 
 		<property name="tablesList" > 
 			<util:list  list-class="java.util.ArrayList">
			 	<bean class="com.db.TableDescribe">
					<!--源表名-->
 					<property name="tableName" value="SIGNAL"/> 
					<!--目标表名-->
 					<property name="targetTableName" value="SIGNAL"/> 
					<!--筛选条件-->
					<property name="whereSqlString" value="name='TEST'"/>
 				</bean>
 				<bean class="com.db.TableDescribe">
 					<property name="tableName" value="CALENDAR"/> 
 					<property name="targetTableName" value="M_CALENDAR"/> 
 				</bean>

 				
 			</util:list>
 		</property>
 	</bean>
 	
 

 	<!-- 配置执行的任务列表  -->
 	<util:list id="pipeLineTask" list-class="java.util.ArrayList">
 		<ref bean="transData"/>
 	</util:list>
 	
</beans>
```
