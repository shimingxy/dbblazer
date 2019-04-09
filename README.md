# Blazer/数据传播者
Blazer意为**传播者**，主要实现不同数据库之间的数据同步，导出数据库表结构创建语句。

1. 导出数据库表数据到txt/CSV文件（多线程）或者excel(.xlsx/.xls)；

2. 把txt文件或者excel中的数据导入到数据库表，支持基于javascript脚本对数据的处理；

3. 翻译特定的数据库表结构ddl成其他数据库的语法，方便数据迁移；

4. 数据库之间的数据同步；

5. 目前支持数据有mysql/MariaDB,Greenplum,Oracle,DB2,PostgreSQL;

6. 支持JDK支持平台，JDK1.8+

------------

   项目基于Java Spring Framework框架，简化了系统的配置，引入"管道"（PipeLine）的概念，一个管道包含多个任务，任务可以是数据同步、导入、导出或者表结构的导出。


   在不同的XML中引入不同管道任务，实现不同的功能，其中pipeLine.xml描述管道的功能，需要根据需求进行定义，可以基于以下的命令内容copy出不同脚本功能,传入不同的"管道"实现不同的功能。
   
------------
#### 脚本
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
#### "管道"（PipeLine）
###### 1、实现Oracle到greenplum数据同步pipeLine
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
	<!-- Application properties configs  应用程序属性配置文件-->
	<bean id="propertySourcesPlaceholderConfigurer" class="org.springframework.context.support.PropertySourcesPlaceholderConfigurer">
	  <property name="locations"><list>
	   	 <value>/pipeline/config/applicationConfig.properties</value>
	  </list></property>
	  <property name="ignoreUnresolvablePlaceholders" value="true"/>
	</bean>
	
	<!-- enable component scanning (beware that this does not enable mapper scanning!) -->    
    <context:component-scan base-package="com.blazer" />
 	
 	<!-- Datastore configuration /数据源配置 -->
 	<import resource="database.xml"/>

 	<bean id="transData" class="com.blazer.trans.TransData">
 		<!--源数据库-->
		<property name="sourceDataSource" ref="datasource_oracle"/>
		<property name="fromUrl" value="${datasource_oracle.url}"/>
 		<property name="fromUser" value="${datasource_oracle.username}"/> 
 		<!--目标数据库-->
 		<property name="targetDataSource" ref="datasource_greenplum"/>
 		<property name="toUrl" value="${datasource_greenplum.url}"/>
 		<property name="toUser" value="${datasource_greenplum.username}"/> 
		<!-- FULL      在插入前进行删除,默认    -->
		<!-- INCREMENT 先清除条件相关数据，然后按照条件进行增量插入 -->
 		<property name="transType" value="FULL"/> 
 		<property name="tablesList" > 
 			<util:list  list-class="java.util.ArrayList">
			 	<bean class="com.db.TableDescribe">
					<!--源表名-->
 					<property name="tableName" value="STUDENT"/> 
					<!--目标表名-->
 					<property name="targetTableName" value="STUDENT"/> 
					<!--筛选条件-->
					<property name="whereSqlString" value="CLASSES='2020'"/>
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
###### 2、数据库数据导出到文件(csv,xlsx,xls)pipeLine
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
 	<!-- Application properties configs  应用程序属性配置文件-->
	<bean id="propertySourcesPlaceholderConfigurer" class="org.springframework.context.support.PropertySourcesPlaceholderConfigurer">
	  <property name="locations"><list>
	   	 <value>/pipeline/config/applicationConfig.properties</value>
	  </list></property>
	  <property name="ignoreUnresolvablePlaceholders" value="true"/>
	</bean>
 	
 	<!-- Datastore configuration /数据源配置 -->
 	<import resource="database.xml"/>
 	
	<!--基本配置-->
 	<bean id="exportBasicConfigure" class="com.blazer.export.file.BasicConfigure">
 		<property name="sourceDataSource" ref="datasource_oracle"/>
		<property name="fromUrl" value="${datasource_oracle.url}"/>
 		<property name="fromUser" value="${datasource_oracle.username}"/> 
		<!--每次写条数-->
 		<property name="commitNumber" value="400"/>
		<!--导出线程-->
		<property name="threadSize" value="1"/>
		<!--导出文件格式 csv/xlsx/xls-->
		<property name="fileType" value="csv"/>
		<!--导出文件路径-->
 		<property name="exportFilePath" value="D:/dmp/"/>
		<!--导出文件后缀 csv(.txt,.csv)/xlsx(.xlsx)/xls(.xls) -->
 		<property name="fileNameSuffix" value=".txt"/>
		<!--导出文件字段分割-->
 		<property name="terminatedString" value="|+|"/>
 	</bean>	
 	<!--导出基本配置-->	
 	<bean id="transDataExport_STUD" class="com.blazer.export.file.TransDataExport">
 		<property name="sourceDataSource" ref="datasource_oracle"/>
		<property name="fromUrl" value="${datasource_oracle.url}"/>
 		<property name="fromUser" value="${datasource_oracle.username}"/> 
		<!--导出表名-->
		<property name="tableName" value="STUDENT"/>
		<!--导出文件名，其中{yyyyMMdd}为日期格式-->
		<property name="outFileName" value="STUDENT_{yyyyMMdd}_000_000"/>
		<!--每次写条数-->
 		<property name="commitNumber" value="400"/>
		<!--导出线程-->
		<property name="threadSize" value="1"/>
		<!--导出文件格式 csv/xlsx/xls-->
		<property name="fileType" value="csv"/>
		<!--导出文件路径-->
 		<property name="exportFilePath" value="D:/dmp/"/>
		<!--导出文件后缀 csv(.txt,.csv)/xlsx(.xlsx)/xls(.xls) -->
 		<property name="fileNameSuffix" value=".txt"/>
		<!--导出文件字段分割-->
 		<property name="terminatedString" value="|+|"/>
		<!--导出表数据过滤条件-->
		<!--
		<property name="whereSqlString" value="where length(body)&gt;10000 and length(body)&lt;50000" and FLAG='_EXPORT_ETL_DATE_'/>-->
		<property name="whereSqlString" value=""/>
		<!--导出字段，如果是*则全表导出-->
 		<property name="selectFieldsString" value="
			STUD_NAME           ,
			STUD_SEX         
		"
		/>
 	</bean>

 	<!-- 配置执行的任务列表  -->
 	<util:list id="pipeLineTask" list-class="java.util.ArrayList">
		<ref bean="transDataExport_STUD"/>
 	</util:list>
</beans>
```
###### 3、数据文件导入到数据库 pipeLine
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

 	<!-- Application properties configs  应用程序属性配置文件-->
	<bean id="propertySourcesPlaceholderConfigurer" class="org.springframework.context.support.PropertySourcesPlaceholderConfigurer">
	  <property name="locations"><list>
	   	 <value>/pipeline/config/applicationConfig.properties</value>
	  </list></property>
	  <property name="ignoreUnresolvablePlaceholders" value="true"/>
	</bean>
 	
 	<!-- Datastore configuration /数据源配置 -->
 	<import resource="database.xml"/>
 	
 	<bean id="transDataLoad_STUD" class="com.blazer.load.file.runner.TransDataLoadFile">
		<!--导入数据源-->
 		<property name="sourceDataSource" ref="datasource_oracle"/>
		<!--每次提交数据量-->
 		<property name="commitNumber" value="100"/>
		<!--字段最长限制-->
 		<property name="limitTextSize" value="0"/>
		<!--导入目标表名-->
 		<property name="tableName" value="STUDENT"/>
		<!--导入文件名-->
 		<property name="loadFileName" value="student_2020"/>
		<!--导入文件扩展名类型 csv/xlsx/xls-->
 		<property name="fileType" value="xls"/>
		<!--导入文件路径-->
 		<property name="loadFilePath" value="D:/dmp/"/>
		<!--是否跳过首行 true是跳过，false不跳过，默认为false-->
 		<property name="skipFirstRow" value="true"/>
 		<!--导入文件后缀-->
 		<property name="fileNameSuffix" value=".xls"/>
		<!--导入csv类型文件分隔符-->
 		<property name="terminatedString" value="|+|"/>
		<!--导入字段列表-->
 		<property name="listTableColumns" > 
 			<util:list  list-class="java.util.ArrayList">
				<bean class="com.db.TableColumns">
					<!--导入字段名-->
					<property name="columnName" value="STUD_NAME"/>
					<!--导入字段类型-->
 					<property name="dataType" value="VARCHAR"/>
					<!--导入字段是否跳过-->
					<property name="skip" value="false"/>
					<!--导入字段为固定值-->
					<property name="fixed" value="false"/>
					<!--导入字段默认值，配合fixed使用-->
					<property name="defaultValue" value=""/>
				</bean>
				<bean class="com.db.TableColumns">
					<property name="columnName" value="STUD_SEX"/>
 					<property name="dataType" value="VARCHAR"/>
					<!--导入字段基于javascript转换脚本-->
					<property name="convert" value="
						if(dataValue=='M'){
							returnValue='男';
						}else if(columns[1]=='男'){
							returnValue='男';
						}
						if(dataValue=='F'){
							returnValue='女';
						}
					"/>
				</bean>
		 	</util:list>
 		</property>
 		
 	</bean>
 	
 	<bean id="transDataLoad" class="com.blazer.load.file.TransDataLoad">
		<!--默认数据源-->
 		<property name="sourceDataSource" ref="datasource_oracle"/>
		<property name="fromUrl" value="${datasource_oracle.url}"/>
 		<property name="fromUser" value="${datasource_oracle.username}"/> 
		<!--默认导入线程数-->
 		<property name="threadSize" value="10"/>
		<!--默认导入文件路径-->
 		<property name="loadFilePath" value="D:/dmp/"/>
		<!--导入任务列表-->
 		<property name="transDataLoadFileList" > 
 			<util:list  list-class="java.util.ArrayList">
				<ref bean="transDataLoad_STUD"/>
		 	</util:list>
 		</property>
 	</bean>
 
 	<!-- 配置执行的任务列表  -->
 	<util:list id="pipeLineTask" list-class="java.util.ArrayList">
		<ref bean="transDataLoad"/>
 	</util:list>
</beans>
```
###### 4、数据表结构导出特定数据库DDL pipeLine
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
	<!-- Application properties configs  应用程序属性配置文件-->
	<bean id="propertySourcesPlaceholderConfigurer" class="org.springframework.context.support.PropertySourcesPlaceholderConfigurer">
	  <property name="locations"><list>
	   	 <value>/pipeline/config/applicationConfig.properties</value>
	  </list></property>
	  <property name="ignoreUnresolvablePlaceholders" value="true"/>
	</bean>
 	<context:component-scan base-package="com.blazer" />
 	<!-- Datastore configuration /数据源配置 -->
 	<import resource="database.xml"/>
	<!-- DDL导出描述文件 -->
 	<bean id="dllExport" class="com.blazer.ddl.DDLExport">
		<!-- 导出数据源 -->
 		<property name="dataSource" ref="datasource_oracle"/>
		<property name="url" value="${datasource_oracle.url}"/>
 		<property name="user" value="${datasource_oracle.username}"/> 
		<!-- 导出数据库的owner或者schema -->
 		<property name="owner" value="DW"/>
		<!-- 导出创建表的前缀 -->
 		<property name="tablePrefix" value="DWMART."/> 
		<!-- 导出权限赋予用户 -->
 		<property name="grantUser" value=""/> 
		<!-- 导出创建表的engine针对mysql -->
 		<property name="engine" value=""/>
		<!-- 导出目标数据库类型 -->
 		<property name="toDbType" value="Greenplum"/>
		<!-- 导出源数据库的表名列表 -->
 		<property name="configFilePath" value="#{systemProperties['APP_PATH']}/conf/oraTable2Greenplum_user.txt"/>
		<!-- 导出创建语句的输出文件 -->
 		<property name="exportFilePath" value="#{systemProperties['APP_PATH']}/export/exportOracle2Greenplum.sql"/>
 	</bean>
 	
 	<!-- 配置执行的任务列表  -->
 	<util:list id="pipeLineTask" list-class="java.util.ArrayList">
 		<ref bean="dllExport"/>
 	</util:list>
</beans>
```