Project Structure/Project Settings/Project -> jdk 1.8
Settings/Build, Execution, Deployment/Compiler/Scala Compiler/Scala Compile Server -> JVM JDK jdk 1.8

export JAVA_HOME=$(/usr/libexec/java_home -v1.8)
export PATH=$JAVA_HOME/bin:$PATH
mvn -version
mvn clean install

