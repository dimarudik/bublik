https://stackoverflow.com/questions/4955635/how-to-add-local-jar-files-to-a-maven-project
```
mvn install:install-file -Dfile=./target/bublik-1.2.jar -DgroupId=org.bublik -DartifactId=bublik -Dversion=1.2 -Dpackaging=jar -DgeneratePom=true
```
