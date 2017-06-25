# CRSS Dataflow Application


Quick Start Guide
==========================



### Things you need to install

* JDK 1.8 
* Gradle 2.4.7 or above
* Docker 1.12 or above
* IDE of your choice. preferrably IntelliJ IDEA.
* Config Server and Config Repo
* PostgreSQL 9.4 or above


### Building the application

```
    $ gradle clean build -x test    
```

### Running the application

```
    $ gradle bootRun    
```


### Loading initial database schema

```
    $ cd buildtools
    $ gradle update
```

### Updating liquibase scripts

After doing model changes and executed `gradle update`, in buildtools folder, execute: 

```
    gradle diffChangelog
``` 

This will create a changelog file `changelog-1498402620542.groovy` on the `src/main/liquibase/2.0.0/schema` folder. 


> *where 1498402620542 is a system-generated timestamp and 2.0.0 is the version you are working on*


Review these changes and register this file to the base changelog file `src/main/liquibase/changelog.groovy`

```groovy
    databaseChangeLog {
        include(file: '2.0.0/schema/changelog.groovy', relativeToChangelogFile: 'true')
    
        // add succeeding changelog updates here
        include(file: '2.0.0/schema/changelog-1498402620542.groovy', relativeToChangelogFile: 'true')

    }
```


Execute: 

```
    gradle updateSQL
``` 

This will generate the sql script `src/main/liquibase/<version>/sql/dataflow-1498402620542.sql` to be used for execution to different database environments. 


To update your local database with the changes. Execute: 

```
    gradle update
```


Commit and/or push the changes to your working branch.




