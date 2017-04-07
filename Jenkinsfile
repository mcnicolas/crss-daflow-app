#!groovy​
pipeline {
    agent any

    stages {
        stage('Dataflow App: Prepare') {
            steps {
                git branch: 'frapuccino', url: 'git@gitlab.exist.com:pemc/dataflow-app.git'
            }
        }

        stage('Dataflow App: Build and Push') {
            steps {
                echo 'Building..'
                sh "./gradlew clean build buildDockerImage pushDockerImage -x test"
            }
        }

        stage('Dataflow App: Deploy') {
            steps {
                echo 'Building..'
                sh "./gradlew stopApp deployApp -x test -PregistryUrl=pemc.medcurial.com -PmarathonUrl=http://crss-master.pemc.exist.com/marathon/v2/apps -PappProfile=dev2,skipSSLA"
            }
        }
    }
}