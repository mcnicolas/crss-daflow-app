#!groovy​
pipeline {
    agent any

    stages {
        stage('Shared: Prepare') {
            steps {
                gitlabCommitStatus {
                    git branch: "${env.gitlabSourceBranch}", url: 'git@gitlab.exist.com:pemc/shared.git'
                }
            }
        }

        stage('Shared: Publish') {
            steps {
                gitlabCommitStatus {
                    echo 'Publishing..'
                    sh "./gradlew clean build -x test publish"
                }
            }
        }

        stage('Dataflow App: Prepare') {
            steps {
                gitlabCommitStatus {
                    git branch: "${env.gitlabSourceBranch}", url: 'git@gitlab.exist.com:pemc/dataflow-app.git'
                }
            }
        }

        stage('Dataflow App: Build') {
            steps {
                gitlabCommitStatus {
                    echo 'Building..'
                    sh "/jenkins/functions/deleteGradleCache.sh com.pemc.crss"
                    sh "./gradlew clean build -x test"
                }
            }
        }
    }
}