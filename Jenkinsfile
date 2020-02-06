pipeline {
    agent any

    tools {
        go 'go-1.13'
    }

    stages {
        stage('run tests') {
            steps {
                dir("tests") {
                    sh 'go test -v'
                }
            }
        }
    }

    post {
        success {
            script {
                currentBuild.result = 'SUCCESS'
                notifyBitbucket()
            }
        }

        failure {
            script {
                currentBuild.result = 'FAILED'
                notifyBitbucket()
            }
        }
    }
}
