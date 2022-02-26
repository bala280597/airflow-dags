pipeline{
    agent any
    environment {
        PROJECT_ID = 'essential-city-336316'
        CLUSTER_NAME = 'cluster-1'
        LOCATION = 'us-central1-c'
        CREDENTIALS_ID = 'essential-city-336316'
        DOCKER_USER = credentials('docker-user')
        DOCKER_PASS = credentials('docker-pass')
    }
    stages{
        stage('Checkout'){
            steps{
                checkout([$class: 'GitSCM',
                branches: [[name: '**']],
                extensions: [],
                userRemoteConfigs: [[
                    credentialsId: 'c94b22eb-6c7d-440b-b468-06679d537899',
                    url: 'https://github.com/bala280597/Nodejs.git']]])
            }
        }
        
        
        stage("Docker Build"){
          steps {
              script {
              if( (env.GIT_BRANCH.contains("test")) || (env.GIT_BRANCH.contains("develop")) || (env.GIT_BRANCH == "main") ) {
                  sh """ docker login -u $DOCKER_USER -p $DOCKER_PASS """
              if(env.GIT_BRANCH=="main"){
                 sh """
                        docker build -t bala2805/nodejs:main-${env.BUILD_ID} .
                        docker push bala2805/nodejs:main-${env.BUILD_ID}
                    """
              }
              if(env.GIT_BRANCH.contains("develop")){
                 sh """
                        docker build -t bala2805/nodejs:dev-${env.BUILD_ID} .
                        docker push bala2805/nodejs:dev-${env.BUILD_ID}
                    """
              }
              if(env.GIT_BRANCH.contains("test")) {
                 sh """
                        docker build -t bala2805/nodejs:test-${env.BUILD_ID} .
                        docker push bala2805/nodejs:test-${env.BUILD_ID}
                    """
               }
              }
             }
          }
        }
        
        
        stage("Kubernetes Deployment"){
          steps {
              script {
              if( (env.GIT_BRANCH.contains("test")) || (env.GIT_BRANCH.contains("develop")) || (env.GIT_BRANCH == "main") ) {
                  sh """ docker login -u $DOCKER_USER -p $DOCKER_PASS """
              if(env.GIT_BRANCH=="main"){
                   sh """
                        export IMAGE_NAME=bala2805/nodejs:main-${env.BUILD_ID}
                        export NAMESPACE=${env.GIT_BRANCH}
                        cat deploy.yml | envsubst > deployment.yml
                    """
                 }
               if(env.GIT_BRANCH.contains("develop")){
                   sh """
                        docker pull bala2805/nodejs:dev-${env.BUILD_ID}
                        export IMAGE_NAME=bala2805/nodejs:dev-${env.BUILD_ID}
                        export NAMESPACE=${env.GIT_BRANCH}
                        cat deploy.yml | envsubst > deployment.yml
                    """
                 }
                 if(env.GIT_BRANCH.contains("test")){
                   sh """
                        docker pull bala2805/nodejs:test-${env.BUILD_ID}
                        export IMAGE_NAME=bala2805/nodejs:test-${env.BUILD_ID}
                        export NAMESPACE=${env.GIT_BRANCH}
                        cat deploy.yml | envsubst > deployment.yml
                    """
                 }
                step([
                    $class: 'KubernetesEngineBuilder',
                    projectId: env.PROJECT_ID,
                    clusterName: env.CLUSTER_NAME,
                    location: env.LOCATION,
                    manifestPattern: 'deployment.yml',
                    credentialsId: env.CREDENTIALS_ID,
                    verifyDeployments: false])
                }
             }
          }
        }
        
    }
}