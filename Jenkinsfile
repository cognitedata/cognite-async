@Library('jenkins-helpers@v0.1.36') _

def label = "cognite-async-${UUID.randomUUID().toString()}"

podTemplate(
    label: label,
    annotations: [
            podAnnotation(key: "jenkins/build-url", value: env.BUILD_URL ?: ""),
            podAnnotation(key: "jenkins/github-pr-url", value: env.CHANGE_URL ?: ""),
    ],
    containers: [
        containerTemplate(name: 'python',
            image: 'eu.gcr.io/cognitedata/multi-python:7040fac',
            command: '/bin/cat -',
            resourceRequestCpu: '1000m',
            resourceRequestMemory: '800Mi',
            resourceLimitCpu: '1000m',
            resourceLimitMemory: '800Mi',
            ttyEnabled: true),
        containerTemplate(name: 'node',
            image: 'node:slim',
            command: '/bin/cat -',
            resourceRequestCpu: '300m',
            resourceRequestMemory: '300Mi',
            resourceLimitCpu: '300m',
            resourceLimitMemory: '300Mi',
            ttyEnabled: true),
    ],
    volumes: [
        secretVolume(secretName: 'jenkins-docker-builder', mountPath: '/jenkins-docker-builder', readOnly: true),
        secretVolume(secretName: 'pypi-credentials', mountPath: '/pypi', readOnly: true),
    ],
    envVars: [
        secretEnvVar(key: 'COGNITE_API_KEY', secretName: 'cognite-async', secretKey: 'integration-test-api-key'),
        envVar(key: 'COGNITE_BASE_URL', value: "https://greenfield.cognitedata.com"),
        envVar(key: 'COGNITE_CLIENT_NAME', value: "cognite-async-integration-tests"),
        envVar(key: 'COGNITE_PROJECT', value: "sander"),
    ]) {
    node(label) {
        def gitCommit
        container('jnlp') {
            stage('Checkout') {
                checkout(scm)
                gitCommit = sh(returnStdout: true, script: 'git rev-parse --short HEAD').trim()
            }
        }
        container('python') {
            stage('Install pipenv and black') {
                sh("pip3 install pipenv black")
            }
            stage('Install core dependencies') {
                sh("pipenv run pip install -r requirements.txt")
            }
            stage('Check code') {
                sh("pipenv run black -l 120 --check .")
            }
            stage('Build Docs'){
                sh("pipenv run pip install .")
                dir('./docs'){
                    sh("pipenv run sphinx-build -W -b html ./source ./build")
                }
            }
            stage('Test OpenAPI Generator'){
                sh('pipenv run pytest openapi/tests')
            }
            stage('Test Client') {
                sh("pyenv local 3.5.0 3.6.6 3.7.2")
                sh("pipenv run tox -p auto")
                junit(allowEmptyResults: true, testResults: '**/test-report.xml')
                summarizeTestResults()
            }
            stage('Build') {
                sh("python3 setup.py sdist")
                sh("python3 setup.py bdist_wheel")
            }
        }
    }
}
