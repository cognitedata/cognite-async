@Library('jenkins-helpers') _


testBuildReleasePoetryPackage {
    releaseToArtifactory = true
    uploadCoverageReport = true
    testWithTox = true
    toxEnvList = ['py36', 'py37', 'py38']
    extraEnvVars = [
		secretEnvVar(key: 'COGNITE_API_KEY', secretName: 'cognite-async', secretKey: 'integration-test-api-key'),
		envVar(key: 'COGNITE_BASE_URL', value: "https://greenfield.cognitedata.com"),
		envVar(key: 'COGNITE_CLIENT_NAME', value: "cognite-async-integration-tests"),
		envVar(key: 'COGNITE_PROJECT', value: "sander"),
	]
}
