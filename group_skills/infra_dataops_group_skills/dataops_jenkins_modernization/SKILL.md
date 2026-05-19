---
name: dataops-jenkins-modernization
description: Jenkins modernization for DataOps — migration from Freestyle to Declarative Pipeline, Jenkinsfile best practices, shared libraries (vars/src structure), parallel stages, Kubernetes agent pods (docker-in-docker vs kaniko), credential management (withCredentials/Jenkins Credential Store), pipeline-as-code with Multibranch Pipeline, Blue Ocean UI, migration path to GitHub Actions, JCasC (Jenkins Configuration as Code)
---

# Jenkins Modernization

## When to Use

- Migrating legacy Freestyle jobs to Declarative Pipeline
- Adding Kubernetes agents to eliminate static build slaves
- Extracting shared logic into Jenkins Shared Libraries
- Evaluating migration from Jenkins to GitHub Actions
- Auditing credential management (no plaintext secrets in Jenkinsfile)

---

## Declarative Pipeline Structure

```groovy
// Jenkinsfile
pipeline {
    agent {
        kubernetes {
            yaml """
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: python
    image: python:3.11-slim
    command: ['cat']
    tty: true
    resources:
      requests:
        cpu: 500m
        memory: 1Gi
  - name: kaniko
    image: gcr.io/kaniko-project/executor:debug
    command: ['cat']
    tty: true
"""
        }
    }

    options {
        timeout(time: 30, unit: 'MINUTES')
        buildDiscarder(logRotator(numToKeepStr: '20'))
        disableConcurrentBuilds()          // prevent parallel builds of same branch
        timestamps()
    }

    environment {
        IMAGE_REGISTRY = "ghcr.io/my-org"
        IMAGE_NAME     = "airflow"
    }

    stages {
        stage('Lint') {
            steps {
                container('python') {
                    sh '''
                        pip install ruff sqlfluff --quiet
                        ruff check .
                        sqlfluff lint dbt/models --dialect trino
                    '''
                }
            }
        }

        stage('Test') {
            parallel {
                stage('Unit Tests') {
                    steps {
                        container('python') {
                            sh 'pytest tests/unit/ -v --junitxml=test-unit.xml'
                        }
                    }
                }
                stage('DAG Validation') {
                    steps {
                        container('python') {
                            sh '''
                                pip install apache-airflow==2.8.0 --quiet
                                python -c "
                                from airflow.models import DagBag
                                bag = DagBag(dag_folder='dags/', include_examples=False)
                                assert not bag.import_errors
                                "
                            '''
                        }
                    }
                }
            }
        }

        stage('Build Image') {
            when {
                branch 'main'
            }
            steps {
                container('kaniko') {
                    withCredentials([usernamePassword(
                        credentialsId: 'ghcr-credentials',
                        usernameVariable: 'REGISTRY_USER',
                        passwordVariable: 'REGISTRY_PASS'
                    )]) {
                        sh '''
                            mkdir -p /kaniko/.docker
                            echo '{"auths":{"ghcr.io":{"auth":"'"$(echo -n ${REGISTRY_USER}:${REGISTRY_PASS} | base64)"'"}}}' \
                              > /kaniko/.docker/config.json

                            /kaniko/executor \
                              --context . \
                              --dockerfile Dockerfile \
                              --destination ${IMAGE_REGISTRY}/${IMAGE_NAME}:${GIT_COMMIT:0:7} \
                              --cache=true \
                              --cache-repo ${IMAGE_REGISTRY}/${IMAGE_NAME}-cache
                        '''
                    }
                }
            }
        }

        stage('Deploy Staging') {
            when { branch 'main' }
            steps {
                container('python') {
                    withCredentials([file(credentialsId: 'k8s-staging-kubeconfig', variable: 'KUBECONFIG')]) {
                        sh '''
                            kubectl set image deployment/airflow-scheduler \
                              scheduler=${IMAGE_REGISTRY}/${IMAGE_NAME}:${GIT_COMMIT:0:7} \
                              -n airflow-staging
                            kubectl rollout status deployment/airflow-scheduler -n airflow-staging
                        '''
                    }
                }
            }
        }
    }

    post {
        always {
            junit 'test-*.xml'
            cleanWs()
        }
        failure {
            slackSend(
                channel: '#data-eng-alerts',
                color: 'danger',
                message: "Pipeline FAILED: ${env.JOB_NAME} #${env.BUILD_NUMBER}\n${env.BUILD_URL}"
            )
        }
        success {
            archiveArtifacts artifacts: 'dbt/target/manifest.json', fingerprint: true
        }
    }
}
```

---

## Shared Library Structure

```
jenkins-shared-library/
  vars/
    dbtRun.groovy       # callable as dbtRun() in Jenkinsfile
    dockerBuild.groovy
    slackNotify.groovy
  src/
    org/myorg/
      DbtHelper.groovy  # class-based helpers
      SlackClient.groovy
  resources/
    pod-templates/
      python-pod.yaml
      spark-pod.yaml
```

```groovy
// vars/dbtRun.groovy
def call(Map config = [:]) {
    def target  = config.get('target', 'ci')
    def select  = config.get('select', '')
    def baseDir = config.get('baseDir', 'dbt')

    container('python') {
        withCredentials([
            string(credentialsId: 'trino-host', variable: 'DBT_TRINO_HOST'),
            usernamePassword(credentialsId: 'trino-creds',
                usernameVariable: 'DBT_TRINO_USER',
                passwordVariable: 'DBT_TRINO_PASSWORD')
        ]) {
            sh """
                cd ${baseDir}
                dbt deps
                dbt run --target ${target} ${select ? '--select ' + select : ''}
                dbt test --target ${target} ${select ? '--select ' + select : ''}
            """
        }
    }
}
```

```groovy
// Jenkinsfile — using shared library
@Library('jenkins-shared-library@main') _

pipeline {
    agent any
    stages {
        stage('dbt') {
            steps {
                dbtRun(target: 'staging', select: 'state:modified+')
            }
        }
    }
}
```

---

## Multibranch Pipeline (Jenkinsfile Discovery)

```groovy
// Jenkins Job DSL or via UI:
// New Item → Multibranch Pipeline
// Branch Sources → GitHub
// Build Configuration → Jenkinsfile path: Jenkinsfile

// Per-branch behavior in Jenkinsfile:
pipeline {
    agent any
    stages {
        stage('Test') {
            steps { sh 'pytest tests/' }
        }
        stage('Deploy') {
            when {
                anyOf {
                    branch 'main'
                    branch 'release/*'
                }
            }
            steps {
                sh './scripts/deploy.sh ${BRANCH_NAME}'
            }
        }
    }
}
```

---

## Credential Management Best Practices

```groovy
// ✅ Always use withCredentials — never echo secrets or store in env
withCredentials([
    string(credentialsId: 'airflow-fernet-key', variable: 'FERNET_KEY'),
    usernamePassword(credentialsId: 'aws-creds',
        usernameVariable: 'AWS_ACCESS_KEY_ID',
        passwordVariable: 'AWS_SECRET_ACCESS_KEY')
]) {
    sh 'python deploy.py'   // FERNET_KEY, AWS_* are available only in this block
}

// ✅ File credentials for kubeconfig, service accounts
withCredentials([file(credentialsId: 'gcp-service-account', variable: 'GOOGLE_APPLICATION_CREDENTIALS')]) {
    sh 'gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS'
}

// ❌ Never do this:
// environment {
//     AWS_SECRET = "hardcoded-secret"  // visible in build logs, Jenkinsfile
// }
```

---

## Jenkins Configuration as Code (JCasC)

```yaml
# jenkins.yaml — managed by JCasC plugin
jenkins:
  systemMessage: "Data Platform Jenkins"
  numExecutors: 0      # all builds run on agents, not master

  clouds:
    - kubernetes:
        name: "kubernetes"
        serverUrl: "https://kubernetes.default.svc"
        namespace: "jenkins"
        jenkinsUrl: "http://jenkins.jenkins.svc.cluster.local:8080"
        podRetention: "never"
        templates:
          - name: "python-agent"
            label: "python"
            containers:
              - name: "python"
                image: "python:3.11-slim"
                command: "cat"
                ttyEnabled: true

credentials:
  system:
    domainCredentials:
      - credentials:
          - usernamePassword:
              id: "ghcr-credentials"
              username: "${GHCR_USER}"
              password: "${GHCR_TOKEN}"    # injected from Kubernetes secret
              scope: GLOBAL
```

---

## Migration Path to GitHub Actions

| Jenkins Concept | GitHub Actions Equivalent |
|-----------------|--------------------------|
| Jenkinsfile | `.github/workflows/*.yml` |
| Shared Library | Reusable workflows / composite actions |
| Kubernetes agent pod | `runs-on: [self-hosted, k8s]` |
| `withCredentials` | `secrets: GITHUB_TOKEN` + OIDC |
| Multibranch Pipeline | Workflow triggers with `branches:` filter |
| Blue Ocean | GitHub Actions UI |
| `post { failure {} }` | `if: failure()` job step |
| `disableConcurrentBuilds()` | `concurrency: cancel-in-progress: true` |

**Migration decision criteria:**
- Migrate if: team already uses GitHub, pipeline is < 200 lines, no complex shared library
- Stay on Jenkins if: on-prem with air-gap, heavy shared library investment, complex plugin ecosystem

---

## Anti-Patterns

1. **Freestyle jobs with shell scripts** — no code review, no history, not reproducible; migrate to Declarative Pipeline in Jenkinsfile.
2. **Secrets in Jenkinsfile environment block** — stored in plaintext in VCS; use `withCredentials` with Jenkins Credential Store.
3. **Long-running builds on Jenkins master** — master handles scheduling, not execution; all builds should use pod agents.
4. **No `disableConcurrentBuilds()`** — two developers push simultaneously, both builds deploy to the same environment; add concurrent build prevention.
5. **Not archiving dbt manifest.json** — slim CI requires the manifest; archive it as a build artifact and download in subsequent runs.

---

## References

- Jenkins Declarative Pipeline: `jenkins.io/doc/book/pipeline/syntax/`
- Jenkins Shared Libraries: `jenkins.io/doc/book/pipeline/shared-libraries/`
- JCasC plugin: `plugins.jenkins.io/configuration-as-code/`
- Kaniko (rootless Docker builds): `github.com/GoogleContainerTools/kaniko`
- Related skills: `[[dataops-cicd-pipeline-review]]`, `[[infra-docker-best-practices]]`, `[[dataops-github-actions-optimizer]]`
