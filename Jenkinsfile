@Library('ecdc-pipeline')
import ecdcpipeline.ContainerBuildNode
import ecdcpipeline.PipelineBuilder
import ecdcpipeline.NodeStatus

containerBuildNodes = [
  'centos': ContainerBuildNode.getDefaultContainerBuildNode('centos7')
]

pipelineBuilder = new PipelineBuilder(this, containerBuildNodes)
pipelineBuilder.activateEmailFailureNotifications()

builders = pipelineBuilder.createBuilders { container ->

  pipelineBuilder.stage("${container.key}: Clone") {
    dir(pipelineBuilder.project) {
      checkout scm
    }
    container.copyTo(pipelineBuilder.project, pipelineBuilder.project)
  }

  pipelineBuilder.stage("${container.key}: Setup") {
    container.sh """
      export LANG=en_US.UTF-8
      cd ${pipelineBuilder.project}
      python3.6 -m venv virtualenv
      virtualenv/bin/pip install --upgrade pip
      virtualenv/bin/pip install -r requirements-dev.txt
      virtualenv/bin/pip install .
    """
  }

  pipelineBuilder.stage("${container.key}: Test") {
    container.sh """
      cd ${pipelineBuilder.project}
      virtualenv/bin/pytest --junit-xml=test_results.xml
    """
    container.copyFrom("${pipelineBuilder.project}/test_results.xml", '.')
    junit 'test_results.xml'
  }

  pipelineBuilder.stage("${container.key}: Coverage") {
    path = pwd()
    container.sh """
      cd ${pipelineBuilder.project}
      virtualenv/bin/pytest --cov=utgardtests --cov-report=xml:coverage.xml
      jenkins/replace-path-and-name-in-coverage.sh "/home/jenkins/${pipelineBuilder.project}" "${path}/${pipelineBuilder.project}/src/utgardtests" utgardtests coverage.xml
    """
    container.copyFrom("${pipelineBuilder.project}/coverage.xml", "${pipelineBuilder.project}")
    dir(pipelineBuilder.project) {
      step([
        $class: 'CoberturaPublisher',
        autoUpdateHealth: true,
        autoUpdateStability: true,
        coberturaReportFile: 'coverage.xml',
        failUnhealthy: false,
        failUnstable: false,
        maxNumberOfBuilds: 0,
        onlyStable: false,
        sourceEncoding: 'ASCII',
        zoomCoverageChart: true
      ])
    }
  }
}

node {
  checkout scm
  try {
    parallel builders
    } catch (e) {
      pipelineBuilder.handleFailureMessages()
      throw e
  }
}
