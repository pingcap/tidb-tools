#!groovy

node {
    fileLoader.withGit('git@github.com:pingcap/SRE.git', 'xiang/tools', 'github-iamxy-ssh', '') {
        fileLoader.load('jenkins/ci/pingcap_tidb_tools_branch.groovy').call()
    }
}
