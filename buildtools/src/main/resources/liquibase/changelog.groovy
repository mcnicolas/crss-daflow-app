databaseChangeLog {
    include(file: '2.0.0/schema/changelog.groovy', relativeToChangelogFile: 'true')

    // add succeeding changelog updates here
    include(file: '2.2.0/schema/changelog-1501669448827.groovy', relativeToChangelogFile: 'true')
    include(file: '2.2.0/schema/changelog-1505357200478.groovy', relativeToChangelogFile: 'true')
    include(file: '2.2.0/schema/changelog-1505357200479.groovy', relativeToChangelogFile: 'true')
}