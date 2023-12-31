databaseChangeLog {
    include(file: '2.0.0/schema/changelog.groovy', relativeToChangelogFile: 'true')

    // add succeeding changelog updates here
    include(file: '2.2.0/schema/changelog-1501669448827.groovy', relativeToChangelogFile: 'true')
    include(file: '2.2.0/schema/changelog-1505357200478.groovy', relativeToChangelogFile: 'true')
    include(file: '2.2.0/schema/changelog-1505357200479.groovy', relativeToChangelogFile: 'true')
    include(file: '2.2.0/schema/changelog-1511840276753.groovy', relativeToChangelogFile: 'true')

    include(file: '2.3.0/schema/changelog-1506580616475.groovy', relativeToChangelogFile: 'true')
    include(file: '2.3.0/schema/changelog-1509966984705.groovy', relativeToChangelogFile: 'true')
    include(file: '2.3.0/schema/changelog-1512625182857.groovy', relativeToChangelogFile: 'true')

    // sow13 - 2.6.0
    include(file: '2.6.0/schema/changelog-20180614.groovy', relativeToChangelogFile: 'true')
    include(file: '2.6.0/schema/changelog-1528936790096.groovy', relativeToChangelogFile: 'true')
    include(file: '2.6.0/schema/changelog-1529561667951.groovy', relativeToChangelogFile: 'true')
    include(file: '2.6.0/schema/changelog-1531185960684.groovy', relativeToChangelogFile: 'true')
    include(file: '2.6.0/schema/changelog-20180711.groovy', relativeToChangelogFile: 'true')


    include(file: '2.8.0/schema/changelog-20191104.groovy', relativeToChangelogFile: 'true')
}