# example-spark-job
A simple spark job to test deployment, oozie and emr configurations.

Notes on classpath collisions:

The class path provided on the EMR machines in YARN is complex. Likely as we add more jars to the shadowjar
we will find version conflicts. The primary way to avoid this is to exclude the conflicting jars from
the shadow jar and let the EMR jars provide the dependency.

Example:

```
shadowJar {
  zip64 true
  mergeServiceFiles()
  dependencies {
    exclude(dependency('com.fasterxml.jackson.core::'))
    exclude(dependency('com.fasterxml.jackson.module::'))
    exclude(dependency('com.fasterxml.jackson.dataformat::'))
  }
}
```

This excludes all jars/all versions from these three groups in jackson from the shadowjar.



