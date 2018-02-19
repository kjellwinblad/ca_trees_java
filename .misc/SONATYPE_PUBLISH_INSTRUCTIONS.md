


1. Set up plugins using the scripts `.misc/add_plugins.sh` and `.misc/create_sonatype_login_info.sh`.
3. Comment out test dependencies
4. run `sbt`
5. run `set PgpKeys.useGpg in Global := true` in the sbt console
6. run `set PgpKeys.gpgCommand in Global := "gpg2"` in the sbt console
7. run `publishSigned` in the sbt console
8. run `sonatypeRelease` in the sbt console
9. Comment back test dependencies

# Sources

https://github.com/sbt/sbt-pgp/issues/72
https://leonard.io/blog/2017/01/an-in-depth-guide-to-deploying-to-maven-central/