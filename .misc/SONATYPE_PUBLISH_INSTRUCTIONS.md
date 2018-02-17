


1. Set up plugins using the scripts `.misc/add_plugins.sh` and `.misc/create_sonatype_login_info.sh`.
2. run `sbt`
3. run `set PgpKeys.useGpg in Global := true` in the sbt console
4. run `set PgpKeys.gpgCommand in Global := "gpg2"` in the sbt console
5. run `publishSigned` in the sbt console
6. run `sonatypeRelease` in the sbt console


# Sources

https://github.com/sbt/sbt-pgp/issues/72
https://leonard.io/blog/2017/01/an-in-depth-guide-to-deploying-to-maven-central/