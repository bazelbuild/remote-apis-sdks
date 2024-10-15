load("@bazel_gazelle//:deps.bzl", "go_repository")

def remote_apis_sdks_go_deps():
    go_repository(
        name = "com_github_bazelbuild_remote_apis",
        importpath = "github.com/bazelbuild/remote-apis",
        sum = "h1:PDqlk0P5XldzQr3muwfBsu88Aayjv1gSRGyQqsI7uGA=",
        version = "v0.0.0-20240926071355-6777112ef7de",
        repo_mapping = {
            "@rules_go": "@io_bazel_rules_go",
            "@grpc": "@com_github_grpc_grpc",
        }
    )

    go_repository(
        name = "com_github_census_instrumentation_opencensus_proto",
        importpath = "github.com/census-instrumentation/opencensus-proto",
        sum = "h1:iKLQ0xPNFxR/2hzXZMrBo8f1j86j5WHzznCCQxV/b8g=",
        version = "v0.4.1",
    )
    go_repository(
        name = "com_github_cespare_xxhash_v2",
        importpath = "github.com/cespare/xxhash/v2",
        sum = "h1:UL815xU9SqsFlibzuggzjXhog7bL6oX9BbNZnL2UFvs=",
        version = "v2.3.0",
    )

    go_repository(
        name = "com_github_cncf_xds_go",
        importpath = "github.com/cncf/xds/go",
        sum = "h1:ga8SEFjZ60pxLcmhnThWgvH2wg8376yUJmPhEH4H3kw=",
        version = "v0.0.0-20240423153145-555b57ec207b",
    )

    go_repository(
        name = "com_github_envoyproxy_go_control_plane",
        importpath = "github.com/envoyproxy/go-control-plane",
        sum = "h1:4X+VP1GHd1Mhj6IB5mMeGbLCleqxjletLK6K0rbxyZI=",
        version = "v0.12.0",
    )
    go_repository(
        name = "com_github_envoyproxy_protoc_gen_validate",
        importpath = "github.com/envoyproxy/protoc-gen-validate",
        sum = "h1:gVPz/FMfvh57HdSJQyvBtF00j8JU4zdyUgIUNhlgg0A=",
        version = "v1.0.4",
    )
    go_repository(
        name = "com_github_felixge_httpsnoop",
        importpath = "github.com/felixge/httpsnoop",
        sum = "h1:NFTV2Zj1bL4mc9sqWACXbQFVBBg2W3GPvqp8/ESS2Wg=",
        version = "v1.0.4",
    )
    go_repository(
        name = "com_github_go_logr_logr",
        importpath = "github.com/go-logr/logr",
        sum = "h1:6pFjapn8bFcIbiKo3XT4j/BhANplGihG6tvd+8rYgrY=",
        version = "v1.4.2",
    )
    go_repository(
        name = "com_github_go_logr_stdr",
        importpath = "github.com/go-logr/stdr",
        sum = "h1:hSWxHoqTgW2S2qGc0LTAI563KZ5YKYRhT3MFKZMbjag=",
        version = "v1.2.2",
    )

    go_repository(
        name = "com_github_golang_glog",
        importpath = "github.com/golang/glog",
        sum = "h1:OptwRhECazUx5ix5TTWC3EZhsZEHWcYWY4FQHTIubm4=",
        version = "v1.2.1",
    )
    go_repository(
        name = "com_github_golang_groupcache",
        importpath = "github.com/golang/groupcache",
        sum = "h1:oI5xCqsCo564l8iNU+DwB5epxmsaqB+rhGL0m5jtYqE=",
        version = "v0.0.0-20210331224755-41bb18bfe9da",
    )

    go_repository(
        name = "com_github_golang_protobuf",
        importpath = "github.com/golang/protobuf",
        sum = "h1:i7eJL8qZTpSEXOPTxNKhASYpMn+8e5Q6AdndVa1dWek=",
        version = "v1.5.4",
    )

    go_repository(
        name = "com_github_google_go_cmp",
        importpath = "github.com/google/go-cmp",
        sum = "h1:ofyhxvXcZhMsU5ulbFiLKl/XBFqE1GSq7atu8tAmTRI=",
        version = "v0.6.0",
    )
    go_repository(
        name = "com_github_google_s2a_go",
        importpath = "github.com/google/s2a-go",
        sum = "h1:zZDs9gcbt9ZPLV0ndSyQk6Kacx2g/X+SKYovpnz3SMM=",
        version = "v0.1.8",
    )

    go_repository(
        name = "com_github_google_uuid",
        importpath = "github.com/google/uuid",
        sum = "h1:NIvaJDMOsjHA8n1jAhLSgzrAzy1Hgr+hNrb57e+94F0=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_github_googleapis_enterprise_certificate_proxy",
        importpath = "github.com/googleapis/enterprise-certificate-proxy",
        sum = "h1:Vie5ybvEvT75RniqhfFxPRy3Bf7vr3h0cechB90XaQs=",
        version = "v0.3.2",
    )
    go_repository(
        name = "com_github_googleapis_gax_go_v2",
        importpath = "github.com/googleapis/gax-go/v2",
        sum = "h1:yitjD5f7jQHhyDsnhKEBU52NdvvdSeGzlAnDPT0hH1s=",
        version = "v2.13.0",
    )
    go_repository(
        name = "com_github_klauspost_compress",
        importpath = "github.com/klauspost/compress",
        sum = "h1:YcnTYrq7MikUT7k0Yb5eceMmALQPYBW/Xltxn0NAMnU=",
        version = "v1.17.8",
    )
    go_repository(
        name = "com_github_pkg_xattr",
        importpath = "github.com/pkg/xattr",
        sum = "h1:FSoblPdYobYoKCItkqASqcrKCxRn9Bgurz0sCBwzO5g=",
        version = "v0.4.4",
    )

    go_repository(
        name = "com_github_stretchr_testify",
        importpath = "github.com/stretchr/testify",
        sum = "h1:HtqpIVDClZ4nwg75+f6Lvsy/wHu+3BoSGCbBAcpTsTg=",
        version = "v1.9.0",
    )

    go_repository(
        name = "com_google_cloud_go",
        importpath = "cloud.google.com/go",
        sum = "h1:CnFSK6Xo3lDYRoBKEcAtia6VSC837/ZkJuRduSFnr14=",
        version = "v0.115.0",
    )
    go_repository(
        name = "com_google_cloud_go_accessapproval",
        importpath = "cloud.google.com/go/accessapproval",
        sum = "h1:MgtE8CI+YJWPGGHnxQ9z1VQqV87h+vSGy2MeM/m0ggQ=",
        version = "v1.7.11",
    )
    go_repository(
        name = "com_google_cloud_go_accesscontextmanager",
        importpath = "cloud.google.com/go/accesscontextmanager",
        sum = "h1:IQ3KLJmNKPgstN0ZcRw0niU4KfsiOZmzvcGCF+NT618=",
        version = "v1.8.11",
    )
    go_repository(
        name = "com_google_cloud_go_aiplatform",
        importpath = "cloud.google.com/go/aiplatform",
        sum = "h1:EPPqgHDJpBZKRvv+OsB3cr0jYz3EL2pZ+802rBPcG8U=",
        version = "v1.68.0",
    )
    go_repository(
        name = "com_google_cloud_go_analytics",
        importpath = "cloud.google.com/go/analytics",
        sum = "h1:BY8ZY7hQwKBi+lNp1IkiMTOK4xe4lxZCeYv3S9ARXtE=",
        version = "v0.23.6",
    )
    go_repository(
        name = "com_google_cloud_go_apigateway",
        importpath = "cloud.google.com/go/apigateway",
        sum = "h1:VtEvpnqqY2T5gZBzo+p7C87yGH3omHUkPIbRQkmGS9I=",
        version = "v1.6.11",
    )
    go_repository(
        name = "com_google_cloud_go_apigeeconnect",
        importpath = "cloud.google.com/go/apigeeconnect",
        sum = "h1:CftZgGXFRLJeD2/5ZIdWuAMxW/88UG9tHhRPI/NY75M=",
        version = "v1.6.11",
    )
    go_repository(
        name = "com_google_cloud_go_apigeeregistry",
        importpath = "cloud.google.com/go/apigeeregistry",
        sum = "h1:3vLwk0tS9L++6ZyV4RDH4UCydfVoqxJbpWvqG6MTtUw=",
        version = "v0.8.9",
    )

    go_repository(
        name = "com_google_cloud_go_appengine",
        importpath = "cloud.google.com/go/appengine",
        sum = "h1:ZLoWWwakgRzRnXX2bsgk2g1sdzti3wq+ebunTJsZNog=",
        version = "v1.8.11",
    )
    go_repository(
        name = "com_google_cloud_go_area120",
        importpath = "cloud.google.com/go/area120",
        sum = "h1:UID1dl7lW2zs8OpYVtVZ5WsXU9kUcxC1nd3nnToHW70=",
        version = "v0.8.11",
    )
    go_repository(
        name = "com_google_cloud_go_artifactregistry",
        importpath = "cloud.google.com/go/artifactregistry",
        sum = "h1:NNK4vYVA5NGQmbmYidfJhnfmYU6SSSRUM2oopNouJNs=",
        version = "v1.14.13",
    )
    go_repository(
        name = "com_google_cloud_go_asset",
        importpath = "cloud.google.com/go/asset",
        sum = "h1:/R2XZS6lR8oj/Y3L+epD2yy7mf44Zp62H4xZ4vzaR/Y=",
        version = "v1.19.5",
    )
    go_repository(
        name = "com_google_cloud_go_assuredworkloads",
        importpath = "cloud.google.com/go/assuredworkloads",
        sum = "h1:pwZp9o8aF5QmX4Z0YNlRe1ZOUzDw0UALmkem3aPobZc=",
        version = "v1.11.11",
    )
    go_repository(
        name = "com_google_cloud_go_auth",
        importpath = "cloud.google.com/go/auth",
        sum = "h1:y8jUJLl/Fg+qNBWxP/Hox2ezJvjkrPb952PC1p0G6A4=",
        version = "v0.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_auth_oauth2adapt",
        importpath = "cloud.google.com/go/auth/oauth2adapt",
        sum = "h1:MlxF+Pd3OmSudg/b1yZ5lJwoXCEaeedAguodky1PcKI=",
        version = "v0.2.3",
    )

    go_repository(
        name = "com_google_cloud_go_automl",
        importpath = "cloud.google.com/go/automl",
        sum = "h1:FBCLjGS+Did/wtRHqyS055bRs/EJXx3meTvHPcdZgk8=",
        version = "v1.13.11",
    )
    go_repository(
        name = "com_google_cloud_go_baremetalsolution",
        importpath = "cloud.google.com/go/baremetalsolution",
        sum = "h1:VvBiXT9QJ4VpNVyfzHhLScY1aymZxpQgOa20yUvgphw=",
        version = "v1.2.10",
    )
    go_repository(
        name = "com_google_cloud_go_batch",
        importpath = "cloud.google.com/go/batch",
        sum = "h1:o1RAjc0ExGAAm41YB9LbJZyJDgZR4M6SKyITsd/Smr4=",
        version = "v1.9.2",
    )
    go_repository(
        name = "com_google_cloud_go_beyondcorp",
        importpath = "cloud.google.com/go/beyondcorp",
        sum = "h1:K4blSIQZn3YO4F4LmvWrH52pb8Y0L3NOrwkf22+x67M=",
        version = "v1.0.10",
    )
    go_repository(
        name = "com_google_cloud_go_bigquery",
        importpath = "cloud.google.com/go/bigquery",
        sum = "h1:SYEA2f7fKqbSRRBHb7g0iHTtZvtPSPYdXfmqsjpsBwo=",
        version = "v1.62.0",
    )
    go_repository(
        name = "com_google_cloud_go_bigtable",
        importpath = "cloud.google.com/go/bigtable",
        sum = "h1:OF+V7OrVPhsXy++iTHewE4VD1kv6ikWQJbRIiq1/Kjc=",
        version = "v1.27.2-0.20240730134218-123c88616251",
    )

    go_repository(
        name = "com_google_cloud_go_billing",
        importpath = "cloud.google.com/go/billing",
        sum = "h1:sGRWx7PvsfHuZyx151Xr6CrORIgjvCMO4GRabihSdQQ=",
        version = "v1.18.9",
    )
    go_repository(
        name = "com_google_cloud_go_binaryauthorization",
        importpath = "cloud.google.com/go/binaryauthorization",
        sum = "h1:ItT9uR/0/ok2Ru3LCcbSIBUPsKqTA49ZmxCupqQaeFo=",
        version = "v1.8.7",
    )
    go_repository(
        name = "com_google_cloud_go_certificatemanager",
        importpath = "cloud.google.com/go/certificatemanager",
        sum = "h1:ASC9N81NU8JnGzi9kiY2QTqtTgOziwGv48sjt3YG420=",
        version = "v1.8.5",
    )
    go_repository(
        name = "com_google_cloud_go_channel",
        importpath = "cloud.google.com/go/channel",
        sum = "h1:AkKyMl2pSoJxBQtjAd6LYOtMgOaCl/kuiKoSg/Gf/H4=",
        version = "v1.17.11",
    )
    go_repository(
        name = "com_google_cloud_go_cloudbuild",
        importpath = "cloud.google.com/go/cloudbuild",
        sum = "h1:RvK5r8JBCLNg9XmfGPy05t3bmhLJV3Xh3sDHGHAATgM=",
        version = "v1.16.5",
    )
    go_repository(
        name = "com_google_cloud_go_clouddms",
        importpath = "cloud.google.com/go/clouddms",
        sum = "h1:EA3y9v5TZiAlwgHJh2vPOEelqYiCxXBYZRCNnGK5q+g=",
        version = "v1.7.10",
    )
    go_repository(
        name = "com_google_cloud_go_cloudtasks",
        importpath = "cloud.google.com/go/cloudtasks",
        sum = "h1:p91Brp4nJkyRRI/maYdO+FT+e9tU+2xoGr20s2rvalU=",
        version = "v1.12.12",
    )
    go_repository(
        name = "com_google_cloud_go_compute",
        importpath = "cloud.google.com/go/compute",
        sum = "h1:XM8ulx6crjdl09XBfji7viFgZOEQuIxBwKmjRH9Rtmc=",
        version = "v1.27.4",
    )
    go_repository(
        name = "com_google_cloud_go_compute_metadata",
        importpath = "cloud.google.com/go/compute/metadata",
        sum = "h1:Zr0eK8JbFv6+Wi4ilXAR8FJ3wyNdpxHKJNPos6LTZOY=",
        version = "v0.5.0",
    )
    go_repository(
        name = "com_google_cloud_go_contactcenterinsights",
        importpath = "cloud.google.com/go/contactcenterinsights",
        sum = "h1:LRcI5RAlLIbjwT312sGt+gyXcaXTr+v7uEQlNyArO9g=",
        version = "v1.13.6",
    )
    go_repository(
        name = "com_google_cloud_go_container",
        importpath = "cloud.google.com/go/container",
        sum = "h1:GP5zLamfvPZeOTifnGBSER/br76D5eJ97xhcXXrh5tM=",
        version = "v1.38.0",
    )
    go_repository(
        name = "com_google_cloud_go_containeranalysis",
        importpath = "cloud.google.com/go/containeranalysis",
        sum = "h1:Xb8Eu7vVmWR5nAl5WPTGTx/dCr+R+oF7VbuYV47EHHs=",
        version = "v0.12.1",
    )
    go_repository(
        name = "com_google_cloud_go_datacatalog",
        importpath = "cloud.google.com/go/datacatalog",
        sum = "h1:Cosg/L60myEbpP1HoNv77ykV7zWe7hqSwY4uUDmhx/I=",
        version = "v1.20.5",
    )
    go_repository(
        name = "com_google_cloud_go_dataflow",
        importpath = "cloud.google.com/go/dataflow",
        sum = "h1:YIhStasKFDESaUdpnsHsp/5bACYL/yvW0OuZ6zPQ6nY=",
        version = "v0.9.11",
    )
    go_repository(
        name = "com_google_cloud_go_dataform",
        importpath = "cloud.google.com/go/dataform",
        sum = "h1:oNtTx9PdH7aPnvrKIsPrh+Y6Mw+8Bw5/ZgLWVHAev/c=",
        version = "v0.9.8",
    )
    go_repository(
        name = "com_google_cloud_go_datafusion",
        importpath = "cloud.google.com/go/datafusion",
        sum = "h1:GVcVisjVKmoj1eNnIp3G3qjjo+7koHr0Kf8tF6Cjqe0=",
        version = "v1.7.11",
    )
    go_repository(
        name = "com_google_cloud_go_datalabeling",
        importpath = "cloud.google.com/go/datalabeling",
        sum = "h1:7jSuJEAc7upeMmyICzqfU0OyxUV38JSWW+8r5GmoHX0=",
        version = "v0.8.11",
    )
    go_repository(
        name = "com_google_cloud_go_dataplex",
        importpath = "cloud.google.com/go/dataplex",
        sum = "h1:bIU1r1YnsX6P1qTnaRnah/STHoLJ3EHUZVCjJl2+1Eo=",
        version = "v1.18.2",
    )
    go_repository(
        name = "com_google_cloud_go_dataproc_v2",
        importpath = "cloud.google.com/go/dataproc/v2",
        sum = "h1:OgTfUARkF8AfkNmoyT0wyLLXNh4LbT3l55s5gUlvFOk=",
        version = "v2.5.3",
    )

    go_repository(
        name = "com_google_cloud_go_dataqna",
        importpath = "cloud.google.com/go/dataqna",
        sum = "h1:bEUidOYRS0EQ7qHbZtcnospuks72iTapboszXU9poz8=",
        version = "v0.8.11",
    )
    go_repository(
        name = "com_google_cloud_go_datastore",
        importpath = "cloud.google.com/go/datastore",
        sum = "h1:6Me8ugrAOAxssGhSo8im0YSuy4YvYk4mbGvCadAH5aE=",
        version = "v1.17.1",
    )

    go_repository(
        name = "com_google_cloud_go_datastream",
        importpath = "cloud.google.com/go/datastream",
        sum = "h1:klGhjQCLoLIRHMzMFIqM73cPNKliGveqC+Vrms+ce6A=",
        version = "v1.10.10",
    )
    go_repository(
        name = "com_google_cloud_go_deploy",
        importpath = "cloud.google.com/go/deploy",
        sum = "h1:nQfMNeCPPSCrAUrG9AYAbiwxIK2+snTkVWW17AfCSwo=",
        version = "v1.20.0",
    )
    go_repository(
        name = "com_google_cloud_go_dialogflow",
        importpath = "cloud.google.com/go/dialogflow",
        sum = "h1:H28O0WAm2waHpNAz2n9jbv8FApfXxeKAkfHObdP2MMk=",
        version = "v1.55.0",
    )
    go_repository(
        name = "com_google_cloud_go_dlp",
        importpath = "cloud.google.com/go/dlp",
        sum = "h1:U0/IWY/X8b7tg4yTHYTcTs86FA9esshHYSutYWDOGgg=",
        version = "v1.15.0",
    )
    go_repository(
        name = "com_google_cloud_go_documentai",
        importpath = "cloud.google.com/go/documentai",
        sum = "h1:AUwiPqALsAkY2WYdr5BqmGrdMGDeeY1GFjd+ilkpexA=",
        version = "v1.30.5",
    )
    go_repository(
        name = "com_google_cloud_go_domains",
        importpath = "cloud.google.com/go/domains",
        sum = "h1:8peNiXtaMNIF9Wybci859M/yprFcEve1R2z08pErUBs=",
        version = "v0.9.11",
    )
    go_repository(
        name = "com_google_cloud_go_edgecontainer",
        importpath = "cloud.google.com/go/edgecontainer",
        sum = "h1:wTo0ulZDSsDzeoVjICJZjZMzZ1Nn9y//AwAQlXbaTbs=",
        version = "v1.2.5",
    )
    go_repository(
        name = "com_google_cloud_go_errorreporting",
        importpath = "cloud.google.com/go/errorreporting",
        sum = "h1:E/gLk+rL7u5JZB9oq72iL1bnhVlLrnfslrgcptjJEUE=",
        version = "v0.3.1",
    )

    go_repository(
        name = "com_google_cloud_go_essentialcontacts",
        importpath = "cloud.google.com/go/essentialcontacts",
        sum = "h1:JaQXS+qCFYs8yectfZHpzw4+NjTvFqTuDMCtfPzMvbw=",
        version = "v1.6.12",
    )
    go_repository(
        name = "com_google_cloud_go_eventarc",
        importpath = "cloud.google.com/go/eventarc",
        sum = "h1:HVJmOVc+7eVFAqMpJRrq0nY0KlYBEBVZW7Gz7TxTio8=",
        version = "v1.13.10",
    )
    go_repository(
        name = "com_google_cloud_go_filestore",
        importpath = "cloud.google.com/go/filestore",
        sum = "h1:LF9t5MClPyFJMuXdez/AjF1uyO9xHKUFF3GUqA+xFPI=",
        version = "v1.8.7",
    )
    go_repository(
        name = "com_google_cloud_go_firestore",
        importpath = "cloud.google.com/go/firestore",
        sum = "h1:YwmDHcyrxVRErWcgxunzEaZxtNbc8QoFYA/JOEwDPgc=",
        version = "v1.16.0",
    )

    go_repository(
        name = "com_google_cloud_go_functions",
        importpath = "cloud.google.com/go/functions",
        sum = "h1:tPe3/48RpjcFk96VeB6jOKQpK8nliGJLsgjh6pUOyFQ=",
        version = "v1.16.6",
    )

    go_repository(
        name = "com_google_cloud_go_gkebackup",
        importpath = "cloud.google.com/go/gkebackup",
        sum = "h1:mufh0PNpvqbfLV+TcxzSGESX8jGBcjKgctldv7kwQns=",
        version = "v1.5.4",
    )
    go_repository(
        name = "com_google_cloud_go_gkeconnect",
        importpath = "cloud.google.com/go/gkeconnect",
        sum = "h1:4bZAzvqhuv1uP+i4yG9cEMQ6ggdP26nBVjUgroPU6IM=",
        version = "v0.8.11",
    )
    go_repository(
        name = "com_google_cloud_go_gkehub",
        importpath = "cloud.google.com/go/gkehub",
        sum = "h1:hQkVCcOiW/vPVYsthvKl1nje430/TpdFfgeIuqcYVOA=",
        version = "v0.14.11",
    )
    go_repository(
        name = "com_google_cloud_go_gkemulticloud",
        importpath = "cloud.google.com/go/gkemulticloud",
        sum = "h1:6zV05tyl37HoEjCGGY+zHFNxnKQCjvVpiqWAUVgGaEs=",
        version = "v1.2.4",
    )
    go_repository(
        name = "com_google_cloud_go_gsuiteaddons",
        importpath = "cloud.google.com/go/gsuiteaddons",
        sum = "h1:zydWX0nVT0Ut/P1X25Sy+4Rqe2PH04IzhwlF1BJd8To=",
        version = "v1.6.11",
    )
    go_repository(
        name = "com_google_cloud_go_iam",
        importpath = "cloud.google.com/go/iam",
        sum = "h1:JixGLimRrNGcxvJEQ8+clfLxPlbeZA6MuRJ+qJNQ5Xw=",
        version = "v1.1.12",
    )
    go_repository(
        name = "com_google_cloud_go_iap",
        importpath = "cloud.google.com/go/iap",
        sum = "h1:j7jQqqSkZ2nWAOCiyaZfnJ+REycTJ2NP2dUEjLoW4aA=",
        version = "v1.9.10",
    )
    go_repository(
        name = "com_google_cloud_go_ids",
        importpath = "cloud.google.com/go/ids",
        sum = "h1:JhlR1d0XhMsj6YmSmbLbbXV5CGkffnUkPj0HNxJYNtc=",
        version = "v1.4.11",
    )
    go_repository(
        name = "com_google_cloud_go_iot",
        importpath = "cloud.google.com/go/iot",
        sum = "h1:UBqSUZA6+7bM+mv6uvhl8tVsyT2Fi50njtBFRbrKSlI=",
        version = "v1.7.11",
    )
    go_repository(
        name = "com_google_cloud_go_kms",
        importpath = "cloud.google.com/go/kms",
        sum = "h1:dYN3OCsQ6wJLLtOnI8DGUwQ5shMusXsWCCC+s09ATsk=",
        version = "v1.18.4",
    )
    go_repository(
        name = "com_google_cloud_go_language",
        importpath = "cloud.google.com/go/language",
        sum = "h1:zzN4QifNRII72H+M0KemHclpuO4fEL7W67hgVJVwKoU=",
        version = "v1.12.9",
    )
    go_repository(
        name = "com_google_cloud_go_lifesciences",
        importpath = "cloud.google.com/go/lifesciences",
        sum = "h1:xyPSYICJWZElcELYgWCKs5PltyNX3TzOKaQAZA7d/I0=",
        version = "v0.9.11",
    )
    go_repository(
        name = "com_google_cloud_go_logging",
        importpath = "cloud.google.com/go/logging",
        sum = "h1:v3ktVzXMV7CwHq1MBF65wcqLMA7i+z3YxbUsoK7mOKs=",
        version = "v1.11.0",
    )

    go_repository(
        name = "com_google_cloud_go_longrunning",
        importpath = "cloud.google.com/go/longrunning",
        sum = "h1:5LqSIdERr71CqfUsFlJdBpOkBH8FBCFD7P1nTWy3TYE=",
        version = "v0.5.12",
    )
    go_repository(
        name = "com_google_cloud_go_managedidentities",
        importpath = "cloud.google.com/go/managedidentities",
        sum = "h1:YU6NtRRBX5R1f3a8ryqhh1dUb1/pt3rnhSO50b63yZY=",
        version = "v1.6.11",
    )
    go_repository(
        name = "com_google_cloud_go_maps",
        importpath = "cloud.google.com/go/maps",
        sum = "h1:XgSh470MFdCgUY+6Oj2LTxYqSyzLgKFb9b7hwSlg1nw=",
        version = "v1.11.5",
    )

    go_repository(
        name = "com_google_cloud_go_mediatranslation",
        importpath = "cloud.google.com/go/mediatranslation",
        sum = "h1:QvO405ocKTmcJqjfqL1zps08yrKk8rE+0E1ZNSWfjbw=",
        version = "v0.8.11",
    )
    go_repository(
        name = "com_google_cloud_go_memcache",
        importpath = "cloud.google.com/go/memcache",
        sum = "h1:DGPEJOVL4Qix2GLKQKcgzGpNLD7gAnCFLr9ch9YSIhU=",
        version = "v1.10.11",
    )
    go_repository(
        name = "com_google_cloud_go_metastore",
        importpath = "cloud.google.com/go/metastore",
        sum = "h1:E5eAxzIRoVP0DrV+ZtTLMYkkjSs4fcfsbL7wv1mXV2U=",
        version = "v1.13.10",
    )
    go_repository(
        name = "com_google_cloud_go_monitoring",
        importpath = "cloud.google.com/go/monitoring",
        sum = "h1:v/7MXFxYrhXLEZ9sSfwXdlTLLB/xrU7xTyYjY5acynQ=",
        version = "v1.20.3",
    )
    go_repository(
        name = "com_google_cloud_go_networkconnectivity",
        importpath = "cloud.google.com/go/networkconnectivity",
        sum = "h1:2EE8pKiv1AI8fBdZCdiUjNgQ+TaBgwE4GxIze4fDdY0=",
        version = "v1.14.10",
    )
    go_repository(
        name = "com_google_cloud_go_networkmanagement",
        importpath = "cloud.google.com/go/networkmanagement",
        sum = "h1:6TGn7ZZXyj5rloN0vv5Aw0awYbfbheNRg8BKroT7/2g=",
        version = "v1.13.6",
    )
    go_repository(
        name = "com_google_cloud_go_networksecurity",
        importpath = "cloud.google.com/go/networksecurity",
        sum = "h1:6wUzyHCwDEOkDbAJjT6jxsAi+vMfe3aj2JWwqSFVXOQ=",
        version = "v0.9.11",
    )
    go_repository(
        name = "com_google_cloud_go_notebooks",
        importpath = "cloud.google.com/go/notebooks",
        sum = "h1:c8I0EaLGqStRmvX29L7jb4mOrpigxn1mGyBt65OdS0s=",
        version = "v1.11.9",
    )
    go_repository(
        name = "com_google_cloud_go_optimization",
        importpath = "cloud.google.com/go/optimization",
        sum = "h1:++U21U9LWFdgnnVFaq4kDeOafft6gI/CHzsiJ173c6U=",
        version = "v1.6.9",
    )
    go_repository(
        name = "com_google_cloud_go_orchestration",
        importpath = "cloud.google.com/go/orchestration",
        sum = "h1:xfczjtNDabsXTnDySAwD/TMfDSkcxEgH1rxfS6BVQtM=",
        version = "v1.9.6",
    )
    go_repository(
        name = "com_google_cloud_go_orgpolicy",
        importpath = "cloud.google.com/go/orgpolicy",
        sum = "h1:StymaN9vS7949m15Nwgf5aKd9yaRtzWJ4VqHdbXcOEM=",
        version = "v1.12.7",
    )
    go_repository(
        name = "com_google_cloud_go_osconfig",
        importpath = "cloud.google.com/go/osconfig",
        sum = "h1:IbbTg7jtTEn4+iEJwgbCYck5NLMOc2eKrqVpQb7Xx6c=",
        version = "v1.13.2",
    )
    go_repository(
        name = "com_google_cloud_go_oslogin",
        importpath = "cloud.google.com/go/oslogin",
        sum = "h1:q9x7tjKtfBpXMpiJKwb5UyhMA3GrwmJHvx56uCEuS8M=",
        version = "v1.13.7",
    )
    go_repository(
        name = "com_google_cloud_go_phishingprotection",
        importpath = "cloud.google.com/go/phishingprotection",
        sum = "h1:3Kr7TINZ+8pbdWe3JnJf9c84ibz60NRTvwLdVtI3SK8=",
        version = "v0.8.11",
    )
    go_repository(
        name = "com_google_cloud_go_policytroubleshooter",
        importpath = "cloud.google.com/go/policytroubleshooter",
        sum = "h1:EHXkBYgHQtVH8P41G2xxmQbMwQh+o5ggno8l3/9CXaA=",
        version = "v1.10.9",
    )
    go_repository(
        name = "com_google_cloud_go_privatecatalog",
        importpath = "cloud.google.com/go/privatecatalog",
        sum = "h1:t8dJpQf22H6COeDvp7TDl7+KuwLT6yVmqAVRIUIUj6U=",
        version = "v0.9.11",
    )
    go_repository(
        name = "com_google_cloud_go_pubsub",
        importpath = "cloud.google.com/go/pubsub",
        sum = "h1:0LdP+zj5XaPAGtWr2V6r88VXJlmtaB/+fde1q3TU8M0=",
        version = "v1.40.0",
    )
    go_repository(
        name = "com_google_cloud_go_pubsublite",
        importpath = "cloud.google.com/go/pubsublite",
        sum = "h1:jLQozsEVr+c6tOU13vDugtnaBSUy/PD5zK6mhm+uF1Y=",
        version = "v1.8.2",
    )

    go_repository(
        name = "com_google_cloud_go_recaptchaenterprise_v2",
        importpath = "cloud.google.com/go/recaptchaenterprise/v2",
        sum = "h1:80Mx0i3uyv5dPNUYsNPFk9GJ+19AmTlnWnXFCTC9NkI=",
        version = "v2.14.2",
    )
    go_repository(
        name = "com_google_cloud_go_recommendationengine",
        importpath = "cloud.google.com/go/recommendationengine",
        sum = "h1:STJYdA/e/MAh2ZSdjss5YE/d0t0nt0WotBF9V0pgpPQ=",
        version = "v0.8.11",
    )
    go_repository(
        name = "com_google_cloud_go_recommender",
        importpath = "cloud.google.com/go/recommender",
        sum = "h1:asEAoj4a3inPCdH8nbPaZDJWhR/xwfKi4tuSmIlaS2I=",
        version = "v1.12.7",
    )
    go_repository(
        name = "com_google_cloud_go_redis",
        importpath = "cloud.google.com/go/redis",
        sum = "h1:9CO6EcuM9/CpgtcjG6JZV+GFw3oDrRfwLwmvwo/uM1o=",
        version = "v1.16.4",
    )
    go_repository(
        name = "com_google_cloud_go_resourcemanager",
        importpath = "cloud.google.com/go/resourcemanager",
        sum = "h1:N8CmqszjKNOgJnrQVsg+g8VWIEGgcwsD5rPiay9cMC4=",
        version = "v1.9.11",
    )
    go_repository(
        name = "com_google_cloud_go_resourcesettings",
        importpath = "cloud.google.com/go/resourcesettings",
        sum = "h1:1VwLfvJi8QtGrKPwuisGqr6gcgaCSR6A57wIvN+fqkM=",
        version = "v1.7.4",
    )
    go_repository(
        name = "com_google_cloud_go_retail",
        importpath = "cloud.google.com/go/retail",
        sum = "h1:YJgpBwCarAPqzaJS8ycIhyn2sAQT1RhTJRiTVBjtJAI=",
        version = "v1.17.4",
    )
    go_repository(
        name = "com_google_cloud_go_run",
        importpath = "cloud.google.com/go/run",
        sum = "h1:ai1rnbX92iPqWg9MrbDbebsxlUSAiOK6N9dEDDQeVA0=",
        version = "v1.4.0",
    )
    go_repository(
        name = "com_google_cloud_go_scheduler",
        importpath = "cloud.google.com/go/scheduler",
        sum = "h1:8BxDXoHCcsAe2fXsvFrkBbTxgl+5JBrIy1+/HRS0nxY=",
        version = "v1.10.12",
    )
    go_repository(
        name = "com_google_cloud_go_secretmanager",
        importpath = "cloud.google.com/go/secretmanager",
        sum = "h1:tXlHvpm97mFD0Lv50N4U4zlXfkoTNay3BmpNA/W7/oI=",
        version = "v1.13.5",
    )
    go_repository(
        name = "com_google_cloud_go_security",
        importpath = "cloud.google.com/go/security",
        sum = "h1:ERhxAa02mnMEIIAXvzje+qJ+yWniP6l5uOX+k9ELCaA=",
        version = "v1.17.4",
    )
    go_repository(
        name = "com_google_cloud_go_securitycenter",
        importpath = "cloud.google.com/go/securitycenter",
        sum = "h1:K+jfFUTum2jl//uWCN+QKkKXRgidxTyGfGTqXPyDvUY=",
        version = "v1.33.1",
    )

    go_repository(
        name = "com_google_cloud_go_servicedirectory",
        importpath = "cloud.google.com/go/servicedirectory",
        sum = "h1:8Ky2lY0CWJJIIlsc+rKTn6C3SqOuVEwT3brDC6TJCjk=",
        version = "v1.11.11",
    )

    go_repository(
        name = "com_google_cloud_go_shell",
        importpath = "cloud.google.com/go/shell",
        sum = "h1:RobTXyL33DQITYQh//KJ9GjS4bsdj4fGmm2rkb/ywzM=",
        version = "v1.7.11",
    )
    go_repository(
        name = "com_google_cloud_go_spanner",
        importpath = "cloud.google.com/go/spanner",
        sum = "h1:XK15cs9lFFQo5n4Wh9nfrcPXAxWln6NdodDiQKmoD08=",
        version = "v1.65.0",
    )

    go_repository(
        name = "com_google_cloud_go_speech",
        importpath = "cloud.google.com/go/speech",
        sum = "h1:3j+WpeBY57C0FDJxg317vpKgOLjL/kNxlcNPGSqXkqE=",
        version = "v1.24.0",
    )
    go_repository(
        name = "com_google_cloud_go_storagetransfer",
        importpath = "cloud.google.com/go/storagetransfer",
        sum = "h1:GfxaYqX+kwlrSrJAENNmRTCGmSTgvouvS3XhgwKpOT8=",
        version = "v1.10.10",
    )
    go_repository(
        name = "com_google_cloud_go_talent",
        importpath = "cloud.google.com/go/talent",
        sum = "h1:JN721EjG+UTfHVVaMhyxwKCCJPjUc8PiS0RnW/7kWfE=",
        version = "v1.6.12",
    )
    go_repository(
        name = "com_google_cloud_go_texttospeech",
        importpath = "cloud.google.com/go/texttospeech",
        sum = "h1:jzko1ahItjLYEWr6n3lTIoBSinD1JzavEuDzYLWZNko=",
        version = "v1.7.11",
    )
    go_repository(
        name = "com_google_cloud_go_tpu",
        importpath = "cloud.google.com/go/tpu",
        sum = "h1:uMrwnK05cocNt3OOp+mZ16xlvIKaXUt3QUXkUbG4LdM=",
        version = "v1.6.11",
    )
    go_repository(
        name = "com_google_cloud_go_trace",
        importpath = "cloud.google.com/go/trace",
        sum = "h1:+Y1emOgcyGy6OdJ2KQbT4t2oecPp49GtJn8j3GM1pWo=",
        version = "v1.10.11",
    )
    go_repository(
        name = "com_google_cloud_go_translate",
        importpath = "cloud.google.com/go/translate",
        sum = "h1:W16MpZ2Z3TWoHbNHmyHz9As276lGVTSwxRcquv454R0=",
        version = "v1.10.7",
    )
    go_repository(
        name = "com_google_cloud_go_video",
        importpath = "cloud.google.com/go/video",
        sum = "h1:+FTZi7NtT4FV2Y1j3zC3zYjaRrlGqKsZpbLweredEWM=",
        version = "v1.22.0",
    )
    go_repository(
        name = "com_google_cloud_go_videointelligence",
        importpath = "cloud.google.com/go/videointelligence",
        sum = "h1:zl8xijOEavernn/t6mZZ4fg0pIVc2yquHH73oj0Leo4=",
        version = "v1.11.11",
    )
    go_repository(
        name = "com_google_cloud_go_vision_v2",
        importpath = "cloud.google.com/go/vision/v2",
        sum = "h1:HyFEUXQa0SvlF0LASCn/x+juNCH4kIXQrUqi6SIcYvE=",
        version = "v2.8.6",
    )
    go_repository(
        name = "com_google_cloud_go_vmmigration",
        importpath = "cloud.google.com/go/vmmigration",
        sum = "h1:yqwkTPpvSw9dUfnl9/APAVrwO9UW1jJZtgbZpNQ+WdU=",
        version = "v1.7.11",
    )
    go_repository(
        name = "com_google_cloud_go_vmwareengine",
        importpath = "cloud.google.com/go/vmwareengine",
        sum = "h1:9Fjn/RoeOMo8UQt1TbXmmw7rJApC26BqnISAI1AERcc=",
        version = "v1.2.0",
    )

    go_repository(
        name = "com_google_cloud_go_vpcaccess",
        importpath = "cloud.google.com/go/vpcaccess",
        sum = "h1:1XgRP+Q2X6MvE/xnexpQ7ydgav+IO5UcKUIJEbL65J8=",
        version = "v1.7.11",
    )
    go_repository(
        name = "com_google_cloud_go_webrisk",
        importpath = "cloud.google.com/go/webrisk",
        sum = "h1:2qwEqnXrToIv2Y4xvsUSxCk7R2Ki+3W2+GNyrytoKTQ=",
        version = "v1.9.11",
    )
    go_repository(
        name = "com_google_cloud_go_websecurityscanner",
        importpath = "cloud.google.com/go/websecurityscanner",
        sum = "h1:r3ePI3YN7ujwX8c9gIkgbVjYVwP4yQA4X2z6P7+HNxI=",
        version = "v1.6.11",
    )
    go_repository(
        name = "com_google_cloud_go_workflows",
        importpath = "cloud.google.com/go/workflows",
        sum = "h1:EGJeZmwgE71jxFOI5s9iKST2Bivif3DSzlqVbiXACXQ=",
        version = "v1.12.10",
    )
    go_repository(
        name = "dev_cel_expr",
        importpath = "cel.dev/expr",
        sum = "h1:O1jzfJCQBfL5BFoYktaxwIhuttaQPsVWerH9/EEKx0w=",
        version = "v0.15.0",
    )

    go_repository(
        name = "io_opencensus_go",
        importpath = "go.opencensus.io",
        sum = "h1:y73uSU6J157QMP2kn2r30vwW1A2W2WFwSCGnAVxeaD0=",
        version = "v0.24.0",
    )
    go_repository(
        name = "io_opentelemetry_go_contrib_instrumentation_google_golang_org_grpc_otelgrpc",
        importpath = "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc",
        sum = "h1:4Pp6oUg3+e/6M4C0A/3kJ2VYa++dsWVTtGgLVj5xtHg=",
        version = "v0.49.0",
    )
    go_repository(
        name = "io_opentelemetry_go_contrib_instrumentation_net_http_otelhttp",
        importpath = "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp",
        sum = "h1:jq9TW8u3so/bN+JPT166wjOI6/vQPF6Xe7nMNIltagk=",
        version = "v0.49.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel",
        importpath = "go.opentelemetry.io/otel",
        sum = "h1:0LAOdjNmQeSTzGBzduGe/rU4tZhMwL5rWgtp9Ku5Jfo=",
        version = "v1.24.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_metric",
        importpath = "go.opentelemetry.io/otel/metric",
        sum = "h1:6EhoGWWK28x1fbpA4tYTOWBkPefTDQnb8WSGXlc88kI=",
        version = "v1.24.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_trace",
        importpath = "go.opentelemetry.io/otel/trace",
        sum = "h1:CsKnnL4dUAr/0llH9FKuc698G04IrpWV0MQA/Y1YELI=",
        version = "v1.24.0",
    )

    go_repository(
        name = "org_golang_google_genproto",
        importpath = "google.golang.org/genproto",
        sum = "h1:OqdXDEakZCVtDiZTjcxfwbHPCT11ycCEsTKesBVKvyY=",
        version = "v0.0.0-20240730163845-b1a4ccb954bf",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_api",
        importpath = "google.golang.org/genproto/googleapis/api",
        sum = "h1:+/tmTy5zAieooKIXfzDm9KiA3Bv6JBwriRN9LY+yayk=",
        version = "v0.0.0-20240812133136-8ffd90a71988",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_bytestream",
        importpath = "google.golang.org/genproto/googleapis/bytestream",
        sum = "h1:T4tsZBlZYXK3j40sQNP5MBO32I+rn6ypV1PpklsiV8k=",
        version = "v0.0.0-20240730163845-b1a4ccb954bf",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_rpc",
        importpath = "google.golang.org/genproto/googleapis/rpc",
        sum = "h1:V71AcdLZr2p8dC9dbOIMCpqi4EmRl8wUwnJzXXLmbmc=",
        version = "v0.0.0-20240812133136-8ffd90a71988",
    )

    go_repository(
        name = "org_golang_google_protobuf",
        importpath = "google.golang.org/protobuf",
        sum = "h1:6xV6lTsCfpGD21XK49h7MhtcApnLqkfYgPcdHftf6hg=",
        version = "v1.34.2",
    )
    go_repository(
        name = "org_golang_x_crypto",
        importpath = "golang.org/x/crypto",
        sum = "h1:RrRspgV4mU+YwB4FYnuBoKsUapNIL5cohGAmSH3azsw=",
        version = "v0.26.0",
    )

    go_repository(
        name = "org_golang_x_mod",
        importpath = "golang.org/x/mod",
        sum = "h1:zY54UmvipHiNd+pm+m0x9KhZ9hl1/7QNMyxXbc6ICqA=",
        version = "v0.17.0",
    )
    go_repository(
        name = "org_golang_x_net",
        importpath = "golang.org/x/net",
        sum = "h1:a9JDOJc5GMUJ0+UDqmLT86WiEy7iWyIhz8gz8E4e5hE=",
        version = "v0.28.0",
    )
    go_repository(
        name = "org_golang_x_sync",
        importpath = "golang.org/x/sync",
        sum = "h1:3NFvSEYkUoMifnESzZl15y791HH1qU2xm6eCJU5ZPXQ=",
        version = "v0.8.0",
    )
    go_repository(
        name = "org_golang_x_sys",
        importpath = "golang.org/x/sys",
        sum = "h1:Twjiwq9dn6R1fQcyiK+wQyHWfaz/BJB+YIpzU/Cv3Xg=",
        version = "v0.24.0",
    )
    go_repository(
        name = "org_golang_x_term",
        importpath = "golang.org/x/term",
        sum = "h1:F6D4vR+EHoL9/sWAWgAR1H2DcHr4PareCbAaCo1RpuU=",
        version = "v0.23.0",
    )
    go_repository(
        name = "org_golang_x_text",
        importpath = "golang.org/x/text",
        sum = "h1:XtiM5bkSOt+ewxlOE/aE/AKEHibwj/6gvWMl9Rsh0Qc=",
        version = "v0.17.0",
    )
    go_repository(
        name = "org_golang_x_time",
        importpath = "golang.org/x/time",
        sum = "h1:eTDhh4ZXt5Qf0augr54TN6suAUudPcawVZeIAPU7D4U=",
        version = "v0.6.0",
    )

    go_repository(
        name = "org_golang_x_tools",
        importpath = "golang.org/x/tools",
        sum = "h1:vU5i/LfpvrRCpgM/VPfJLg5KjxD3E+hfT1SH+d9zLwg=",
        version = "v0.21.1-0.20240508182429-e35e4ccd0d2d",
    )
    go_repository(
        name = "org_golang_x_xerrors",
        importpath = "golang.org/x/xerrors",
        sum = "h1:E7g+9GITq07hpfrRu66IVDexMakfv52eLZ2CXBWiKr4=",
        version = "v0.0.0-20191204190536-9bdfabe68543",
    )
