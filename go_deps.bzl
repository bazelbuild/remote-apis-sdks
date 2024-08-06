load("@bazel_gazelle//:deps.bzl", "go_repository")

def remote_apis_sdks_go_deps():
    go_repository(
        name = "co_honnef_go_tools",
        importpath = "honnef.co/go/tools",
        sum = "h1:qTakTkI6ni6LFD5sBwwsdSO+AQqbSIxOauHTTQKZ/7o=",
        version = "v0.1.3",
    )
    go_repository(
        name = "com_github_ajstarks_deck",
        importpath = "github.com/ajstarks/deck",
        sum = "h1:7kQgkwGRoLzC9K0oyXdJo7nve/bynv/KwUsxbiTlzAM=",
        version = "v0.0.0-20200831202436-30c9fc6549a9",
    )
    go_repository(
        name = "com_github_ajstarks_deck_generate",
        importpath = "github.com/ajstarks/deck/generate",
        sum = "h1:iXUgAaqDcIUGbRoy2TdeofRG/j1zpGRSEmNK05T+bi8=",
        version = "v0.0.0-20210309230005-c3f852c02e19",
    )
    go_repository(
        name = "com_github_ajstarks_svgo",
        importpath = "github.com/ajstarks/svgo",
        sum = "h1:slYM766cy2nI3BwyRiyQj/Ud48djTMtMebDqepE95rw=",
        version = "v0.0.0-20211024235047-1546f124cd8b",
    )
    go_repository(
        name = "com_github_alecthomas_assert_v2",
        importpath = "github.com/alecthomas/assert/v2",
        sum = "h1:mAsH2wmvjsuvyBvAmCtm7zFsBlb8mIHx5ySLVdDZXL0=",
        version = "v2.3.0",
    )
    go_repository(
        name = "com_github_alecthomas_participle_v2",
        importpath = "github.com/alecthomas/participle/v2",
        sum = "h1:z7dElHRrOEEq45F2TG5cbQihMtNTv8vwldytDj7Wrz4=",
        version = "v2.1.0",
    )
    go_repository(
        name = "com_github_alecthomas_repr",
        importpath = "github.com/alecthomas/repr",
        sum = "h1:HAzS41CIzNW5syS8Mf9UwXhNH1J9aix/BvDRf1Ml2Yk=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_andybalholm_brotli",
        importpath = "github.com/andybalholm/brotli",
        sum = "h1:8uQZIdzKmjc/iuPu7O2ioW48L81FgatrcpfFmiq/cCs=",
        version = "v1.0.5",
    )
    go_repository(
        name = "com_github_antihax_optional",
        importpath = "github.com/antihax/optional",
        sum = "h1:xK2lYat7ZLaVVcIuj82J8kIro4V6kDe0AUDFboUCwcg=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_apache_arrow_go_v10",
        importpath = "github.com/apache/arrow/go/v10",
        sum = "h1:n9dERvixoC/1JjDmBcs9FPaEryoANa2sCgVFo6ez9cI=",
        version = "v10.0.1",
    )
    go_repository(
        name = "com_github_apache_arrow_go_v11",
        importpath = "github.com/apache/arrow/go/v11",
        sum = "h1:hqauxvFQxww+0mEU/2XHG6LT7eZternCZq+A5Yly2uM=",
        version = "v11.0.0",
    )
    go_repository(
        name = "com_github_apache_arrow_go_v12",
        importpath = "github.com/apache/arrow/go/v12",
        sum = "h1:JsR2+hzYYjgSUkBSaahpqCetqZMr76djX80fF/DiJbg=",
        version = "v12.0.1",
    )
    go_repository(
        name = "com_github_apache_arrow_go_v14",
        importpath = "github.com/apache/arrow/go/v14",
        sum = "h1:N8OkaJEOfI3mEZt07BIkvo4sC6XDbL+48MBPWO5IONw=",
        version = "v14.0.2",
    )
    go_repository(
        name = "com_github_apache_thrift",
        importpath = "github.com/apache/thrift",
        sum = "h1:cMd2aj52n+8VoAtvSvLn4kDC3aZ6IAkBuqWQ2IDu7wo=",
        version = "v0.17.0",
    )
    go_repository(
        name = "com_github_boombuler_barcode",
        importpath = "github.com/boombuler/barcode",
        sum = "h1:NDBbPmhS+EqABEs5Kg3n/5ZNjy73Pz7SIV+KCeqyXcs=",
        version = "v1.0.1",
    )

    go_repository(
        name = "com_github_burntsushi_toml",
        importpath = "github.com/BurntSushi/toml",
        sum = "h1:WXkYYl6Yr3qBf1K79EBnL4mak0OimBfB0XUf9Vl28OQ=",
        version = "v0.3.1",
    )
    go_repository(
        name = "com_github_burntsushi_xgb",
        importpath = "github.com/BurntSushi/xgb",
        sum = "h1:1BDTz0u9nC3//pOCMdNH+CiXJVYJh5UQNCOBG7jbELc=",
        version = "v0.0.0-20160522181843-27f122750802",
    )

    go_repository(
        name = "com_github_census_instrumentation_opencensus_proto",
        importpath = "github.com/census-instrumentation/opencensus-proto",
        sum = "h1:iKLQ0xPNFxR/2hzXZMrBo8f1j86j5WHzznCCQxV/b8g=",
        version = "v0.4.1",
    )
    go_repository(
        name = "com_github_cespare_xxhash",
        importpath = "github.com/cespare/xxhash",
        sum = "h1:a6HrQnmkObjyL+Gs60czilIUGqrzKutQD6XZog3p+ko=",
        version = "v1.1.0",
    )

    go_repository(
        name = "com_github_cespare_xxhash_v2",
        importpath = "github.com/cespare/xxhash/v2",
        sum = "h1:DC2CZ1Ep5Y4k3ZQ899DldepgrayRUGE6BBZ/cd9Cj44=",
        version = "v2.2.0",
    )
    go_repository(
        name = "com_github_chzyer_logex",
        importpath = "github.com/chzyer/logex",
        sum = "h1:+eqR0HfOetur4tgnC8ftU5imRnhi4te+BadWS95c5AM=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_chzyer_readline",
        importpath = "github.com/chzyer/readline",
        sum = "h1:lSwwFrbNviGePhkewF1az4oLmcwqCZijQ2/Wi3BGHAI=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_github_chzyer_test",
        importpath = "github.com/chzyer/test",
        sum = "h1:dZ0/VyGgQdVGAss6Ju0dt5P0QltE0SFY5Woh6hbIfiQ=",
        version = "v0.0.0-20210722231415-061457976a23",
    )

    go_repository(
        name = "com_github_client9_misspell",
        importpath = "github.com/client9/misspell",
        sum = "h1:ta993UF76GwbvJcIo3Y68y/M3WxlpEHPWIGDkJYwzJI=",
        version = "v0.3.4",
    )
    go_repository(
        name = "com_github_cncf_udpa_go",
        importpath = "github.com/cncf/udpa/go",
        sum = "h1:QQ3GSy+MqSHxm/d8nCtnAiZdYFd45cYZPs8vOOIYKfk=",
        version = "v0.0.0-20220112060539-c52dc94e7fbe",
    )
    go_repository(
        name = "com_github_cncf_xds_go",
        importpath = "github.com/cncf/xds/go",
        sum = "h1:DBmgJDC9dTfkVyGgipamEh2BpGYxScCH1TOF1LL1cXc=",
        version = "v0.0.0-20240318125728-8a4994d93e50",
    )
    go_repository(
        name = "com_github_creack_pty",
        importpath = "github.com/creack/pty",
        sum = "h1:uDmaGzcdjhF4i/plgjmEsriH11Y0o7RKapEf/LDaM3w=",
        version = "v1.1.9",
    )

    go_repository(
        name = "com_github_davecgh_go_spew",
        importpath = "github.com/davecgh/go-spew",
        sum = "h1:vj9j/u1bqnvCEfJOwUhtlOARqs3+rkHYY13jYWTU97c=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_docopt_docopt_go",
        importpath = "github.com/docopt/docopt-go",
        sum = "h1:bWDMxwH3px2JBh6AyO7hdCn/PkvCZXii8TGj7sbtEbQ=",
        version = "v0.0.0-20180111231733-ee0de3bc6815",
    )
    go_repository(
        name = "com_github_dustin_go_humanize",
        importpath = "github.com/dustin/go-humanize",
        sum = "h1:GzkhY7T5VNhEkwH0PVJgjz+fX1rhBrR7pRT3mDkpeCY=",
        version = "v1.0.1",
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
        name = "com_github_fatih_color",
        importpath = "github.com/fatih/color",
        sum = "h1:kOqh6YHBtK8aywxGerMG2Eq3H6Qgoqeo13Bk2Mv/nBs=",
        version = "v1.15.0",
    )

    go_repository(
        name = "com_github_felixge_httpsnoop",
        importpath = "github.com/felixge/httpsnoop",
        sum = "h1:NFTV2Zj1bL4mc9sqWACXbQFVBBg2W3GPvqp8/ESS2Wg=",
        version = "v1.0.4",
    )
    go_repository(
        name = "com_github_fogleman_gg",
        importpath = "github.com/fogleman/gg",
        sum = "h1:/7zJX8F6AaYQc57WQCyN9cAIz+4bCJGO9B+dyW29am8=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_github_ghodss_yaml",
        importpath = "github.com/ghodss/yaml",
        sum = "h1:wQHKEahhL6wmXdzwWG11gIVCkOv05bNOh+Rxn0yngAk=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_go_fonts_dejavu",
        importpath = "github.com/go-fonts/dejavu",
        sum = "h1:JSajPXURYqpr+Cu8U9bt8K+XcACIHWqWrvWCKyeFmVQ=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_go_fonts_latin_modern",
        importpath = "github.com/go-fonts/latin-modern",
        sum = "h1:5/Tv1Ek/QCr20C6ZOz15vw3g7GELYL98KWr8Hgo+3vk=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_go_fonts_liberation",
        importpath = "github.com/go-fonts/liberation",
        sum = "h1:jAkAWJP4S+OsrPLZM4/eC9iW7CtHy+HBXrEwZXWo5VM=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_go_fonts_stix",
        importpath = "github.com/go-fonts/stix",
        sum = "h1:UlZlgrvvmT/58o573ot7NFw0vZasZ5I6bcIft/oMdgg=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_go_gl_glfw",
        importpath = "github.com/go-gl/glfw",
        sum = "h1:QbL/5oDUmRBzO9/Z7Seo6zf912W/a6Sr4Eu0G/3Jho0=",
        version = "v0.0.0-20190409004039-e6da0acd62b1",
    )
    go_repository(
        name = "com_github_go_gl_glfw_v3_3_glfw",
        importpath = "github.com/go-gl/glfw/v3.3/glfw",
        sum = "h1:WtGNWLvXpe6ZudgnXrq0barxBImvnnJoMEhXAzcbM0I=",
        version = "v0.0.0-20200222043503-6f7a984d4dc4",
    )
    go_repository(
        name = "com_github_go_latex_latex",
        importpath = "github.com/go-latex/latex",
        sum = "h1:6zl3BbBhdnMkpSj2YY30qV3gDcVBGtFgVsV3+/i+mKQ=",
        version = "v0.0.0-20210823091927-c0d11ff05a81",
    )

    go_repository(
        name = "com_github_go_logr_logr",
        importpath = "github.com/go-logr/logr",
        sum = "h1:pKouT5E8xu9zeFC39JXRDukb6JFQPXM5p5I91188VAQ=",
        version = "v1.4.1",
    )
    go_repository(
        name = "com_github_go_logr_stdr",
        importpath = "github.com/go-logr/stdr",
        sum = "h1:hSWxHoqTgW2S2qGc0LTAI563KZ5YKYRhT3MFKZMbjag=",
        version = "v1.2.2",
    )
    go_repository(
        name = "com_github_go_pdf_fpdf",
        importpath = "github.com/go-pdf/fpdf",
        sum = "h1:MlgtGIfsdMEEQJr2le6b/HNr1ZlQwxyWr77r2aj2U/8=",
        version = "v0.6.0",
    )
    go_repository(
        name = "com_github_go_playground_assert_v2",
        importpath = "github.com/go-playground/assert/v2",
        sum = "h1:MsBgLAaY856+nPRTKrp3/OZK38U/wa0CcBYNjji3q3A=",
        version = "v2.0.1",
    )
    go_repository(
        name = "com_github_go_playground_locales",
        importpath = "github.com/go-playground/locales",
        sum = "h1:HyWk6mgj5qFqCT5fjGBuRArbVDfE4hi8+e8ceBS/t7Q=",
        version = "v0.13.0",
    )
    go_repository(
        name = "com_github_go_playground_universal_translator",
        importpath = "github.com/go-playground/universal-translator",
        sum = "h1:icxd5fm+REJzpZx7ZfpaD876Lmtgy7VtROAbHHXk8no=",
        version = "v0.17.0",
    )
    go_repository(
        name = "com_github_go_playground_validator_v10",
        importpath = "github.com/go-playground/validator/v10",
        sum = "h1:pH2c5ADXtd66mxoE0Zm9SUhxE20r7aM3F26W0hOn+GE=",
        version = "v10.4.1",
    )
    go_repository(
        name = "com_github_goccy_go_json",
        importpath = "github.com/goccy/go-json",
        sum = "h1:CrxCmQqYDkv1z7lO7Wbh2HN93uovUHgrECaO5ZrCXAU=",
        version = "v0.10.2",
    )
    go_repository(
        name = "com_github_goccy_go_yaml",
        importpath = "github.com/goccy/go-yaml",
        sum = "h1:n7Z+zx8S9f9KgzG6KtQKf+kwqXZlLNR2F6018Dgau54=",
        version = "v1.11.0",
    )
    go_repository(
        name = "com_github_golang_freetype",
        importpath = "github.com/golang/freetype",
        sum = "h1:DACJavvAHhabrF08vX0COfcOBJRhZ8lUbR+ZWIs0Y5g=",
        version = "v0.0.0-20170609003504-e2365dfdc4a0",
    )

    go_repository(
        name = "com_github_golang_glog",
        importpath = "github.com/golang/glog",
        sum = "h1:uCdmnmatrKCgMBlM4rMuJZWOkPDqdbZPnrMXDY4gI68=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_golang_groupcache",
        importpath = "github.com/golang/groupcache",
        sum = "h1:oI5xCqsCo564l8iNU+DwB5epxmsaqB+rhGL0m5jtYqE=",
        version = "v0.0.0-20210331224755-41bb18bfe9da",
    )
    go_repository(
        name = "com_github_golang_mock",
        importpath = "github.com/golang/mock",
        sum = "h1:ErTB+efbowRARo13NNdxyJji2egdxLGQhRaY+DUumQc=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_github_golang_protobuf",
        importpath = "github.com/golang/protobuf",
        sum = "h1:i7eJL8qZTpSEXOPTxNKhASYpMn+8e5Q6AdndVa1dWek=",
        version = "v1.5.4",
    )
    go_repository(
        name = "com_github_golang_snappy",
        importpath = "github.com/golang/snappy",
        sum = "h1:yAGX7huGHXlcLOEtBnF4w7FQwA26wojNCwOYAEhLjQM=",
        version = "v0.0.4",
    )
    go_repository(
        name = "com_github_google_btree",
        importpath = "github.com/google/btree",
        sum = "h1:0udJVsspx3VBr5FwtLhQQtuAsVc79tTq0ocGIPAU6qo=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_google_flatbuffers",
        importpath = "github.com/google/flatbuffers",
        sum = "h1:M9dgRyhJemaM4Sw8+66GHBu8ioaQmyPLg1b8VwK5WJg=",
        version = "v23.5.26+incompatible",
    )

    go_repository(
        name = "com_github_google_go_cmp",
        importpath = "github.com/google/go-cmp",
        sum = "h1:ofyhxvXcZhMsU5ulbFiLKl/XBFqE1GSq7atu8tAmTRI=",
        version = "v0.6.0",
    )
    go_repository(
        name = "com_github_google_go_pkcs11",
        importpath = "github.com/google/go-pkcs11",
        sum = "h1:OF1IPgv+F4NmqmJ98KTjdN97Vs1JxDPB3vbmYzV2dpk=",
        version = "v0.2.1-0.20230907215043-c6f79328ddf9",
    )
    go_repository(
        name = "com_github_google_gofuzz",
        importpath = "github.com/google/gofuzz",
        sum = "h1:A8PeW59pxE9IoFRqBp37U+mSNaQoZ46F1f0f863XSXw=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_google_martian",
        importpath = "github.com/google/martian",
        sum = "h1:/CP5g8u/VJHijgedC/Legn3BAbAaWPgecwXBIDzw5no=",
        version = "v2.1.0+incompatible",
    )
    go_repository(
        name = "com_github_google_martian_v3",
        importpath = "github.com/google/martian/v3",
        sum = "h1:IqNFLAmvJOgVlpdEBiQbDc2EwKW77amAycfTuWKdfvw=",
        version = "v3.3.2",
    )
    go_repository(
        name = "com_github_google_pprof",
        importpath = "github.com/google/pprof",
        sum = "h1:Xim43kblpZXfIBQsbuBVKCudVG457BR2GZFIz3uw3hQ=",
        version = "v0.0.0-20221118152302-e6195bd50e26",
    )
    go_repository(
        name = "com_github_google_renameio",
        importpath = "github.com/google/renameio",
        sum = "h1:GOZbcHa3HfsPKPlmyPyN2KEohoMXOhdMbHrvbpl2QaA=",
        version = "v0.1.0",
    )

    go_repository(
        name = "com_github_google_s2a_go",
        importpath = "github.com/google/s2a-go",
        sum = "h1:60BLSyTrOV4/haCDW4zb1guZItoSq8foHCXrAnjBo/o=",
        version = "v0.1.7",
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
        sum = "h1:9gWcmF85Wvq4ryPFvGFaOgPIs1AQX0d0bcbGw4Z96qg=",
        version = "v2.12.4",
    )
    go_repository(
        name = "com_github_googleapis_go_type_adapters",
        importpath = "github.com/googleapis/go-type-adapters",
        sum = "h1:9XdMn+d/G57qq1s8dNc5IesGCXHf6V2HZ2JwRxfA2tA=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_googleapis_google_cloud_go_testing",
        importpath = "github.com/googleapis/google-cloud-go-testing",
        sum = "h1:tlyzajkF3030q6M8SvmJSemC9DTHL/xaMa18b65+JM4=",
        version = "v0.0.0-20200911160855-bcd43fbb19e8",
    )
    go_repository(
        name = "com_github_grpc_ecosystem_grpc_gateway",
        importpath = "github.com/grpc-ecosystem/grpc-gateway",
        sum = "h1:gmcG1KaJ57LophUzW0Hy8NmPhnMZb4M0+kPpLofRdBo=",
        version = "v1.16.0",
    )
    go_repository(
        name = "com_github_grpc_ecosystem_grpc_gateway_v2",
        importpath = "github.com/grpc-ecosystem/grpc-gateway/v2",
        sum = "h1:YBftPWNWd4WwGqtY2yeZL2ef8rHAxPBD8KFhJpmcqms=",
        version = "v2.16.0",
    )
    go_repository(
        name = "com_github_hashicorp_golang_lru",
        importpath = "github.com/hashicorp/golang-lru",
        sum = "h1:0hERBMJE1eitiLkihrMvRVBYAkpHzc/J3QdDN+dAcgU=",
        version = "v0.5.1",
    )

    go_repository(
        name = "com_github_hectane_go_acl",
        importpath = "github.com/hectane/go-acl",
        sum = "h1:PGufWXXDq9yaev6xX1YQauaO1MV90e6Mpoq1I7Lz/VM=",
        version = "v0.0.0-20230122075934-ca0b05cb1adb",
    )
    go_repository(
        name = "com_github_hexops_gotextdiff",
        importpath = "github.com/hexops/gotextdiff",
        sum = "h1:gitA9+qJrrTCsiCl7+kh75nPqQt1cx4ZkudSTLoUqJM=",
        version = "v1.0.3",
    )
    go_repository(
        name = "com_github_iancoleman_strcase",
        importpath = "github.com/iancoleman/strcase",
        sum = "h1:nTXanmYxhfFAMjZL34Ov6gkzEsSJZ5DbhxWjvSASxEI=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_github_ianlancetaylor_demangle",
        importpath = "github.com/ianlancetaylor/demangle",
        sum = "h1:rcanfLhLDA8nozr/K289V1zcntHr3V+SHlXwzz1ZI2g=",
        version = "v0.0.0-20220319035150-800ac71e25c2",
    )
    go_repository(
        name = "com_github_johncgriffin_overflow",
        importpath = "github.com/JohnCGriffin/overflow",
        sum = "h1:RGWPOewvKIROun94nF7v2cua9qP+thov/7M50KEoeSU=",
        version = "v0.0.0-20211019200055-46fa312c352c",
    )
    go_repository(
        name = "com_github_json_iterator_go",
        importpath = "github.com/json-iterator/go",
        sum = "h1:PV8peI4a0ysnczrg+LtxykD8LfKY9ML6u2jnxaEnrnM=",
        version = "v1.1.12",
    )
    go_repository(
        name = "com_github_jstemmer_go_junit_report",
        importpath = "github.com/jstemmer/go-junit-report",
        sum = "h1:6QPYqodiu3GuPL+7mfx+NwDdp2eTkp9IfEUpgAwUN0o=",
        version = "v0.9.1",
    )
    go_repository(
        name = "com_github_jung_kurt_gofpdf",
        importpath = "github.com/jung-kurt/gofpdf",
        sum = "h1:PJr+ZMXIecYc1Ey2zucXdR73SMBtgjPgwa31099IMv0=",
        version = "v1.0.3-0.20190309125859-24315acbbda5",
    )
    go_repository(
        name = "com_github_kballard_go_shellquote",
        importpath = "github.com/kballard/go-shellquote",
        sum = "h1:Z9n2FFNUXsshfwJMBgNA0RU6/i7WVaAegv3PtuIHPMs=",
        version = "v0.0.0-20180428030007-95032a82bc51",
    )
    go_repository(
        name = "com_github_kisielk_gotool",
        importpath = "github.com/kisielk/gotool",
        sum = "h1:AV2c/EiW3KqPNT9ZKl07ehoAGi4C5/01Cfbblndcapg=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_klauspost_asmfmt",
        importpath = "github.com/klauspost/asmfmt",
        sum = "h1:4Ri7ox3EwapiOjCki+hw14RyKk201CN4rzyCJRFLpK4=",
        version = "v1.3.2",
    )

    go_repository(
        name = "com_github_klauspost_compress",
        importpath = "github.com/klauspost/compress",
        sum = "h1:YcnTYrq7MikUT7k0Yb5eceMmALQPYBW/Xltxn0NAMnU=",
        version = "v1.17.8",
    )
    go_repository(
        name = "com_github_klauspost_cpuid_v2",
        importpath = "github.com/klauspost/cpuid/v2",
        sum = "h1:0E5MSMDEoAulmXNFquVs//DdoomxaoTY1kUhbc/qbZg=",
        version = "v2.2.5",
    )
    go_repository(
        name = "com_github_kr_fs",
        importpath = "github.com/kr/fs",
        sum = "h1:Jskdu9ieNAYnjxsi0LbQp1ulIKZV1LAFgK1tWhpZgl8=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_kr_pretty",
        importpath = "github.com/kr/pretty",
        sum = "h1:flRD4NNwYAUpkphVc1HcthR4KEIFJ65n8Mw5qdRn3LE=",
        version = "v0.3.1",
    )
    go_repository(
        name = "com_github_kr_pty",
        importpath = "github.com/kr/pty",
        sum = "h1:VkoXIwSboBpnk99O/KFauAEILuNHv5DVFKZMBN/gUgw=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_kr_text",
        importpath = "github.com/kr/text",
        sum = "h1:5Nx0Ya0ZqY2ygV366QzturHI13Jq95ApcVaJBhpS+AY=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_leodido_go_urn",
        importpath = "github.com/leodido/go-urn",
        sum = "h1:hpXL4XnriNwQ/ABnpepYM/1vCLWNDfUNts8dX3xTG6Y=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_lyft_protoc_gen_star",
        importpath = "github.com/lyft/protoc-gen-star",
        sum = "h1:erE0rdztuaDq3bpGifD95wfoPrSZc95nGA6tbiNYh6M=",
        version = "v0.6.1",
    )
    go_repository(
        name = "com_github_lyft_protoc_gen_star_v2",
        importpath = "github.com/lyft/protoc-gen-star/v2",
        sum = "h1:/3+/2sWyXeMLzKd1bX+ixWKgEMsULrIivpDsuaF441o=",
        version = "v2.0.3",
    )
    go_repository(
        name = "com_github_mattn_go_colorable",
        importpath = "github.com/mattn/go-colorable",
        sum = "h1:fFA4WZxdEF4tXPZVKMLwD8oUnCTTo08duU7wxecdEvA=",
        version = "v0.1.13",
    )
    go_repository(
        name = "com_github_mattn_go_isatty",
        importpath = "github.com/mattn/go-isatty",
        sum = "h1:JITubQf0MOLdlGRuRq+jtsDlekdYPia9ZFsB8h/APPA=",
        version = "v0.0.19",
    )
    go_repository(
        name = "com_github_mattn_go_sqlite3",
        importpath = "github.com/mattn/go-sqlite3",
        sum = "h1:yOQRA0RpS5PFz/oikGwBEqvAWhWg5ufRz4ETLjwpU1Y=",
        version = "v1.14.16",
    )
    go_repository(
        name = "com_github_minio_asm2plan9s",
        importpath = "github.com/minio/asm2plan9s",
        sum = "h1:AMFGa4R4MiIpspGNG7Z948v4n35fFGB3RR3G/ry4FWs=",
        version = "v0.0.0-20200509001527-cdd76441f9d8",
    )
    go_repository(
        name = "com_github_minio_c2goasm",
        importpath = "github.com/minio/c2goasm",
        sum = "h1:+n/aFZefKZp7spd8DFdX7uMikMLXX4oubIzJF4kv/wI=",
        version = "v0.0.0-20190812172519-36a3d3bbc4f3",
    )
    go_repository(
        name = "com_github_modern_go_concurrent",
        importpath = "github.com/modern-go/concurrent",
        sum = "h1:TRLaZ9cD/w8PVh93nsPXa1VrQ6jlwL5oN8l14QlcNfg=",
        version = "v0.0.0-20180306012644-bacd9c7ef1dd",
    )
    go_repository(
        name = "com_github_modern_go_reflect2",
        importpath = "github.com/modern-go/reflect2",
        sum = "h1:xBagoLtFs94CBntxluKeaWgTMpvLxC4ur3nMaC9Gz0M=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_oneofone_xxhash",
        importpath = "github.com/OneOfOne/xxhash",
        sum = "h1:KMrpdQIwFcEqXDklaen+P1axHaj9BSKzvpUUfnHldSE=",
        version = "v1.2.2",
    )
    go_repository(
        name = "com_github_phpdave11_gofpdf",
        importpath = "github.com/phpdave11/gofpdf",
        sum = "h1:KPKiIbfwbvC/wOncwhrpRdXVj2CZTCFlw4wnoyjtHfQ=",
        version = "v1.4.2",
    )
    go_repository(
        name = "com_github_phpdave11_gofpdi",
        importpath = "github.com/phpdave11/gofpdi",
        sum = "h1:o61duiW8M9sMlkVXWlvP92sZJtGKENvW3VExs6dZukQ=",
        version = "v1.0.13",
    )
    go_repository(
        name = "com_github_pierrec_lz4_v4",
        importpath = "github.com/pierrec/lz4/v4",
        sum = "h1:xaKrnTkyoqfh1YItXl56+6KJNVYWlEEPuAQW9xsplYQ=",
        version = "v4.1.18",
    )
    go_repository(
        name = "com_github_pkg_diff",
        importpath = "github.com/pkg/diff",
        sum = "h1:aoZm08cpOy4WuID//EZDgcC4zIxODThtZNPirFr42+A=",
        version = "v0.0.0-20210226163009-20ebb0f2a09e",
    )
    go_repository(
        name = "com_github_pkg_errors",
        importpath = "github.com/pkg/errors",
        sum = "h1:FEBLx1zS214owpjy7qsBeixbURkuhQAwrK5UwLGTwt4=",
        version = "v0.9.1",
    )
    go_repository(
        name = "com_github_pkg_sftp",
        importpath = "github.com/pkg/sftp",
        sum = "h1:I2qBYMChEhIjOgazfJmV3/mZM256btk6wkCDRmW7JYs=",
        version = "v1.13.1",
    )

    go_repository(
        name = "com_github_pkg_xattr",
        importpath = "github.com/pkg/xattr",
        sum = "h1:5883YPCtkSd8LFbs13nXplj9g9tlrwoJRjgpgMu1/fE=",
        version = "v0.4.9",
    )
    go_repository(
        name = "com_github_pmezard_go_difflib",
        importpath = "github.com/pmezard/go-difflib",
        sum = "h1:4DBwDE0NGyQoBHbLQYPwSUPoCMWR5BEzIk/f1lZbAQM=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_prometheus_client_model",
        importpath = "github.com/prometheus/client_model",
        sum = "h1:VQw1hfvPvk3Uv6Qf29VrPF32JB6rtbgI6cYPYQjL0Qw=",
        version = "v0.5.0",
    )
    go_repository(
        name = "com_github_remyoudompheng_bigfft",
        importpath = "github.com/remyoudompheng/bigfft",
        sum = "h1:W09IVJc94icq4NjY3clb7Lk8O1qJ8BdBEF8z0ibU0rE=",
        version = "v0.0.0-20230129092748-24d4a6f8daec",
    )
    go_repository(
        name = "com_github_rogpeppe_fastuuid",
        importpath = "github.com/rogpeppe/fastuuid",
        sum = "h1:Ppwyp6VYCF1nvBTXL3trRso7mXMlRrw9ooo375wvi2s=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_rogpeppe_go_internal",
        importpath = "github.com/rogpeppe/go-internal",
        sum = "h1:exVL4IDcn6na9z1rAb56Vxr+CgyK3nn3O+epU5NdKM8=",
        version = "v1.12.0",
    )
    go_repository(
        name = "com_github_ruudk_golang_pdf417",
        importpath = "github.com/ruudk/golang-pdf417",
        sum = "h1:K1Xf3bKttbF+koVGaX5xngRIZ5bVjbmPnaxE/dR08uY=",
        version = "v0.0.0-20201230142125-a7e3863a1245",
    )
    go_repository(
        name = "com_github_spaolacci_murmur3",
        importpath = "github.com/spaolacci/murmur3",
        sum = "h1:qLC7fQah7D6K1B0ujays3HV9gkFtllcxhzImRR7ArPQ=",
        version = "v0.0.0-20180118202830-f09979ecbc72",
    )
    go_repository(
        name = "com_github_spf13_afero",
        importpath = "github.com/spf13/afero",
        sum = "h1:EaGW2JJh15aKOejeuJ+wpFSHnbd7GE6Wvp3TsNhb6LY=",
        version = "v1.10.0",
    )
    go_repository(
        name = "com_github_stoewer_go_strcase",
        importpath = "github.com/stoewer/go-strcase",
        sum = "h1:g0eASXYtp+yvN9fK8sH94oCIk0fau9uV1/ZdJ0AVEzs=",
        version = "v1.3.0",
    )

    go_repository(
        name = "com_github_stretchr_objx",
        importpath = "github.com/stretchr/objx",
        sum = "h1:xuMeJ0Sdp5ZMRXx/aWO6RZxdr3beISkG5/G/aIRr3pY=",
        version = "v0.5.2",
    )
    go_repository(
        name = "com_github_stretchr_testify",
        importpath = "github.com/stretchr/testify",
        sum = "h1:HtqpIVDClZ4nwg75+f6Lvsy/wHu+3BoSGCbBAcpTsTg=",
        version = "v1.9.0",
    )
    go_repository(
        name = "com_github_substrait_io_substrait_go",
        importpath = "github.com/substrait-io/substrait-go",
        sum = "h1:buDnjsb3qAqTaNbOR7VKmNgXf4lYQxWEcnSGUWBtmN8=",
        version = "v0.4.2",
    )

    go_repository(
        name = "com_github_yuin_goldmark",
        importpath = "github.com/yuin/goldmark",
        sum = "h1:fVcFKWvrslecOb/tg+Cc05dkeYx540o0FuFt3nUVDoE=",
        version = "v1.4.13",
    )
    go_repository(
        name = "com_github_zeebo_assert",
        importpath = "github.com/zeebo/assert",
        sum = "h1:g7C04CbJuIDKNPFHmsk4hwZDO5O+kntRxzaUoNXj+IQ=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_github_zeebo_xxh3",
        importpath = "github.com/zeebo/xxh3",
        sum = "h1:xZmwmqxHZA8AI603jOQ0tMqmBr9lPeFwGg6d+xy9DC0=",
        version = "v1.0.2",
    )

    go_repository(
        name = "com_google_cloud_go",
        importpath = "cloud.google.com/go",
        sum = "h1:OIPFAdfrFDFO2ve2U7r/H5SwSbBzEdrBdE7xkgwc+kY=",
        version = "v0.114.0",
    )
    go_repository(
        name = "com_google_cloud_go_accessapproval",
        importpath = "cloud.google.com/go/accessapproval",
        sum = "h1:fMbP4cJX/926h+kwGtABmcG83PXsjkB+q7nSBzZpJoo=",
        version = "v1.7.6",
    )
    go_repository(
        name = "com_google_cloud_go_accesscontextmanager",
        importpath = "cloud.google.com/go/accesscontextmanager",
        sum = "h1:NipmPd3BCzwa/mr40SK8pWRkbzv9Th5Azhi4dBYazlM=",
        version = "v1.8.6",
    )
    go_repository(
        name = "com_google_cloud_go_aiplatform",
        importpath = "cloud.google.com/go/aiplatform",
        sum = "h1:bbFYY4JInclG10czRFUYj2rjD+obhh3Gi9zVlyoMgEc=",
        version = "v1.66.0",
    )
    go_repository(
        name = "com_google_cloud_go_analytics",
        importpath = "cloud.google.com/go/analytics",
        sum = "h1:UH/PWBcPxHKolWxaS3hO+aj+wDTuq7XKvdtpqR3lyOI=",
        version = "v0.23.1",
    )
    go_repository(
        name = "com_google_cloud_go_apigateway",
        importpath = "cloud.google.com/go/apigateway",
        sum = "h1:60GMRN1JFwq9MldvEVMdR3gDJ0vI0C/BwgkImG6bx/M=",
        version = "v1.6.6",
    )
    go_repository(
        name = "com_google_cloud_go_apigeeconnect",
        importpath = "cloud.google.com/go/apigeeconnect",
        sum = "h1:ObsKNGtdu0ckkCSUpCN5fVAVg+DmzFg89JlqsW4OAWk=",
        version = "v1.6.6",
    )
    go_repository(
        name = "com_google_cloud_go_apigeeregistry",
        importpath = "cloud.google.com/go/apigeeregistry",
        sum = "h1:l8VFHdNMC+9Q4EHKye2eOZBu5IwddXF6ufAXI7D+PB8=",
        version = "v0.8.4",
    )
    go_repository(
        name = "com_google_cloud_go_apikeys",
        importpath = "cloud.google.com/go/apikeys",
        sum = "h1:B9CdHFZTFjVti89tmyXXrO+7vSNo2jvZuHG8zD5trdQ=",
        version = "v0.6.0",
    )

    go_repository(
        name = "com_google_cloud_go_appengine",
        importpath = "cloud.google.com/go/appengine",
        sum = "h1:cM+Lj0A0nCtujM9lqRId5L8hz7bHRfu3q3KdKtpn+38=",
        version = "v1.8.6",
    )
    go_repository(
        name = "com_google_cloud_go_area120",
        importpath = "cloud.google.com/go/area120",
        sum = "h1:7QJ4ZzqLOwh0pHD3UAa+VwiyWmDI7bdmkYKVkte8/wg=",
        version = "v0.8.6",
    )
    go_repository(
        name = "com_google_cloud_go_artifactregistry",
        importpath = "cloud.google.com/go/artifactregistry",
        sum = "h1:icIyRzJ1Ag6EOafuDuFFJ/AdStcOFRVfSGURn27/7Pk=",
        version = "v1.14.8",
    )
    go_repository(
        name = "com_google_cloud_go_asset",
        importpath = "cloud.google.com/go/asset",
        sum = "h1:+NpxL5L53VY91EoJTHeGGXSWEUllf2hhXpCyTnSrd3Q=",
        version = "v1.18.1",
    )
    go_repository(
        name = "com_google_cloud_go_assuredworkloads",
        importpath = "cloud.google.com/go/assuredworkloads",
        sum = "h1:3NlUes0xLN2kcSU24qQADFYsOaetCPg0HSA302AyV5s=",
        version = "v1.11.6",
    )
    go_repository(
        name = "com_google_cloud_go_auth",
        importpath = "cloud.google.com/go/auth",
        sum = "h1:0QNO7VThG54LUzKiQxv8C6x1YX7lUrzlAa1nVLF8CIw=",
        version = "v0.5.1",
    )
    go_repository(
        name = "com_google_cloud_go_auth_oauth2adapt",
        importpath = "cloud.google.com/go/auth/oauth2adapt",
        sum = "h1:+TTV8aXpjeChS9M+aTtN/TjdQnzJvmzKFt//oWu7HX4=",
        version = "v0.2.2",
    )

    go_repository(
        name = "com_google_cloud_go_automl",
        importpath = "cloud.google.com/go/automl",
        sum = "h1:NHBO5cjo2IgwaJ5qlez/iA35XI1db87PPlOB0Kjt5RM=",
        version = "v1.13.6",
    )
    go_repository(
        name = "com_google_cloud_go_baremetalsolution",
        importpath = "cloud.google.com/go/baremetalsolution",
        sum = "h1:jCR4rnVsG6ocK6ngFr2Z6ugKZfTENmMZkiV6Ma2tEeE=",
        version = "v1.2.5",
    )
    go_repository(
        name = "com_google_cloud_go_batch",
        importpath = "cloud.google.com/go/batch",
        sum = "h1:b9fVZDxxp4LWMhXV7uAhyMGmPuzlzPrsZ0uh+RM92h8=",
        version = "v1.8.3",
    )
    go_repository(
        name = "com_google_cloud_go_beyondcorp",
        importpath = "cloud.google.com/go/beyondcorp",
        sum = "h1:fnil8viEdcAJJiwiJPCT2fl3Grx3XFwXxTq7n9g/8QM=",
        version = "v1.0.5",
    )
    go_repository(
        name = "com_google_cloud_go_bigquery",
        importpath = "cloud.google.com/go/bigquery",
        sum = "h1:kA96WfgvCbkqfLnr7xI5uEfJ4h4FrnkdEb0yty0KSZo=",
        version = "v1.60.0",
    )
    go_repository(
        name = "com_google_cloud_go_billing",
        importpath = "cloud.google.com/go/billing",
        sum = "h1:XcYB8aKj97d4/0kh+LQgrxPxOo/0jgPNp5Q1nyzCyvs=",
        version = "v1.18.4",
    )
    go_repository(
        name = "com_google_cloud_go_binaryauthorization",
        importpath = "cloud.google.com/go/binaryauthorization",
        sum = "h1:XiAdW5HAWtk9WEjEA5MXYiRJ28Tjp1xGPPAMRFI5bnc=",
        version = "v1.8.2",
    )
    go_repository(
        name = "com_google_cloud_go_certificatemanager",
        importpath = "cloud.google.com/go/certificatemanager",
        sum = "h1:oc15T+leZ2Wm5oocvGTbDXwonka0chpJTEiHIVsBiEA=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_channel",
        importpath = "cloud.google.com/go/channel",
        sum = "h1:rBnTls9G5nC/jOrb9V/tZFHFXt6kBMNlIQKg6DjAlRM=",
        version = "v1.17.6",
    )
    go_repository(
        name = "com_google_cloud_go_cloudbuild",
        importpath = "cloud.google.com/go/cloudbuild",
        sum = "h1:66hY1gXV2cdn4gquy5TieaJwaZmRzrQ5cK++pVgnCkQ=",
        version = "v1.16.0",
    )
    go_repository(
        name = "com_google_cloud_go_clouddms",
        importpath = "cloud.google.com/go/clouddms",
        sum = "h1:t1nc49kRzEU2vrDxQQIEc5rZ4Hr187YrdOZPMAgMwMI=",
        version = "v1.7.5",
    )
    go_repository(
        name = "com_google_cloud_go_cloudtasks",
        importpath = "cloud.google.com/go/cloudtasks",
        sum = "h1:Ev+poxwb7pudBhiF0ObwAWT7Dh9BZAcsvAfFTWg0MPc=",
        version = "v1.12.7",
    )
    go_repository(
        name = "com_google_cloud_go_compute",
        importpath = "cloud.google.com/go/compute",
        sum = "h1:ZRpHJedLtTpKgr3RV1Fx23NuaAEN1Zfx9hw1u4aJdjU=",
        version = "v1.25.1",
    )
    go_repository(
        name = "com_google_cloud_go_compute_metadata",
        importpath = "cloud.google.com/go/compute/metadata",
        sum = "h1:Tz+eQXMEqDIKRsmY3cHTL6FVaynIjX2QxYC4trgAKZc=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_google_cloud_go_contactcenterinsights",
        importpath = "cloud.google.com/go/contactcenterinsights",
        sum = "h1:sCDKUmDj9Tfd6Qj7x4XbwC43oYzEBwSDLC1tReQWS/Y=",
        version = "v1.13.1",
    )
    go_repository(
        name = "com_google_cloud_go_container",
        importpath = "cloud.google.com/go/container",
        sum = "h1:y5gmgrMMhTrLnQQdMCw0t/Yis9Ps7jvAG4JYcRWxR8g=",
        version = "v1.35.0",
    )
    go_repository(
        name = "com_google_cloud_go_containeranalysis",
        importpath = "cloud.google.com/go/containeranalysis",
        sum = "h1:yzohQ0HDoZq2TtCJkbUBsJs9RIR5WbKZlHrD7ilp2yg=",
        version = "v0.11.5",
    )
    go_repository(
        name = "com_google_cloud_go_datacatalog",
        importpath = "cloud.google.com/go/datacatalog",
        sum = "h1:BGDsEjqpAo0Ka+b9yDLXnE5k+jU3lXGMh//NsEeDMIg=",
        version = "v1.20.0",
    )
    go_repository(
        name = "com_google_cloud_go_dataflow",
        importpath = "cloud.google.com/go/dataflow",
        sum = "h1:GuZJgkOL64cYySwYEYqQkggdxwoZTk8cvekQW0t3KRM=",
        version = "v0.9.6",
    )
    go_repository(
        name = "com_google_cloud_go_dataform",
        importpath = "cloud.google.com/go/dataform",
        sum = "h1:0EzWf+c2R5V/ujZBb22H/EL5wpzD/bDFYPA2f3czB1g=",
        version = "v0.9.3",
    )
    go_repository(
        name = "com_google_cloud_go_datafusion",
        importpath = "cloud.google.com/go/datafusion",
        sum = "h1:zSmMj/qZ0Yk+q/v5Wg40FTJ0WYPCtanYYekRt7cSKJ0=",
        version = "v1.7.6",
    )
    go_repository(
        name = "com_google_cloud_go_datalabeling",
        importpath = "cloud.google.com/go/datalabeling",
        sum = "h1:2zz44bPbDMHsPanQ89SybqhHDQBH1EZk8icGotyrvSU=",
        version = "v0.8.6",
    )
    go_repository(
        name = "com_google_cloud_go_dataplex",
        importpath = "cloud.google.com/go/dataplex",
        sum = "h1:Ob8NPT1UcB4kDaDx7/UdsRfZ8xUvUggZshXUlGWDahk=",
        version = "v1.15.0",
    )
    go_repository(
        name = "com_google_cloud_go_dataproc",
        importpath = "cloud.google.com/go/dataproc",
        sum = "h1:W47qHL3W4BPkAIbk4SWmIERwsWBaNnWm0P2sdx3YgGU=",
        version = "v1.12.0",
    )

    go_repository(
        name = "com_google_cloud_go_dataproc_v2",
        importpath = "cloud.google.com/go/dataproc/v2",
        sum = "h1:+cM8p/R6FdTuQYlriJOSUCvAZfMDgBKf0/ph9bMIjaY=",
        version = "v2.4.1",
    )

    go_repository(
        name = "com_google_cloud_go_dataqna",
        importpath = "cloud.google.com/go/dataqna",
        sum = "h1:FI/1q7VnANchQR9ug+nzujfiusLMfC3BHT7/fHi+BVU=",
        version = "v0.8.6",
    )
    go_repository(
        name = "com_google_cloud_go_datastore",
        importpath = "cloud.google.com/go/datastore",
        sum = "h1:0P9WcsQeTWjuD1H14JIY7XQscIPQ4Laje8ti96IC5vg=",
        version = "v1.15.0",
    )

    go_repository(
        name = "com_google_cloud_go_datastream",
        importpath = "cloud.google.com/go/datastream",
        sum = "h1:nHdOKbFmKJ4tPjGtNNIO0//G7QAht6eHTUnREWPn2yI=",
        version = "v1.10.5",
    )
    go_repository(
        name = "com_google_cloud_go_deploy",
        importpath = "cloud.google.com/go/deploy",
        sum = "h1:UxcxzjwxGPkT7RBdMmoc5a7QxHQVdpZllD6el2VC3JA=",
        version = "v1.17.2",
    )
    go_repository(
        name = "com_google_cloud_go_dialogflow",
        importpath = "cloud.google.com/go/dialogflow",
        sum = "h1:B8Y5j4/QsDirX136OoPm62kG3y5gd8rzBpHSR/FW9vI=",
        version = "v1.52.0",
    )
    go_repository(
        name = "com_google_cloud_go_dlp",
        importpath = "cloud.google.com/go/dlp",
        sum = "h1:dTsEN6r1BoplUACz7teOmE6lRG1swREiwXkfkY7bi6c=",
        version = "v1.12.1",
    )
    go_repository(
        name = "com_google_cloud_go_documentai",
        importpath = "cloud.google.com/go/documentai",
        sum = "h1:UdDy7nDTwr+mN1KiJqsj5AabauoW9SkgH9eY8BuFXJE=",
        version = "v1.26.1",
    )
    go_repository(
        name = "com_google_cloud_go_domains",
        importpath = "cloud.google.com/go/domains",
        sum = "h1:NHqZk4XzHFlmXM3LMGwDVET4NKr60W2jaNCRGYod5Ic=",
        version = "v0.9.6",
    )
    go_repository(
        name = "com_google_cloud_go_edgecontainer",
        importpath = "cloud.google.com/go/edgecontainer",
        sum = "h1:a++vBi1J00NP1ifVP5mV/3j1/EJKWPj0h6NfUPLfuCQ=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_google_cloud_go_errorreporting",
        importpath = "cloud.google.com/go/errorreporting",
        sum = "h1:kj1XEWMu8P0qlLhm3FwcaFsUvXChV/OraZwA70trRR0=",
        version = "v0.3.0",
    )

    go_repository(
        name = "com_google_cloud_go_essentialcontacts",
        importpath = "cloud.google.com/go/essentialcontacts",
        sum = "h1:FDdJGJEXK4RxvT6gdRBqGaCQVpi96RRB7MTyRHUcb20=",
        version = "v1.6.7",
    )
    go_repository(
        name = "com_google_cloud_go_eventarc",
        importpath = "cloud.google.com/go/eventarc",
        sum = "h1:JMUiLYzxkxr7BqnCPkyJ6Ycgrs6YQlZT44H0rHg7jQY=",
        version = "v1.13.5",
    )
    go_repository(
        name = "com_google_cloud_go_filestore",
        importpath = "cloud.google.com/go/filestore",
        sum = "h1:BpaB7bxICPUTntAV+yVUK9bxAUOv7uHRSEibSKMBJVA=",
        version = "v1.8.2",
    )
    go_repository(
        name = "com_google_cloud_go_firestore",
        importpath = "cloud.google.com/go/firestore",
        sum = "h1:/k8ppuWOtNuDHt2tsRV42yI21uaGnKDEQnRFeBpbFF8=",
        version = "v1.15.0",
    )

    go_repository(
        name = "com_google_cloud_go_functions",
        importpath = "cloud.google.com/go/functions",
        sum = "h1:0kcko/2AKwm4USnWcGs/W/k++PAYPA3dYaQw1y5Xg3M=",
        version = "v1.16.1",
    )
    go_repository(
        name = "com_google_cloud_go_gaming",
        importpath = "cloud.google.com/go/gaming",
        sum = "h1:5qZmZEWzMf8GEFgm9NeC3bjFRpt7x4S6U7oLbxaf7N8=",
        version = "v1.10.1",
    )

    go_repository(
        name = "com_google_cloud_go_gkebackup",
        importpath = "cloud.google.com/go/gkebackup",
        sum = "h1:SATJwsF8PjErP7GwHE+xK8gJ7f7hULuqtazV19ylPgg=",
        version = "v1.4.0",
    )
    go_repository(
        name = "com_google_cloud_go_gkeconnect",
        importpath = "cloud.google.com/go/gkeconnect",
        sum = "h1:7X9P6lGkOF/nJRYZBQBG2XPhunqWbNMacy9AXN7qUcU=",
        version = "v0.8.6",
    )
    go_repository(
        name = "com_google_cloud_go_gkehub",
        importpath = "cloud.google.com/go/gkehub",
        sum = "h1:kKreFf+097KfW+Tz/SqZKeXs/eFOjs1NDrsVjRPI18o=",
        version = "v0.14.6",
    )
    go_repository(
        name = "com_google_cloud_go_gkemulticloud",
        importpath = "cloud.google.com/go/gkemulticloud",
        sum = "h1:CFBoDcQi9zLOkzM6xqmRzljZhF4A6A47QaQ0WtNd+DA=",
        version = "v1.1.2",
    )
    go_repository(
        name = "com_google_cloud_go_grafeas",
        importpath = "cloud.google.com/go/grafeas",
        sum = "h1:D4x32R/cHX3MTofKwirz015uEdVk4uAxvZkZCZkOrF4=",
        version = "v0.3.4",
    )

    go_repository(
        name = "com_google_cloud_go_gsuiteaddons",
        importpath = "cloud.google.com/go/gsuiteaddons",
        sum = "h1:q3x2NE0je/tSVL66MAht5YVbGGHjTV9BxFD2lyDQ0dU=",
        version = "v1.6.6",
    )
    go_repository(
        name = "com_google_cloud_go_iam",
        importpath = "cloud.google.com/go/iam",
        sum = "h1:z4VHOhwKLF/+UYXAJDFwGtNF0b6gjsW1Pk9Ml0U/IoM=",
        version = "v1.1.7",
    )
    go_repository(
        name = "com_google_cloud_go_iap",
        importpath = "cloud.google.com/go/iap",
        sum = "h1:FrLAtgXzWPwe8rNp7AD+2Lgg4LqyhgXvEdiGK+jtd9g=",
        version = "v1.9.5",
    )
    go_repository(
        name = "com_google_cloud_go_ids",
        importpath = "cloud.google.com/go/ids",
        sum = "h1:tNc3NpIp2LUmFJxP2CBlzYw0FTnd68r73mIzg8UlM3Q=",
        version = "v1.4.6",
    )
    go_repository(
        name = "com_google_cloud_go_iot",
        importpath = "cloud.google.com/go/iot",
        sum = "h1:nRV/e1e3lEjsVoD5mW99JERwL8MKohyQyY63+lfBMA4=",
        version = "v1.7.6",
    )
    go_repository(
        name = "com_google_cloud_go_kms",
        importpath = "cloud.google.com/go/kms",
        sum = "h1:szIeDCowID8th2i8XE4uRev5PMxQFqW+JjwYxL9h6xs=",
        version = "v1.15.8",
    )
    go_repository(
        name = "com_google_cloud_go_language",
        importpath = "cloud.google.com/go/language",
        sum = "h1:srkreCxnVa5+a5PXUri/K+VWxG50wGvz5+PEYjgaENQ=",
        version = "v1.12.4",
    )
    go_repository(
        name = "com_google_cloud_go_lifesciences",
        importpath = "cloud.google.com/go/lifesciences",
        sum = "h1:8w3edjRiSN6GCxT0uJoXr6Zo2XNYD+6TxPZa7uIIOaU=",
        version = "v0.9.6",
    )
    go_repository(
        name = "com_google_cloud_go_logging",
        importpath = "cloud.google.com/go/logging",
        sum = "h1:iEIOXFO9EmSiTjDmfpbRjOxECO7R8C7b8IXUGOj7xZw=",
        version = "v1.9.0",
    )

    go_repository(
        name = "com_google_cloud_go_longrunning",
        importpath = "cloud.google.com/go/longrunning",
        sum = "h1:WLbHekDbjK1fVFD3ibpFFVoyizlLRl73I7YKuAKilhU=",
        version = "v0.5.7",
    )
    go_repository(
        name = "com_google_cloud_go_managedidentities",
        importpath = "cloud.google.com/go/managedidentities",
        sum = "h1:7+hGPQSojhnYNZCg3fG2mQIF7XMfvNpCpi2Zg5/Qx1g=",
        version = "v1.6.6",
    )
    go_repository(
        name = "com_google_cloud_go_maps",
        importpath = "cloud.google.com/go/maps",
        sum = "h1:vcqmqk0wt1NRzQc84Qo6z8HYyol/znqbG7tAS5Qm91g=",
        version = "v1.7.1",
    )

    go_repository(
        name = "com_google_cloud_go_mediatranslation",
        importpath = "cloud.google.com/go/mediatranslation",
        sum = "h1:EVW0wCQ7asoMjw8fm8oUe6pQWBaVQth/iquk7ysidy0=",
        version = "v0.8.6",
    )
    go_repository(
        name = "com_google_cloud_go_memcache",
        importpath = "cloud.google.com/go/memcache",
        sum = "h1:rqDPCIUfVBvv7ojOGx5PRkPgWeWSKpOht2w6plaxklY=",
        version = "v1.10.6",
    )
    go_repository(
        name = "com_google_cloud_go_metastore",
        importpath = "cloud.google.com/go/metastore",
        sum = "h1:K7gyYoqPvQgCc82tiB0CQkXOpg8AZxJtRGMVdN5B83U=",
        version = "v1.13.5",
    )
    go_repository(
        name = "com_google_cloud_go_monitoring",
        importpath = "cloud.google.com/go/monitoring",
        sum = "h1:0yvFXK+xQd95VKo6thndjwnJMno7c7Xw1CwMByg0B+8=",
        version = "v1.18.1",
    )
    go_repository(
        name = "com_google_cloud_go_networkconnectivity",
        importpath = "cloud.google.com/go/networkconnectivity",
        sum = "h1:t67aEKwmO+SXvQC5ncOjm3vTwnsbO/mTzlCWdK0nwqs=",
        version = "v1.14.5",
    )
    go_repository(
        name = "com_google_cloud_go_networkmanagement",
        importpath = "cloud.google.com/go/networkmanagement",
        sum = "h1:uSoVcd78+uNSW34Q+BNumUvTxAtVaKHc8O9WUz091gg=",
        version = "v1.13.0",
    )
    go_repository(
        name = "com_google_cloud_go_networksecurity",
        importpath = "cloud.google.com/go/networksecurity",
        sum = "h1:3ggPKshcFs1oRh5qI+Gq1s2CIU9BL99MKtYSBG4Z8s0=",
        version = "v0.9.6",
    )
    go_repository(
        name = "com_google_cloud_go_notebooks",
        importpath = "cloud.google.com/go/notebooks",
        sum = "h1:A9jxIdxEccgL9iJLqvU4j5HT3/13YluoF2IbiV+hAN4=",
        version = "v1.11.4",
    )
    go_repository(
        name = "com_google_cloud_go_optimization",
        importpath = "cloud.google.com/go/optimization",
        sum = "h1:T/j8xyIkmHGjU6kxeUjK3UTqiXlbvpZQ2A+F5vnH21Y=",
        version = "v1.6.4",
    )
    go_repository(
        name = "com_google_cloud_go_orchestration",
        importpath = "cloud.google.com/go/orchestration",
        sum = "h1:i5iSxsu1Cx1itTQEEY/YvsAo1OO8gosGGXhnOjBjgJA=",
        version = "v1.9.1",
    )
    go_repository(
        name = "com_google_cloud_go_orgpolicy",
        importpath = "cloud.google.com/go/orgpolicy",
        sum = "h1:x9GttuUZXXeKcJgHSGxYoPn2hOJhhuaN5YYJKfAfmLo=",
        version = "v1.12.2",
    )
    go_repository(
        name = "com_google_cloud_go_osconfig",
        importpath = "cloud.google.com/go/osconfig",
        sum = "h1:wIOhgzklE0hHZsho02rRVXYBHSfsAwYZYIaxFaUBIjs=",
        version = "v1.12.6",
    )
    go_repository(
        name = "com_google_cloud_go_oslogin",
        importpath = "cloud.google.com/go/oslogin",
        sum = "h1:v71OrrkKyqr5Mfnt345GqSOURzByv08qfrtvfhOVcnc=",
        version = "v1.13.2",
    )
    go_repository(
        name = "com_google_cloud_go_phishingprotection",
        importpath = "cloud.google.com/go/phishingprotection",
        sum = "h1:DcAre1psFwJM/FBA/MkDj0H6uxZhACE5IW/xF9ssHDQ=",
        version = "v0.8.6",
    )
    go_repository(
        name = "com_google_cloud_go_policytroubleshooter",
        importpath = "cloud.google.com/go/policytroubleshooter",
        sum = "h1:wxBRfNoMy7rnoEeaTOHIEHCUEdUIQIwQGUqfBWH6cyQ=",
        version = "v1.10.4",
    )
    go_repository(
        name = "com_google_cloud_go_privatecatalog",
        importpath = "cloud.google.com/go/privatecatalog",
        sum = "h1:bcIABOUmpnzQip83OVv+Ju/NxXjUTRLUSP+FVLFG6kk=",
        version = "v0.9.6",
    )
    go_repository(
        name = "com_google_cloud_go_pubsub",
        importpath = "cloud.google.com/go/pubsub",
        sum = "h1:0uEEfaB1VIJzabPpwpZf44zWAKAme3zwKKxHk7vJQxQ=",
        version = "v1.37.0",
    )
    go_repository(
        name = "com_google_cloud_go_pubsublite",
        importpath = "cloud.google.com/go/pubsublite",
        sum = "h1:pX+idpWMIH30/K7c0epN6V703xpIcMXWRjKJsz0tYGY=",
        version = "v1.8.1",
    )
    go_repository(
        name = "com_google_cloud_go_recaptchaenterprise",
        importpath = "cloud.google.com/go/recaptchaenterprise",
        sum = "h1:u6EznTGzIdsyOsvm+Xkw0aSuKFXQlyjGE9a4exk6iNQ=",
        version = "v1.3.1",
    )

    go_repository(
        name = "com_google_cloud_go_recaptchaenterprise_v2",
        importpath = "cloud.google.com/go/recaptchaenterprise/v2",
        sum = "h1:nykUP2WD/914jui/IldiCOuoTn6T8ha1Ys6/N9sAqJY=",
        version = "v2.12.0",
    )
    go_repository(
        name = "com_google_cloud_go_recommendationengine",
        importpath = "cloud.google.com/go/recommendationengine",
        sum = "h1:m0eQtYCToxMSbDKOnpJ2YGdQhyjOPffg4Y8lM2RWzao=",
        version = "v0.8.6",
    )
    go_repository(
        name = "com_google_cloud_go_recommender",
        importpath = "cloud.google.com/go/recommender",
        sum = "h1:3M6lD39/GlOMYOikeF5wflSa4EP5pGFthoIASbyhIXE=",
        version = "v1.12.2",
    )
    go_repository(
        name = "com_google_cloud_go_redis",
        importpath = "cloud.google.com/go/redis",
        sum = "h1:zlGxeAsiwcPU+Cta76ALduhdBAVhuYpEjv59V5L/ves=",
        version = "v1.14.3",
    )
    go_repository(
        name = "com_google_cloud_go_resourcemanager",
        importpath = "cloud.google.com/go/resourcemanager",
        sum = "h1:VPfJFbWxrTYQzEXCDbJNpcvSB8eZhTSM0YHH146fIB8=",
        version = "v1.9.6",
    )
    go_repository(
        name = "com_google_cloud_go_resourcesettings",
        importpath = "cloud.google.com/go/resourcesettings",
        sum = "h1:l/IbRDDmGJFlR4bRZGtfYvix1Pu0jAKGLr7wgUtixHQ=",
        version = "v1.6.6",
    )
    go_repository(
        name = "com_google_cloud_go_retail",
        importpath = "cloud.google.com/go/retail",
        sum = "h1:AyVdElkdIU3JedWpX/qENbt8iUmKD+kiyj7ZpzguhTg=",
        version = "v1.16.1",
    )
    go_repository(
        name = "com_google_cloud_go_run",
        importpath = "cloud.google.com/go/run",
        sum = "h1:xQND6EJn1LgouCLPSfykkzagyr4gq4FKiRexNxXixV0=",
        version = "v1.3.6",
    )
    go_repository(
        name = "com_google_cloud_go_scheduler",
        importpath = "cloud.google.com/go/scheduler",
        sum = "h1:h1/VZk0XdkSh/jI7dDNp3V0Qi8yTkclOljDVPelXvAw=",
        version = "v1.10.7",
    )
    go_repository(
        name = "com_google_cloud_go_secretmanager",
        importpath = "cloud.google.com/go/secretmanager",
        sum = "h1:e5pIo/QEgiFiHPVJPxM5jbtUr4O/u5h2zLHYtkFQr24=",
        version = "v1.12.0",
    )
    go_repository(
        name = "com_google_cloud_go_security",
        importpath = "cloud.google.com/go/security",
        sum = "h1:LYMj7ISEEjVQ0ub6E6ygGhjVbNQTH5CawKZz0bbPMVE=",
        version = "v1.15.6",
    )
    go_repository(
        name = "com_google_cloud_go_securitycenter",
        importpath = "cloud.google.com/go/securitycenter",
        sum = "h1:NpEJeFbm3ad3ibpbpIBKXJS7eQq1cZhtt9nrDTMO/QQ=",
        version = "v1.28.0",
    )
    go_repository(
        name = "com_google_cloud_go_servicecontrol",
        importpath = "cloud.google.com/go/servicecontrol",
        sum = "h1:d0uV7Qegtfaa7Z2ClDzr9HJmnbJW7jn0WhZ7wOX6hLE=",
        version = "v1.11.1",
    )

    go_repository(
        name = "com_google_cloud_go_servicedirectory",
        importpath = "cloud.google.com/go/servicedirectory",
        sum = "h1:gkzx9Cd+OTOD+zY4u5vtbdvOx7vrvHYdeDiNdC6vKyw=",
        version = "v1.11.5",
    )
    go_repository(
        name = "com_google_cloud_go_servicemanagement",
        importpath = "cloud.google.com/go/servicemanagement",
        sum = "h1:fopAQI/IAzlxnVeiKn/8WiV6zKndjFkvi+gzu+NjywY=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_serviceusage",
        importpath = "cloud.google.com/go/serviceusage",
        sum = "h1:rXyq+0+RSIm3HFypctp7WoXxIA563rn206CfMWdqXX4=",
        version = "v1.6.0",
    )

    go_repository(
        name = "com_google_cloud_go_shell",
        importpath = "cloud.google.com/go/shell",
        sum = "h1:/oJf9sboa2FfHWCmHXy+XfTRnZy8lC7O5zFyfE1EA6s=",
        version = "v1.7.6",
    )
    go_repository(
        name = "com_google_cloud_go_spanner",
        importpath = "cloud.google.com/go/spanner",
        sum = "h1:O9kf49dfaDRzPpKJNChHUJ+Bao02WPedZb8ZPyi02lI=",
        version = "v1.60.0",
    )

    go_repository(
        name = "com_google_cloud_go_speech",
        importpath = "cloud.google.com/go/speech",
        sum = "h1:xo/cmhBtqoqqNg/5I8m0ECXPiqYg2fS2ioOccH+qbKE=",
        version = "v1.22.1",
    )
    go_repository(
        name = "com_google_cloud_go_storage",
        importpath = "cloud.google.com/go/storage",
        sum = "h1:MvraqHKhogCOTXTlct/9C3K3+Uy2jBmFYb3/Sp6dVtY=",
        version = "v1.39.1",
    )

    go_repository(
        name = "com_google_cloud_go_storagetransfer",
        importpath = "cloud.google.com/go/storagetransfer",
        sum = "h1:BawJo/u0P21cdxc2gB878qIFDC80COq2i0qWZeNevSw=",
        version = "v1.10.5",
    )
    go_repository(
        name = "com_google_cloud_go_talent",
        importpath = "cloud.google.com/go/talent",
        sum = "h1:4xgDFfOcgcSY0dUzaSc2tQCSRoLDEJ5CfbW5jfcgNJk=",
        version = "v1.6.7",
    )
    go_repository(
        name = "com_google_cloud_go_texttospeech",
        importpath = "cloud.google.com/go/texttospeech",
        sum = "h1:gLEyDoJeFGdoX7jSKbf+nJy7CTgjsSbCZXwzzkXgH9w=",
        version = "v1.7.6",
    )
    go_repository(
        name = "com_google_cloud_go_tpu",
        importpath = "cloud.google.com/go/tpu",
        sum = "h1:Cb1txkZYbKlGIZ4tQr9EjEB9snAOU6qyjvNezGXDunI=",
        version = "v1.6.6",
    )
    go_repository(
        name = "com_google_cloud_go_trace",
        importpath = "cloud.google.com/go/trace",
        sum = "h1:XF0Ejdw0NpRfAvuZUeQe3ClAG4R/9w5JYICo7l2weaw=",
        version = "v1.10.6",
    )
    go_repository(
        name = "com_google_cloud_go_translate",
        importpath = "cloud.google.com/go/translate",
        sum = "h1:SXOtKYnT7ZkeMtPwujaBOBt5Ph4kf6LIuMpAgu/WON0=",
        version = "v1.10.2",
    )
    go_repository(
        name = "com_google_cloud_go_video",
        importpath = "cloud.google.com/go/video",
        sum = "h1:y4jgUqDiWMfX+beJnlrnloBQxEIa9v+KrlkD2QJVpeE=",
        version = "v1.20.5",
    )
    go_repository(
        name = "com_google_cloud_go_videointelligence",
        importpath = "cloud.google.com/go/videointelligence",
        sum = "h1:P0Sa8+5KOEAVk/fazUNjVPzRCijCheZWJ8wL8xBn9Uk=",
        version = "v1.11.6",
    )
    go_repository(
        name = "com_google_cloud_go_vision",
        importpath = "cloud.google.com/go/vision",
        sum = "h1:/CsSTkbmO9HC8iQpxbK8ATms3OQaX3YQUeTMGCxlaK4=",
        version = "v1.2.0",
    )

    go_repository(
        name = "com_google_cloud_go_vision_v2",
        importpath = "cloud.google.com/go/vision/v2",
        sum = "h1:kvR1sHcuPYat1wI3BYY7CEX2xLAcUHPYL6dOzV2Xf4Q=",
        version = "v2.8.1",
    )
    go_repository(
        name = "com_google_cloud_go_vmmigration",
        importpath = "cloud.google.com/go/vmmigration",
        sum = "h1:sbaWK76csqtk0TGPGCiJqZi7tfrU0LnrhUjZHI5YVdc=",
        version = "v1.7.6",
    )
    go_repository(
        name = "com_google_cloud_go_vmwareengine",
        importpath = "cloud.google.com/go/vmwareengine",
        sum = "h1:Mf8abigBstvjfSGq9twhtbMTCONL0Cjds+tGbc2pV0M=",
        version = "v1.1.2",
    )

    go_repository(
        name = "com_google_cloud_go_vpcaccess",
        importpath = "cloud.google.com/go/vpcaccess",
        sum = "h1:wbMTRdZ9P5+3D6oQWWqB/YxDCFR5m5OJ+b+hHwaBBQQ=",
        version = "v1.7.6",
    )
    go_repository(
        name = "com_google_cloud_go_webrisk",
        importpath = "cloud.google.com/go/webrisk",
        sum = "h1:rVhi2WOHcZF72X7spXVTFTmRGeFN4NFeW7/Ku7kgeug=",
        version = "v1.9.6",
    )
    go_repository(
        name = "com_google_cloud_go_websecurityscanner",
        importpath = "cloud.google.com/go/websecurityscanner",
        sum = "h1:YAwNB/HjKOVAy9D7W8Bkv8OQ9G2lqIqFOuJbyH5Xo4Q=",
        version = "v1.6.6",
    )
    go_repository(
        name = "com_google_cloud_go_workflows",
        importpath = "cloud.google.com/go/workflows",
        sum = "h1:hH511zmS93oE6j64m/eiGWnfgqailh/S8+f3MVNLcE8=",
        version = "v1.12.5",
    )
    go_repository(
        name = "com_lukechampine_uint128",
        importpath = "lukechampine.com/uint128",
        sum = "h1:cDdUVfRwDUDovz610ABgFD17nXD4/uDgVHl2sC3+sbo=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_shuralyov_dmitri_gpu_mtl",
        importpath = "dmitri.shuralyov.com/gpu/mtl",
        sum = "h1:VpgP7xuJadIUuKccphEpTJnWhS2jkQyMt6Y7pJCD7fY=",
        version = "v0.0.0-20190408044501-666a987793e9",
    )
    go_repository(
        name = "ht_sr_git_sbinet_gg",
        importpath = "git.sr.ht/~sbinet/gg",
        sum = "h1:LNhjNn8DerC8f9DHLz6lS0YYul/b602DUxDgGkd/Aik=",
        version = "v0.3.1",
    )

    go_repository(
        name = "in_gopkg_check_v1",
        importpath = "gopkg.in/check.v1",
        sum = "h1:Hei/4ADfdWqJk1ZMxUNpqntNwaWcugrBjAiHlqqRiVk=",
        version = "v1.0.0-20201130134442-10cb98267c6c",
    )
    go_repository(
        name = "in_gopkg_errgo_v2",
        importpath = "gopkg.in/errgo.v2",
        sum = "h1:0vLT13EuvQ0hNvakwLuFZ/jYrLp5F3kcWHXdRggjCE8=",
        version = "v2.1.0",
    )

    go_repository(
        name = "in_gopkg_yaml_v2",
        importpath = "gopkg.in/yaml.v2",
        sum = "h1:fvjTMHxHEw/mxHbtzPi3JCcKXQRAnQTBRo6YCJSVHKI=",
        version = "v2.2.3",
    )
    go_repository(
        name = "in_gopkg_yaml_v3",
        importpath = "gopkg.in/yaml.v3",
        sum = "h1:fxVm/GzAzEWqLHuvctI91KS9hhNmmWOoWu0XTYJS7CA=",
        version = "v3.0.1",
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
        name = "io_opentelemetry_go_otel_sdk",
        importpath = "go.opentelemetry.io/otel/sdk",
        sum = "h1:6coWHw9xw7EfClIC/+O31R8IY3/+EiRFHevmHafB2Gw=",
        version = "v1.22.0",
    )

    go_repository(
        name = "io_opentelemetry_go_otel_trace",
        importpath = "go.opentelemetry.io/otel/trace",
        sum = "h1:CsKnnL4dUAr/0llH9FKuc698G04IrpWV0MQA/Y1YELI=",
        version = "v1.24.0",
    )
    go_repository(
        name = "io_opentelemetry_go_proto_otlp",
        importpath = "go.opentelemetry.io/proto/otlp",
        sum = "h1:T0TX0tmXU8a3CbNXzEKGeU5mIVOdf0oykP+u2lIVU/I=",
        version = "v1.0.0",
    )
    go_repository(
        name = "io_rsc_binaryregexp",
        importpath = "rsc.io/binaryregexp",
        sum = "h1:HfqmD5MEmC0zvwBuF187nq9mdnXjXsSivRiXN7SmRkE=",
        version = "v0.2.0",
    )
    go_repository(
        name = "io_rsc_pdf",
        importpath = "rsc.io/pdf",
        sum = "h1:k1MczvYDUvJBe93bYd7wrZLLUEcLZAuF824/I4e5Xr4=",
        version = "v0.1.1",
    )
    go_repository(
        name = "io_rsc_quote_v3",
        importpath = "rsc.io/quote/v3",
        sum = "h1:9JKUTTIUgS6kzR9mK1YuGKv6Nl+DijDNIc0ghT58FaY=",
        version = "v3.1.0",
    )
    go_repository(
        name = "io_rsc_sampler",
        importpath = "rsc.io/sampler",
        sum = "h1:7uVkIFmeBqHfdjD+gZwtXXI+RODJ2Wc4O7MPEh/QiW4=",
        version = "v1.3.0",
    )
    go_repository(
        name = "org_gioui",
        importpath = "gioui.org",
        sum = "h1:K72hopUosKG3ntOPNG4OzzbuhxGuVf06fa2la1/H/Ho=",
        version = "v0.0.0-20210308172011-57750fc8a0a6",
    )

    go_repository(
        name = "org_golang_google_appengine",
        importpath = "google.golang.org/appengine",
        sum = "h1:IhEN5q69dyKagZPYMSdIjS2HqprW324FRQZJcGqPAsM=",
        version = "v1.6.8",
    )
    go_repository(
        name = "org_golang_google_genproto",
        importpath = "google.golang.org/genproto",
        sum = "h1:u7WMYrIrVvs0TF5yaKwKNbcJyySYf+HAIFXxWltJOXE=",
        version = "v0.0.0-20240528184218-531527333157",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_api",
        importpath = "google.golang.org/genproto/googleapis/api",
        sum = "h1:SkdGTrROJl2jRGT/Fxv5QUf9jtdKCQh4KQJXbXVLAi0=",
        version = "v0.0.0-20240521202816-d264139d666e",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_bytestream",
        importpath = "google.golang.org/genproto/googleapis/bytestream",
        sum = "h1:znHUtThh5/fLbEC/p3Khp5xOucyAgMZ1Nj9ditbxd44=",
        version = "v0.0.0-20240528184218-531527333157",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_rpc",
        importpath = "google.golang.org/genproto/googleapis/rpc",
        sum = "h1:Zy9XzmMEflZ/MAaA7vNcoebnRAld7FsPW1EeBB7V0m8=",
        version = "v0.0.0-20240528184218-531527333157",
    )
    go_repository(
        name = "org_golang_google_grpc_cmd_protoc_gen_go_grpc",
        importpath = "google.golang.org/grpc/cmd/protoc-gen-go-grpc",
        sum = "h1:M1YKkFIboKNieVO5DLUEVzQfGwJD30Nv2jfUgzb5UcE=",
        version = "v1.1.0",
    )

    go_repository(
        name = "org_golang_google_protobuf",
        importpath = "google.golang.org/protobuf",
        sum = "h1:9ddQBjfCyZPOHPUiPxpYESBLc+T8P3E+Vo4IbKZgFWg=",
        version = "v1.34.1",
    )
    go_repository(
        name = "org_golang_x_crypto",
        importpath = "golang.org/x/crypto",
        sum = "h1:dIJU/v2J8Mdglj/8rJ6UUOM3Zc9zLZxVZwwxMooUSAI=",
        version = "v0.23.0",
    )
    go_repository(
        name = "org_golang_x_exp",
        importpath = "golang.org/x/exp",
        sum = "h1:jtJma62tbqLibJ5sFQz8bKtEM8rJBtfilJ2qTU199MI=",
        version = "v0.0.0-20231006140011-7918f672742d",
    )
    go_repository(
        name = "org_golang_x_image",
        importpath = "golang.org/x/image",
        sum = "h1:TcHcE0vrmgzNH1v3ppjcMGbhG5+9fMuvOmUYwNEF4q4=",
        version = "v0.0.0-20220302094943-723b81ca9867",
    )

    go_repository(
        name = "org_golang_x_lint",
        importpath = "golang.org/x/lint",
        sum = "h1:VLliZ0d+/avPrXXH+OakdXhpJuEoBZuwh1m2j7U6Iug=",
        version = "v0.0.0-20210508222113-6edffad5e616",
    )
    go_repository(
        name = "org_golang_x_mobile",
        importpath = "golang.org/x/mobile",
        sum = "h1:4+4C/Iv2U4fMZBiMCc98MG1In4gJY5YRhtpDNeDeHWs=",
        version = "v0.0.0-20190719004257-d2bd2a29d028",
    )

    go_repository(
        name = "org_golang_x_mod",
        importpath = "golang.org/x/mod",
        sum = "h1:I/DsJXRlw/8l/0c24sM9yb0T4z9liZTduXvdAWYiysY=",
        version = "v0.13.0",
    )
    go_repository(
        name = "org_golang_x_net",
        importpath = "golang.org/x/net",
        sum = "h1:d/OCCoBEUq33pjydKrGQhw7IlUPI2Oylr+8qLx49kac=",
        version = "v0.25.0",
    )
    go_repository(
        name = "org_golang_x_sync",
        importpath = "golang.org/x/sync",
        sum = "h1:YsImfSBoP9QPYL0xyKJPq0gcaJdG3rInoqxTWbfQu9M=",
        version = "v0.7.0",
    )
    go_repository(
        name = "org_golang_x_sys",
        importpath = "golang.org/x/sys",
        sum = "h1:Od9JTbYCk261bKm4M/mw7AklTlFYIa0bIp9BgSm1S8Y=",
        version = "v0.20.0",
    )
    go_repository(
        name = "org_golang_x_term",
        importpath = "golang.org/x/term",
        sum = "h1:VnkxpohqXaOBYJtBmEppKUG6mXpi+4O6purfc2+sMhw=",
        version = "v0.20.0",
    )
    go_repository(
        name = "org_golang_x_text",
        importpath = "golang.org/x/text",
        sum = "h1:h1V/4gjBv8v9cjcR6+AR5+/cIYK5N/WAgiv4xlsEtAk=",
        version = "v0.15.0",
    )
    go_repository(
        name = "org_golang_x_time",
        importpath = "golang.org/x/time",
        sum = "h1:o7cqy6amK/52YcAKIPlM3a+Fpj35zvRj2TP+e1xFSfk=",
        version = "v0.5.0",
    )

    go_repository(
        name = "org_golang_x_tools",
        importpath = "golang.org/x/tools",
        sum = "h1:jvNa2pY0M4r62jkRQ6RwEZZyPcymeL9XZMLBbV7U2nc=",
        version = "v0.14.0",
    )
    go_repository(
        name = "org_golang_x_xerrors",
        importpath = "golang.org/x/xerrors",
        sum = "h1:+cNy6SZtPcJQH3LJVLOSmiC7MMxXNOb3PU/VUEz+EhU=",
        version = "v0.0.0-20231012003039-104605ab7028",
    )
    go_repository(
        name = "org_gonum_v1_gonum",
        importpath = "gonum.org/v1/gonum",
        sum = "h1:xKuo6hzt+gMav00meVPUlXwSdoEJP46BR+wdxQEFK2o=",
        version = "v0.12.0",
    )
    go_repository(
        name = "org_gonum_v1_netlib",
        importpath = "gonum.org/v1/netlib",
        sum = "h1:OE9mWmgKkjJyEmDAAtGMPjXu+YNeGvK9VTSHY6+Qihc=",
        version = "v0.0.0-20190313105609-8cb42192e0e0",
    )
    go_repository(
        name = "org_gonum_v1_plot",
        importpath = "gonum.org/v1/plot",
        sum = "h1:dnifSs43YJuNMDzB7v8wV64O4ABBHReuAVAoBxqBqS4=",
        version = "v0.10.1",
    )
    go_repository(
        name = "org_modernc_cc_v3",
        importpath = "modernc.org/cc/v3",
        sum = "h1:P3g79IUS/93SYhtoeaHW+kRCIrYaxJ27MFPv+7kaTOw=",
        version = "v3.40.0",
    )
    go_repository(
        name = "org_modernc_ccgo_v3",
        importpath = "modernc.org/ccgo/v3",
        sum = "h1:Mkgdzl46i5F/CNR/Kj80Ri59hC8TKAhZrYSaqvkwzUw=",
        version = "v3.16.13",
    )
    go_repository(
        name = "org_modernc_ccorpus",
        importpath = "modernc.org/ccorpus",
        sum = "h1:J16RXiiqiCgua6+ZvQot4yUuUy8zxgqbqEEUuGPlISk=",
        version = "v1.11.6",
    )
    go_repository(
        name = "org_modernc_httpfs",
        importpath = "modernc.org/httpfs",
        sum = "h1:AAgIpFZRXuYnkjftxTAZwMIiwEqAfk8aVB2/oA6nAeM=",
        version = "v1.0.6",
    )
    go_repository(
        name = "org_modernc_libc",
        importpath = "modernc.org/libc",
        sum = "h1:wymSbZb0AlrjdAVX3cjreCHTPCpPARbQXNz6BHPzdwQ=",
        version = "v1.22.4",
    )
    go_repository(
        name = "org_modernc_mathutil",
        importpath = "modernc.org/mathutil",
        sum = "h1:rV0Ko/6SfM+8G+yKiyI830l3Wuz1zRutdslNoQ0kfiQ=",
        version = "v1.5.0",
    )
    go_repository(
        name = "org_modernc_memory",
        importpath = "modernc.org/memory",
        sum = "h1:N+/8c5rE6EqugZwHii4IFsaJ7MUhoWX07J5tC/iI5Ds=",
        version = "v1.5.0",
    )
    go_repository(
        name = "org_modernc_opt",
        importpath = "modernc.org/opt",
        sum = "h1:3XOZf2yznlhC+ibLltsDGzABUGVx8J6pnFMS3E4dcq4=",
        version = "v0.1.3",
    )
    go_repository(
        name = "org_modernc_sqlite",
        importpath = "modernc.org/sqlite",
        sum = "h1:ixuUG0QS413Vfzyx6FWx6PYTmHaOegTY+hjzhn7L+a0=",
        version = "v1.21.2",
    )
    go_repository(
        name = "org_modernc_strutil",
        importpath = "modernc.org/strutil",
        sum = "h1:fNMm+oJklMGYfU9Ylcywl0CO5O6nTfaowNsh2wpPjzY=",
        version = "v1.1.3",
    )
    go_repository(
        name = "org_modernc_tcl",
        importpath = "modernc.org/tcl",
        sum = "h1:mOQwiEK4p7HruMZcwKTZPw/aqtGM4aY00uzWhlKKYws=",
        version = "v1.15.1",
    )
    go_repository(
        name = "org_modernc_token",
        importpath = "modernc.org/token",
        sum = "h1:Xl7Ap9dKaEs5kLoOQeQmPWevfnk/DM5qcLcYlA8ys6Y=",
        version = "v1.1.0",
    )
    go_repository(
        name = "org_modernc_z",
        importpath = "modernc.org/z",
        sum = "h1:xkDw/KepgEjeizO2sNco+hqYkU12taxQFqPEmgm1GWE=",
        version = "v1.7.0",
    )
    go_repository(
        name = "tech_einride_go_aip",
        importpath = "go.einride.tech/aip",
        sum = "h1:XfV+NQX6L7EOYK11yoHHFtndeaWh3KbD9/cN/6iWEt8=",
        version = "v0.66.0",
    )
    go_repository(
        name = "tools_gotest_v3",
        importpath = "gotest.tools/v3",
        sum = "h1:EENdUnS3pdur5nybKYIh2Vfgc8IUNBjxDPSjtiJcOzU=",
        version = "v3.5.1",
    )
