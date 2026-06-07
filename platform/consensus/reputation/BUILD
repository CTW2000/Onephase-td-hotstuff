package(default_visibility = ["//visibility:public"])

cc_library(
    name = "reputation_algorithm",
    srcs = ["reputation_algorithm.cpp"],
    hdrs = ["reputation_algorithm.h"],
    deps = [
        "//common/crypto:hash",
    ],
)

cc_test(
    name = "reputation_algorithm_test",
    srcs = ["reputation_algorithm_test.cpp"],
    deps = [
        ":reputation_algorithm",
        "//common/test:test_main",
    ],
)
