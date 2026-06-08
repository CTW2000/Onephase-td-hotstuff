package(default_visibility = ["//visibility:public"])

cc_library(
    name = "reputation_algorithm",
    srcs = [
        "reputation_algorithm.cpp",
        "reputation_roots.cpp",
        "reputation_utils.cpp",
        "soft_reputation.cpp",
        "strong_fault_detector.cpp",
        "sybil_graph.cpp",
    ],
    hdrs = [
        "reputation_algorithm.h",
        "reputation_roots.h",
        "reputation_types.h",
        "reputation_utils.h",
        "soft_reputation.h",
        "strong_fault_detector.h",
        "sybil_graph.h",
    ],
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
