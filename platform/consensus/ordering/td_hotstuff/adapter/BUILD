package(default_visibility = ["//visibility:public"])

cc_library(
    name = "td_hotstuff_reputation_adapter",
    srcs = ["td_hotstuff_reputation_adapter.cpp"],
    hdrs = ["td_hotstuff_reputation_adapter.h"],
    deps = [
        "//common:glog",
        "//platform/consensus/reputation:reputation_plugin_runtime",
    ],
)

cc_test(
    name = "td_hotstuff_reputation_adapter_test",
    srcs = ["td_hotstuff_reputation_adapter_test.cpp"],
    deps = [
        ":td_hotstuff_reputation_adapter",
        "//common/test:test_main",
    ],
)
